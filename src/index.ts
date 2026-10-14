import { randomUUID } from "crypto";
import "dotenv/config";
import express from "express";
import type { Server } from "http";
import rateLimit from "express-rate-limit";
import helmet from "helmet";
import { loadConfig, type Config } from "./config";
import { Database } from "./db/database";
import { createErrorHandler } from "./middleware/errorHandler";
import { CheckpointRepository } from "./repositories/CheckpointRepository";
import { DeliveryRepository } from "./repositories/DeliveryRepository";
import { PlanRepository } from "./repositories/PlanRepository";
import { WebhookRepository } from "./repositories/WebhookRepository";
import { createWatcherRoutes } from "./routes/watcherRoutes";
import { BlockScannerService } from "./services/BlockScannerService";
import { DeliveryWorkerService } from "./services/DeliveryWorkerService";
import { type BlockProvider } from "./services/providers/BlockProvider";
import { BscBlockProvider } from "./services/providers/BscBlockProvider";
import { TronBlockProvider } from "./services/providers/TronBlockProvider";
import { NETWORKS, type Network } from "./types";
import { errorMessage } from "./errors";
import { createAppLogger, setupLogging } from "./utils/logger";

function createProvider(config: Config, network: Network): BlockProvider {
  if (network === "TRC20") {
    return new TronBlockProvider(
      config.networks.TRC20.tokenContract,
      config.tron.url,
      config.tron.apiKey,
    );
  }
  return new BscBlockProvider(
    config.networks.BEP20.tokenContract,
    config.bsc.rpcUrl,
  );
}

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createAppLogger({
    level: config.logLevel,
    toFiles: config.nodeEnv !== "test",
  });

  const db = await Database.open(config.databaseUrl);
  const plans = new PlanRepository(db);
  const webhooks = new WebhookRepository(db, plans);
  const checkpoints = new CheckpointRepository(db);
  const deliveries = new DeliveryRepository(db);

  const app = express();
  app.use(helmet());
  app.use(express.json({ limit: "1mb" }));
  app.use(rateLimit({ windowMs: 15 * 60 * 1000, max: 100 }));
  setupLogging(app, logger);
  app.use(
    "/api/watcher",
    createWatcherRoutes({
      db,
      plans,
      webhooks,
      deliveries,
      checkpoints,
      apiKey: config.apiKey,
      networks: NETWORKS.map((network) => ({
        network,
        enabled: config.networks[network].enabled,
        confirmations: config.networks[network].confirmations,
      })),
      retentionDays: config.retention.days,
    }),
  );
  app.use(createErrorHandler(logger));

  const instanceId = randomUUID();
  const scanners: BlockScannerService[] = [];
  let worker: DeliveryWorkerService | null = null;
  let server: Server | null = null;
  let shuttingDown = false;

  const shutdown = async (reason: string, exitCode = 0): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info("shutting down", { reason });
    server?.close();
    await Promise.allSettled(scanners.map((scanner) => scanner.stop()));
    await worker?.stop();
    await db.close();
    process.exit(exitCode);
  };

  for (const network of NETWORKS) {
    const networkConfig = config.networks[network];
    if (!networkConfig.enabled) {
      logger.info("network disabled by configuration", { network });
      continue;
    }
    const scanner = new BlockScannerService(
      {
        network,
        confirmations: networkConfig.confirmations,
        tokenContract: networkConfig.tokenContract,
        decimals: networkConfig.decimals,
        startBlock: networkConfig.startBlock,
        idleIntervalMs: config.scanner.idleIntervalMs,
        instanceId,
        networkMode: config.networkMode,
      },
      createProvider(config, network),
      {
        db,
        checkpoints,
        deliveries,
        plans,
        logger,
        onFatal: (error) => {
          void shutdown(`fatal scanner error: ${error.message}`, 1);
        },
      },
    );
    await scanner.start();
    scanners.push(scanner);
  }

  const deliveryWorker = new DeliveryWorkerService(
    {
      ...config.worker,
      retentionDays: config.retention.days,
      pruneIntervalMs: config.retention.pruneIntervalMs,
    },
    deliveries,
    logger,
  );
  await deliveryWorker.start();
  worker = deliveryWorker;

  server = app.listen(config.port, () => {
    logger.info("tether-watcher listening", {
      port: config.port,
      networkMode: config.networkMode,
      database: config.databaseUrl,
    });
  });

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

main().catch((error) => {
  console.error(errorMessage(error));
  process.exit(1);
});
