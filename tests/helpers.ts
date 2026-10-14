import { createServer } from "http";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import type { Logger } from "winston";
import { Database } from "../src/db/database";
import { ProviderError } from "../src/errors";
import { CheckpointRepository } from "../src/repositories/CheckpointRepository";
import { DeliveryRepository } from "../src/repositories/DeliveryRepository";
import { PlanRepository } from "../src/repositories/PlanRepository";
import { WebhookRepository } from "../src/repositories/WebhookRepository";
import {
  BlockScannerService,
  type ScannerOptions,
} from "../src/services/BlockScannerService";
import { DeliveryWorkerService } from "../src/services/DeliveryWorkerService";
import type { BlockProvider } from "../src/services/providers/BlockProvider";
import type { BlockData, Network } from "../src/types";
import { sleep } from "../src/utils/async";
import { createAppLogger } from "../src/utils/logger";

export const USDT_TRC20 = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t";
export const USDT_BEP20 = "0x55d398326f99059ff775485246999027b3197955";
export const TRON_ADDRESS = "TXYZopYRdj2D9XRtbG411XZZ3kM5VkAeBf";
export const TRON_OTHER_ADDRESS = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t";
export const BSC_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678";
export const BSC_OTHER_ADDRESS = "0x55d398326f99059ff775485246999027b3197955";

export interface TestDatabase {
  db: Database;
  close: () => Promise<void>;
}

export async function openTestDatabase(): Promise<TestDatabase> {
  const directory = mkdtempSync(join(tmpdir(), "tether-watcher-test-"));
  const db = await Database.open(`file:${join(directory, "test.db")}`);
  return {
    db,
    close: async () => {
      await db.close();
      rmSync(directory, { recursive: true, force: true });
    },
  };
}

export function silentLogger(): Logger {
  return createAppLogger({ level: "error", toFiles: false, silent: true });
}

export class FakeBlockProvider implements BlockProvider {
  latestBlockNumber = 0;
  readonly blocks = new Map<number, BlockData>();

  constructor(readonly network: Network) {}

  async getLatestBlockNumber(): Promise<number> {
    return this.latestBlockNumber;
  }

  async getBlock(blockNumber: number): Promise<BlockData> {
    const block = this.blocks.get(blockNumber);
    if (!block) {
      throw new ProviderError(`fake provider has no block ${blockNumber}`);
    }
    return block;
  }
}

export interface ReceivedRequest {
  body: string;
  headers: Record<string, string | string[] | undefined>;
}

export interface TestReceiver {
  url: string;
  received: ReceivedRequest[];
  close: () => Promise<void>;
}

/** Minimal HTTP receiver: records raw bodies/headers and answers with scripted statuses. */
export async function startReceiver(
  statusForRequest: (requestNumber: number) => number,
): Promise<TestReceiver> {
  const received: ReceivedRequest[] = [];
  const server = createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      received.push({
        body: Buffer.concat(chunks).toString("utf8"),
        headers: request.headers,
      });
      response.writeHead(statusForRequest(received.length));
      response.end("ok");
    });
  });

  const listening = Promise.withResolvers<void>();
  server.listen(0, "127.0.0.1", listening.resolve);
  await listening.promise;

  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("test receiver failed to bind a port");
  }

  return {
    url: `http://127.0.0.1:${address.port}/hook`,
    received,
    close: async () => {
      const closed = Promise.withResolvers<void>();
      server.close(() => closed.resolve());
      await closed.promise;
    },
  };
}

/**
 * Polls `check` until it returns a truthy value. The pipeline tests exercise the real
 * scanner/worker poll loops (and real HTTP), which cannot run on vitest's fake clock,
 * so these tests observe the platform clock; the wait ends as soon as the condition holds.
 */
export async function waitFor<T>(
  check: () => Promise<T | null | undefined | false>,
  timeoutMs = 8_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await sleep(25);
  }
}

export interface TestContext extends TestDatabase {
  db: Database;
  logger: Logger;
  plans: PlanRepository;
  webhooks: WebhookRepository;
  deliveries: DeliveryRepository;
  checkpoints: CheckpointRepository;
  makeScanner(
    provider: BlockProvider,
    overrides?: Partial<ScannerOptions>,
  ): BlockScannerService;
  makeWorker(): DeliveryWorkerService;
}

/** Everything a pipeline test needs: an isolated database plus wired repositories. */
export async function createPipeline(): Promise<TestContext> {
  const context = await openTestDatabase();
  const logger = silentLogger();
  const plans = new PlanRepository(context.db);
  const webhooks = new WebhookRepository(context.db, plans);
  const deliveries = new DeliveryRepository(context.db);
  const checkpoints = new CheckpointRepository(context.db);

  return {
    ...context,
    logger,
    plans,
    webhooks,
    deliveries,
    checkpoints,
    makeScanner: (provider, overrides = {}) =>
      new BlockScannerService(
        {
          network: provider.network,
          confirmations: 1,
          tokenContract:
            provider.network === "TRC20" ? USDT_TRC20 : USDT_BEP20,
          decimals: provider.network === "TRC20" ? 6 : 18,
          startBlock: null,
          idleIntervalMs: 20,
          instanceId: `scanner-${provider.network}`,
          networkMode: "mainnet",
          ...overrides,
        },
        provider,
        { db: context.db, plans, deliveries, checkpoints, logger },
      ),
    makeWorker: () =>
      new DeliveryWorkerService(
        {
          pollIntervalMs: 20,
          batchSize: 10,
          concurrency: 2,
          httpTimeoutMs: 2_000,
          backoffBaseMs: 5,
          retentionDays: 30,
          pruneIntervalMs: 60_000,
        },
        deliveries,
        logger,
      ),
  };
}
