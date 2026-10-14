import { z } from "zod";
import { NETWORKS, type Network } from "./types";

const enabled = (defaultValue: boolean) =>
  z
    .enum(["true", "false"])
    .default(String(defaultValue) as "true" | "false")
    .transform((value) => value === "true");

/** Optional integer env var; empty strings (typical for unset dotenv entries) count as absent. */
const optionalInt = z.preprocess(
  (value) => (value === "" || value === undefined ? undefined : value),
  z.coerce.number().int().min(0).optional(),
);

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "production", "test"]).default("development"),
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  API_KEY: z.string().min(1, "API_KEY is required"),
  DATABASE_URL: z.string().min(1).default("file:./data/tether-watcher.db"),
  NETWORK_MODE: z.enum(["mainnet", "testnet"]).default("mainnet"),
  LOG_LEVEL: z.enum(["error", "warn", "info", "debug"]).default("info"),

  TRC20_ENABLED: enabled(true),
  TRC20_CONFIRMATIONS: z.coerce.number().int().min(1).default(20),
  TRC20_TOKEN_CONTRACT: z.string().default("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"),
  TRC20_DECIMALS: z.coerce.number().int().min(0).max(30).default(6),
  TRC20_START_BLOCK: optionalInt,
  TRONGRID_URL: z.string().url().default("https://api.trongrid.io"),
  TRON_API_KEY: z
    .string()
    .optional()
    .transform((value) => value || null),

  BEP20_ENABLED: enabled(true),
  BEP20_CONFIRMATIONS: z.coerce.number().int().min(1).default(12),
  BEP20_TOKEN_CONTRACT: z
    .string()
    .default("0x55d398326f99059ff775485246999027b3197955"),
  BEP20_DECIMALS: z.coerce.number().int().min(0).max(30).default(18),
  BEP20_START_BLOCK: optionalInt,
  BSC_RPC_URL: z.string().url().default("https://bsc-dataseed.bnbchain.org"),

  SCANNER_IDLE_INTERVAL_MS: z.coerce.number().int().min(100).default(3000),
  WORKER_POLL_INTERVAL_MS: z.coerce.number().int().min(50).default(500),
  WORKER_BATCH_SIZE: z.coerce.number().int().min(1).max(1000).default(50),
  WORKER_CONCURRENCY: z.coerce.number().int().min(1).max(200).default(20),
  WORKER_HTTP_TIMEOUT_MS: z.coerce.number().int().min(100).default(5000),
  WORKER_BACKOFF_BASE_MS: z.coerce.number().int().min(1).default(2000),
  DELIVERY_RETENTION_DAYS: z.coerce.number().int().min(1).default(30),
  PRUNE_INTERVAL_MS: z.coerce.number().int().min(60_000).default(86_400_000),
});

export interface NetworkConfig {
  network: Network;
  enabled: boolean;
  /** Confirmation depth required before a block is scanned (§8.1). */
  confirmations: number;
  tokenContract: string;
  decimals: number;
  /** When set, a fresh database starts at `startBlock - 1` instead of the safe head. */
  startBlock: number | null;
}

export interface Config {
  nodeEnv: "development" | "production" | "test";
  port: number;
  apiKey: string;
  databaseUrl: string;
  networkMode: "mainnet" | "testnet";
  logLevel: "error" | "warn" | "info" | "debug";
  tron: { url: string; apiKey: string | null };
  bsc: { rpcUrl: string };
  networks: Record<Network, NetworkConfig>;
  scanner: { idleIntervalMs: number };
  worker: {
    pollIntervalMs: number;
    batchSize: number;
    concurrency: number;
    httpTimeoutMs: number;
    backoffBaseMs: number;
  };
  retention: { days: number; pruneIntervalMs: number };
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = envSchema.safeParse(env);
  if (!parsed.success) {
    const details = parsed.error.issues
      .map((issue) => `${issue.path.join(".")}: ${issue.message}`)
      .join("; ");
    throw new Error(`invalid configuration: ${details}`);
  }
  const e = parsed.data;

  const networks = {
    TRC20: {
      network: "TRC20" as const,
      enabled: e.TRC20_ENABLED,
      confirmations: e.TRC20_CONFIRMATIONS,
      tokenContract: e.TRC20_TOKEN_CONTRACT.trim(),
      decimals: e.TRC20_DECIMALS,
      startBlock: e.TRC20_START_BLOCK ?? null,
    },
    BEP20: {
      network: "BEP20" as const,
      enabled: e.BEP20_ENABLED,
      confirmations: e.BEP20_CONFIRMATIONS,
      tokenContract: e.BEP20_TOKEN_CONTRACT.trim().toLowerCase(),
      decimals: e.BEP20_DECIMALS,
      startBlock: e.BEP20_START_BLOCK ?? null,
    },
  };
  for (const network of NETWORKS) {
    if (networks[network].enabled && networks[network].confirmations < 1) {
      throw new Error(`${network}: confirmations must be >= 1`);
    }
  }

  return {
    nodeEnv: e.NODE_ENV,
    port: e.PORT,
    apiKey: e.API_KEY,
    databaseUrl: e.DATABASE_URL,
    networkMode: e.NETWORK_MODE,
    logLevel: e.LOG_LEVEL,
    tron: { url: e.TRONGRID_URL.replace(/\/+$/, ""), apiKey: e.TRON_API_KEY },
    bsc: { rpcUrl: e.BSC_RPC_URL },
    networks,
    scanner: { idleIntervalMs: e.SCANNER_IDLE_INTERVAL_MS },
    worker: {
      pollIntervalMs: e.WORKER_POLL_INTERVAL_MS,
      batchSize: e.WORKER_BATCH_SIZE,
      concurrency: e.WORKER_CONCURRENCY,
      httpTimeoutMs: e.WORKER_HTTP_TIMEOUT_MS,
      backoffBaseMs: e.WORKER_BACKOFF_BASE_MS,
    },
    retention: { days: e.DELIVERY_RETENTION_DAYS, pruneIntervalMs: e.PRUNE_INTERVAL_MS },
  };
}
