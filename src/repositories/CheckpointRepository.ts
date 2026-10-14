import type { Database, Tx } from "../db/database";
import {
  acquireScannerLease,
  releaseScannerLease,
  renewScannerLease,
  upsertCheckpoint,
} from "../db/statements";
import type { BlockCheckpoint, Network } from "../types";

export interface CheckpointRow {
  network: Network;
  last_processed_block: number;
  last_block_hash: string | null;
  last_block_timestamp: number | null;
  lease_owner: string | null;
  lease_expires_at: number | null;
  updated_at: number;
}

export function toBlockCheckpoint(row: CheckpointRow): BlockCheckpoint {
  return {
    network: row.network,
    lastProcessedBlock: row.last_processed_block,
    lastBlockHash: row.last_block_hash,
    lastBlockTimestamp: row.last_block_timestamp,
    leaseOwner: row.lease_owner,
    leaseExpiresAt: row.lease_expires_at,
    updatedAt: row.updated_at,
  };
}

/** Scanner lease lifetime and heartbeat cadence (§8.3). */
export const LEASE_TTL_MS = 30_000;
export const LEASE_RENEW_INTERVAL_MS = 10_000;

/**
 * `block_checkpoints` access, including the single-scanner lease that makes the
 * "each block is scanned exactly once" invariant enforceable (§8.3).
 */
export class CheckpointRepository {
  constructor(private readonly db: Database) {}

  async get(network: Network): Promise<BlockCheckpoint | null> {
    const row = await this.db.queryOne<CheckpointRow>(
      "SELECT * FROM block_checkpoints WHERE network = ?",
      [network],
    );
    return row ? toBlockCheckpoint(row) : null;
  }

  /**
   * Creates the checkpoint row on first boot and takes the lease in one statement.
   * `false` means another live scanner holds it.
   */
  async acquireLease(params: {
    network: Network;
    instanceId: string;
    initialBlock: number;
    ttlMs?: number;
  }): Promise<boolean> {
    const now = Date.now();
    return this.db.write(async (tx) => {
      const affected = await tx.run(acquireScannerLease, {
        network: params.network,
        instanceId: params.instanceId,
        initialBlock: params.initialBlock,
        leaseExpiresAt: now + (params.ttlMs ?? LEASE_TTL_MS),
        now,
      });
      return affected === 1;
    });
  }

  /** Heartbeat; `false` means the lease was taken over and the scanner must stop. */
  async renewLease(
    network: Network,
    instanceId: string,
    ttlMs: number = LEASE_TTL_MS,
  ): Promise<boolean> {
    const now = Date.now();
    return this.db.write(async (tx) => {
      const affected = await tx.run(renewScannerLease, [
        now + ttlMs,
        now,
        network,
        instanceId,
      ]);
      return affected === 1;
    });
  }

  async releaseLease(network: Network, instanceId: string): Promise<void> {
    await this.db.write((tx) =>
      tx.run(releaseScannerLease, [Date.now(), network, instanceId]),
    );
  }

  /** Advances the cursor to `blockNumber` inside the caller's block transaction (§8.2). */
  async commitBlock(
    tx: Tx,
    params: {
      network: Network;
      blockNumber: number;
      blockHash: string;
      blockTimestamp: number;
    },
  ): Promise<void> {
    await tx.run(upsertCheckpoint, [
      params.network,
      params.blockNumber,
      params.blockHash,
      params.blockTimestamp,
      Date.now(),
    ]);
  }
}
