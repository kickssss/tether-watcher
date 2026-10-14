import type { Database, Queryable, Tx } from "../db/database";
import {
  claimDueDeliveries,
  deliveryExists,
  insertDelivery,
  webhookTargetsByIds,
} from "../db/statements";
import type { DeliveryRecord, DeliveryStatus, Direction, Network } from "../types";

export interface DeliveryRow {
  id: string;
  plan_id: string;
  webhook_id: string;
  network: Network;
  block_number: number;
  tx_hash: string;
  log_index: number;
  from_address: string;
  to_address: string;
  matched_address: string;
  direction: Direction;
  amount_raw: string;
  amount_decimal: string;
  token_contract: string;
  block_timestamp: number;
  payload_json: string;
  status: DeliveryStatus;
  attempt_count: number;
  max_attempts: number;
  next_attempt_at: number;
  last_http_status: number | null;
  last_error: string | null;
  delivered_at: number | null;
  created_at: number;
  updated_at: number;
}

export function toDeliveryRecord(row: DeliveryRow): DeliveryRecord {
  return {
    id: row.id,
    planId: row.plan_id,
    webhookId: row.webhook_id,
    network: row.network,
    blockNumber: row.block_number,
    txHash: row.tx_hash,
    logIndex: row.log_index,
    fromAddress: row.from_address,
    toAddress: row.to_address,
    matchedAddress: row.matched_address,
    direction: row.direction,
    amountRaw: row.amount_raw,
    amountDecimal: row.amount_decimal,
    tokenContract: row.token_contract,
    blockTimestamp: row.block_timestamp,
    payloadJson: row.payload_json,
    status: row.status,
    attemptCount: row.attempt_count,
    maxAttempts: row.max_attempts,
    nextAttemptAt: row.next_attempt_at,
    lastHttpStatus: row.last_http_status,
    lastError: row.last_error,
    deliveredAt: row.delivered_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export interface NewDelivery {
  id: string;
  planId: string;
  webhookId: string;
  network: Network;
  blockNumber: number;
  txHash: string;
  logIndex: number;
  fromAddress: string;
  toAddress: string;
  matchedAddress: string;
  direction: Direction;
  amountRaw: string;
  amountDecimal: string;
  tokenContract: string;
  blockTimestamp: number;
  payloadJson: string;
  /** When the first dispatch attempt may run; normally `now`. */
  nextAttemptAt: number;
}

/** The `deliveries` UNIQUE key, used to make re-scans idempotent. */
export interface DeliveryKey {
  webhookId: string;
  network: Network;
  txHash: string;
  logIndex: number;
  matchedAddress: string;
}

export interface ClaimedDelivery {
  id: string;
  webhookId: string;
  planId: string;
  payloadJson: string;
  attemptCount: number;
  maxAttempts: number;
  /** Null when the webhook row is gone; the worker dead-letters such rows. */
  webhookUrl: string | null;
  webhookSecret: string | null;
}

interface ClaimRow {
  id: string;
  webhook_id: string;
  plan_id: string;
  payload_json: string;
  attempt_count: number;
  max_attempts: number;
}

export interface DeliveryFilter {
  webhookId?: string;
  planId?: string;
  status?: DeliveryStatus;
  limit: number;
  offset: number;
}

/** Outbox queue access: enqueue (§8.2), atomic claim (§9.2), retention (§9.3). */
export class DeliveryRepository {
  constructor(private readonly db: Database) {}

  async insert(tx: Tx, delivery: NewDelivery): Promise<void> {
    await tx.run(insertDelivery, [
      delivery.id,
      delivery.planId,
      delivery.webhookId,
      delivery.network,
      delivery.blockNumber,
      delivery.txHash,
      delivery.logIndex,
      delivery.fromAddress,
      delivery.toAddress,
      delivery.matchedAddress,
      delivery.direction,
      delivery.amountRaw,
      delivery.amountDecimal,
      delivery.tokenContract,
      delivery.blockTimestamp,
      delivery.payloadJson,
      delivery.nextAttemptAt,
    ]);
  }

  async exists(tx: Tx, key: DeliveryKey): Promise<boolean> {
    const rows = await tx.query<{ found: number }>(deliveryExists, [
      key.webhookId,
      key.network,
      key.txHash,
      key.logIndex,
      key.matchedAddress,
    ]);
    return rows.length > 0;
  }

  /**
   * Atomically claims due deliveries for this worker instance. Runs the
   * `UPDATE … RETURNING` through `query` (libsql reports `rowsAffected === 0`
   * for RETURNING statements, so the rows themselves are the result).
   */
  async claimDue(batchSize: number): Promise<ClaimedDelivery[]> {
    const now = Date.now();
    return this.db.write(async (tx) => {
      const claimed = await tx.query<ClaimRow>(claimDueDeliveries, {
        now,
        batchSize,
      });
      if (claimed.length === 0) return [];

      const webhookIds = [...new Set(claimed.map((row) => row.webhook_id))];
      const targets = await tx.query<{ id: string; url: string; secret: string }>(
        webhookTargetsByIds,
        { idsJson: JSON.stringify(webhookIds) },
      );
      const targetsById = new Map(targets.map((target) => [target.id, target]));

      return claimed.map((row) => ({
        id: row.id,
        webhookId: row.webhook_id,
        planId: row.plan_id,
        payloadJson: row.payload_json,
        attemptCount: row.attempt_count,
        maxAttempts: row.max_attempts,
        webhookUrl: targetsById.get(row.webhook_id)?.url ?? null,
        webhookSecret: targetsById.get(row.webhook_id)?.secret ?? null,
      }));
    });
  }

  async markDelivered(id: string, httpStatus: number): Promise<void> {
    const now = Date.now();
    await this.db.write((tx) =>
      tx.run(
        `UPDATE deliveries
         SET status = 'DELIVERED', last_http_status = ?, delivered_at = ?, updated_at = ?
         WHERE id = ?`,
        [httpStatus, now, now, id],
      ),
    );
  }

  async markRetry(
    id: string,
    params: { nextAttemptAt: number; httpStatus: number | null; error: string | null },
  ): Promise<void> {
    await this.db.write((tx) =>
      tx.run(
        `UPDATE deliveries
         SET status = 'FAILED', next_attempt_at = ?, last_http_status = ?, last_error = ?, updated_at = ?
         WHERE id = ?`,
        [params.nextAttemptAt, params.httpStatus, params.error, Date.now(), id],
      ),
    );
  }

  async markDeadLetter(
    id: string,
    params: { httpStatus: number | null; error: string | null },
  ): Promise<void> {
    await this.db.write((tx) =>
      tx.run(
        `UPDATE deliveries
         SET status = 'DEAD_LETTER', last_http_status = ?, last_error = ?, updated_at = ?
         WHERE id = ?`,
        [params.httpStatus, params.error, Date.now(), id],
      ),
    );
  }

  /** Requeues rows stranded in PROCESSING by a worker crash (§9.2 step 4). */
  async recoverStaleProcessing(staleBeforeMs: number): Promise<number> {
    const now = Date.now();
    return this.db.write((tx) =>
      tx.run(
        `UPDATE deliveries
         SET status = 'FAILED', next_attempt_at = ?, updated_at = ?
         WHERE status = 'PROCESSING' AND updated_at < ?`,
        [now, now, staleBeforeMs],
      ),
    );
  }

  /** Deletes old terminal rows in bounded batches so the write lock is never held long (§9.3). */
  async prune(cutoffMs: number, batchSize = 5_000): Promise<number> {
    let total = 0;
    for (let pass = 0; pass < 100; pass++) {
      const deleted = await this.db.write((tx) =>
        tx.run(
          `DELETE FROM deliveries
           WHERE id IN (
             SELECT id FROM deliveries
             WHERE status IN ('DELIVERED', 'DEAD_LETTER') AND updated_at < ?
             LIMIT ?
           )`,
          [cutoffMs, batchSize],
        ),
      );
      total += deleted;
      if (deleted < batchSize) break;
    }
    return total;
  }

  /** Refreshes planner statistics after bulk deletes (§4.2, §9.3). */
  async optimizePlannerStats(): Promise<void> {
    await this.db.write((tx) => tx.run("PRAGMA optimize"));
  }

  async countByStatus(): Promise<Array<{ status: DeliveryStatus; count: number }>> {
    const rows = await this.db.query<{ status: DeliveryStatus; n: number }>(
      "SELECT status, COUNT(*) AS n FROM deliveries GROUP BY status",
    );
    return rows.map((row) => ({ status: row.status, count: Number(row.n) }));
  }

  async list(filter: DeliveryFilter): Promise<DeliveryRecord[]> {
    const conditions: string[] = [];
    const args: Array<string | number> = [];
    if (filter.webhookId) {
      conditions.push("webhook_id = ?");
      args.push(filter.webhookId);
    }
    if (filter.planId) {
      conditions.push("plan_id = ?");
      args.push(filter.planId);
    }
    if (filter.status) {
      conditions.push("status = ?");
      args.push(filter.status);
    }
    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    args.push(filter.limit, filter.offset);
    const rows = await this.db.query<DeliveryRow>(
      `SELECT * FROM deliveries ${where} ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`,
      args,
    );
    return rows.map(toDeliveryRecord);
  }
}

/** Reads delivery rows through any read surface (used by tests and stats endpoints). */
export async function queryDeliveries(
  reader: Queryable,
  sql: string,
  args: Array<string | number> = [],
): Promise<DeliveryRecord[]> {
  const rows = await reader.query<DeliveryRow>(sql, args);
  return rows.map(toDeliveryRecord);
}
