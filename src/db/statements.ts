/**
 * Hot-path SQL shared by the scanner, the worker, and the repositories
 * (§6.3 address matching, §7.2 quota accounting, §8.2 per-block commit,
 * §8.3 scanner lease, §9.2 delivery worker).
 *
 * Named parameters (`:name`) are passed as an object, e.g. `{ network: "TRC20" }`.
 */

export const matchBlockAddresses = `
  SELECT
    wa.address            AS address,
    wa.webhook_id         AS webhook_id,
    wa.plan_id            AS plan_id,
    w.url                 AS webhook_url,
    w.secret              AS webhook_secret
  FROM json_each(:addressesJson) AS batch
  CROSS JOIN webhook_addresses AS wa
    ON wa.network = :network
   AND wa.address = batch.value
  INNER JOIN webhooks AS w
    ON w.id = wa.webhook_id
   AND w.is_active = 1
   AND (w.network_filter = 'ALL' OR w.network_filter = :network)
  INNER JOIN subscription_plans AS sp
    ON sp.id = wa.plan_id
   AND sp.is_active = 1`;

/**
 * Reserves one deliverable event for a plan. `rowsAffected === 1` means the quota
 * was available; `0` means the plan is exhausted or inactive (§7.2).
 */
export const tryConsumeEventQuota = `
  UPDATE subscription_plans
  SET delivered_events_count = delivered_events_count + 1,
      updated_at = ?
  WHERE id = ?
    AND is_active = 1
    AND delivered_events_count < max_deliverable_events`;

export const incrementDroppedEvents = `
  UPDATE subscription_plans
  SET dropped_events_count = dropped_events_count + 1,
      updated_at = ?
  WHERE id = ?`;

export const insertDelivery = `
  INSERT INTO deliveries (
    id, plan_id, webhook_id, network, block_number, tx_hash, log_index,
    from_address, to_address, matched_address, direction, amount_raw,
    amount_decimal, token_contract, block_timestamp, payload_json, next_attempt_at
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

/** Idempotency probe against the UNIQUE (webhook, network, tx, log, matched address) key. */
export const deliveryExists = `
  SELECT 1 AS found
  FROM deliveries
  WHERE webhook_id = ? AND network = ? AND tx_hash = ? AND log_index = ? AND matched_address = ?
  LIMIT 1`;

export const upsertCheckpoint = `
  INSERT INTO block_checkpoints (
    network, last_processed_block, last_block_hash, last_block_timestamp, updated_at
  ) VALUES (?, ?, ?, ?, ?)
  ON CONFLICT(network) DO UPDATE SET
    last_processed_block = excluded.last_processed_block,
    last_block_hash = excluded.last_block_hash,
    last_block_timestamp = excluded.last_block_timestamp,
    updated_at = excluded.updated_at`;

/**
 * Acquires the per-network scanner lease, initializing the checkpoint row on first boot.
 * `rowsAffected === 0` means another live scanner holds the lease (§8.3).
 */
export const acquireScannerLease = `
  INSERT INTO block_checkpoints (
    network, last_processed_block, last_block_hash, last_block_timestamp,
    lease_owner, lease_expires_at, updated_at
  ) VALUES (:network, :initialBlock, NULL, NULL, :instanceId, :leaseExpiresAt, :now)
  ON CONFLICT(network) DO UPDATE SET
    lease_owner = :instanceId,
    lease_expires_at = :leaseExpiresAt,
    updated_at = :now
  WHERE block_checkpoints.lease_owner IS NULL
     OR block_checkpoints.lease_owner = :instanceId
     OR block_checkpoints.lease_expires_at < :now`;

export const renewScannerLease = `
  UPDATE block_checkpoints
  SET lease_expires_at = ?, updated_at = ?
  WHERE network = ? AND lease_owner = ?`;

export const releaseScannerLease = `
  UPDATE block_checkpoints
  SET lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
  WHERE network = ? AND lease_owner = ?`;

/**
 * §9.2 atomic batch claim. Uses `idx_deliveries_worker_poll` and returns the claimed rows
 * (read `rows.length`: libsql reports `rowsAffected === 0` for RETURNING statements).
 */
export const claimDueDeliveries = `
  UPDATE deliveries
  SET status = 'PROCESSING',
      attempt_count = attempt_count + 1,
      updated_at = :now
  WHERE id IN (
    SELECT id FROM deliveries
    WHERE status IN ('PENDING', 'FAILED')
      AND next_attempt_at <= :now
    ORDER BY next_attempt_at ASC, id ASC
    LIMIT :batchSize
  )
  RETURNING id, webhook_id, plan_id, payload_json, attempt_count, max_attempts`;

/** The worker joins only the webhook target it needs to POST to. */
export const webhookTargetsByIds = `
  SELECT id, url, secret FROM webhooks WHERE id IN (SELECT value FROM json_each(:idsJson))`;
