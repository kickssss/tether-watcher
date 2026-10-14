/**
 * Ordered, append-only migrations. Each entry runs once, inside a single write
 * transaction, and bumps `PRAGMA user_version` (§11).
 *
 * Schema mirrors PRD §5.2. Statements are split individually because the libsql
 * driver executes one statement per call.
 */
export interface Migration {
  version: number;
  name: string;
  statements: string[];
}

export const MIGRATIONS: Migration[] = [
  {
    version: 1,
    name: "initial schema",
    statements: [
      // 1. Subscription plans (manually assigned quota records).
      `CREATE TABLE IF NOT EXISTS subscription_plans (
        id                      TEXT PRIMARY KEY,
        name                    TEXT NOT NULL,
        max_webhooks            INTEGER NOT NULL CHECK (max_webhooks >= 0),
        max_addresses           INTEGER NOT NULL CHECK (max_addresses >= 0),
        max_deliverable_events  INTEGER NOT NULL CHECK (max_deliverable_events >= 0),

        used_webhooks_count     INTEGER NOT NULL DEFAULT 0 CHECK (used_webhooks_count >= 0),
        used_addresses_count    INTEGER NOT NULL DEFAULT 0 CHECK (used_addresses_count >= 0),
        delivered_events_count  INTEGER NOT NULL DEFAULT 0 CHECK (delivered_events_count >= 0),
        dropped_events_count    INTEGER NOT NULL DEFAULT 0 CHECK (dropped_events_count >= 0),

        is_active               INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
        created_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 ),
        updated_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 )
      ) STRICT`,

      // 2. Webhook endpoints.
      `CREATE TABLE IF NOT EXISTS webhooks (
        id                      TEXT PRIMARY KEY,
        plan_id                 TEXT NOT NULL,
        url                     TEXT NOT NULL,
        secret                  TEXT NOT NULL,
        description             TEXT,
        network_filter          TEXT CHECK (network_filter IN ('TRC20', 'BEP20', 'ALL')) DEFAULT 'ALL',
        is_active               INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
        created_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 ),
        updated_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 ),
        FOREIGN KEY (plan_id) REFERENCES subscription_plans(id) ON DELETE RESTRICT
      ) STRICT`,

      `CREATE INDEX IF NOT EXISTS idx_webhooks_plan_id ON webhooks(plan_id)`,

      // 3. Watched addresses: clustered (network, address, webhook_id) B-Tree, zero heap lookups.
      `CREATE TABLE IF NOT EXISTS webhook_addresses (
        network                 TEXT NOT NULL CHECK (network IN ('TRC20', 'BEP20')),
        address                 TEXT NOT NULL,
        webhook_id              TEXT NOT NULL,
        plan_id                 TEXT NOT NULL,
        created_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 ),
        PRIMARY KEY (network, address, webhook_id),
        FOREIGN KEY (webhook_id) REFERENCES webhooks(id) ON DELETE CASCADE,
        FOREIGN KEY (plan_id) REFERENCES subscription_plans(id) ON DELETE RESTRICT
      ) WITHOUT ROWID, STRICT`,

      `CREATE INDEX IF NOT EXISTS idx_webhook_addresses_webhook_id ON webhook_addresses(webhook_id)`,
      `CREATE INDEX IF NOT EXISTS idx_webhook_addresses_plan_id ON webhook_addresses(plan_id)`,

      // 4. Per-network block checkpoints + single-scanner lease (§8.3).
      `CREATE TABLE IF NOT EXISTS block_checkpoints (
        network                 TEXT PRIMARY KEY CHECK (network IN ('TRC20', 'BEP20')),
        last_processed_block    INTEGER NOT NULL CHECK (last_processed_block >= 0),
        last_block_hash         TEXT,
        last_block_timestamp    INTEGER,
        lease_owner             TEXT,
        lease_expires_at        INTEGER,
        updated_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 )
      ) WITHOUT ROWID, STRICT`,

      // 5. Transactional outbox: matched events awaiting webhook dispatch.
      `CREATE TABLE IF NOT EXISTS deliveries (
        id                      TEXT PRIMARY KEY,
        plan_id                 TEXT NOT NULL,
        webhook_id              TEXT NOT NULL,
        network                 TEXT NOT NULL CHECK (network IN ('TRC20', 'BEP20')),
        block_number            INTEGER NOT NULL,
        tx_hash                 TEXT NOT NULL,
        log_index               INTEGER NOT NULL,
        from_address            TEXT NOT NULL,
        to_address              TEXT NOT NULL,
        matched_address         TEXT NOT NULL,
        direction               TEXT NOT NULL CHECK (direction IN ('INCOMING', 'OUTGOING')),
        amount_raw              TEXT NOT NULL,
        amount_decimal          TEXT NOT NULL,
        token_contract          TEXT NOT NULL,
        block_timestamp         INTEGER NOT NULL,
        payload_json            TEXT NOT NULL,

        status                  TEXT NOT NULL DEFAULT 'PENDING'
                                CHECK (status IN ('PENDING', 'PROCESSING', 'DELIVERED', 'FAILED', 'DEAD_LETTER')),
        attempt_count           INTEGER NOT NULL DEFAULT 0,
        max_attempts            INTEGER NOT NULL DEFAULT 5,
        next_attempt_at         INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 ),
        last_http_status        INTEGER,
        last_error              TEXT,
        delivered_at            INTEGER,
        created_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 ),
        updated_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 ),

        FOREIGN KEY (webhook_id) REFERENCES webhooks(id) ON DELETE CASCADE,
        FOREIGN KEY (plan_id) REFERENCES subscription_plans(id) ON DELETE CASCADE,

        UNIQUE (webhook_id, network, tx_hash, log_index, matched_address)
      ) STRICT`,

      // Worker polling index: tiny, because it only covers live rows.
      `CREATE INDEX IF NOT EXISTS idx_deliveries_worker_poll
        ON deliveries (next_attempt_at ASC, id ASC)
        WHERE status IN ('PENDING', 'FAILED')`,

      `CREATE INDEX IF NOT EXISTS idx_deliveries_webhook_status ON deliveries (webhook_id, status)`,
      `CREATE INDEX IF NOT EXISTS idx_deliveries_block_number ON deliveries (network, block_number)`,

      // Stale-PROCESSING recovery sweep (§9.2 step 4) without an O(table) scan.
      `CREATE INDEX IF NOT EXISTS idx_deliveries_processing_recovery
        ON deliveries (updated_at)
        WHERE status = 'PROCESSING'`,
    ],
  },
];
