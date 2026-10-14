# Product Requirements Document: Converting Tether-Watcher to High-Performance Stateful MySQL 8 Architecture

**Target Repository:** [moha-abdi/tether-watcher](https://github.com/moha-abdi/tether-watcher) (formerly `TronWatch`; canonical remote `git@github.com:moha-abdi/tether-watcher.git`)

**Document Version:** 1.2 (MySQL 8.0 / 8.4 LTS Edition)

**Target Stack:** Node.js v20 LTS, TypeScript 5.x, Express.js, `mysql2` (`mysql2/promise@^3.11`) backed by MySQL 8.0.30+ or MySQL 8.4 LTS (`InnoDB` storage engine), TronWeb, Ethers.js v6, Axios, Zod

**Revision 1.1 (MySQL architecture design pass):** §2 audit preserved against the actual source tree (`72c5fe0`); §4 replaces embedded SQLite pragmas with `mysql2/promise` connection pooling, binary-protocol prepared statements, and `InnoDB` tuning (`READ COMMITTED` isolation, buffer pool sizing, and redo log capacity); §5 converts SQLite `WITHOUT ROWID, STRICT` tables into `ENGINE=InnoDB` clustered primary key tables with strict SQL mode, binary `ascii_bin` collations for zero-overhead case-sensitive address/hash indexing, and generated virtual queue columns to replace SQLite partial indexes; §6.3 replaces `json_each()` + `CROSS JOIN` with MySQL 8's `JSON_TABLE()` + `STRAIGHT_JOIN` (`/*+ JOIN_ORDER(batch, wa, w, sp) */`); §8.2–§8.3 adapt atomic block commits to single-roundtrip batched/procedural InnoDB transactions and dual-layer scanner leases; §9.2 upgrades worker queue claiming to MySQL 8's lock-free `SELECT ... FOR UPDATE OF d SKIP LOCKED`.

**Revision 1.2 (documentation-verified corrections):** the MySQL-specific claims were re-checked against the MySQL 8.4 Reference Manual and the `mysql2` source. Fixed: unsigned underflow guard in §7.2/§10.2 (`GREATEST(0, x - ?)` cannot clamp an error raised by the subtraction itself → `x - LEAST(x, ?)`); `INSERT IGNORE` replaced with the `ON DUPLICATE KEY UPDATE` no-op form (§7.2, §8.3, §10.2, §11) because `IGNORE` also suppresses truncation/NOT-NULL errors and would count a silently truncated row as inserted; `bigNumberStrings` contradiction between §4.1 and §4.2 resolved to `false` with the ±2^53 rationale; §4.1's "never stall" claim narrowed to the actual per-row lock contention; `sync_binlog = 0` replication caveat added (§4.2); `DELIMITER` handling for the migration runner documented (§8.2); unmeasured latency figures labelled as targets (§6.3). Two items previously flagged as suspect were verified as **correct** and left unchanged: `ENUM` procedure parameters are valid ("type: any valid MySQL data type"), and `mysql2` already defaults to utf8mb4 (`Charsets.UTF8MB4_UNICODE_CI`). Open items that require a live server are collected in §12.3.

## 1. Executive Summary: Stateful InnoDB Persistence Decouples Block Scanning from Webhook Delivery

The existing [moha-abdi/tether-watcher](https://github.com/moha-abdi/tether-watcher) service monitors USDT transactions across **TRON (TRC20)** and **BNB Smart Chain (BEP20)** using an entirely stateless, in-memory design. When the Express server boots (`src/index.ts`), `TetherWatcher` (`src/services/TetherWatcher.ts`) initializes in-memory `Map` structures for *watched transactions* (`watchedTransactions`, `TetherWatcher.ts:96` — each entry holds an exact `fromAddress` + `expectedRecipientAddress` + `amount` triple, a 5-minute `expiresAt` and `maxAttempts = 3`) and for *processed events* (`processedEvents`, `:97`), initializes per-network cursors to zero (`lastProcessedBlocks`, `:98–101`), then starts two polling loops that dispatch webhooks inline via Axios (`src/utils/webhook.ts`). TRON polls TronGrid's *latest confirmed* Transfer-event window rather than an explicit block range (`onlyConfirmed: true`, `:303`); BSC scans `max(currentBlock - 100, cursor)` on every tick (`:340–343`). Any process restart or crash wipes all registered watch targets and loses block progress; webhook dispatch failures are caught and only logged — they are never retried (`src/utils/webhook.ts`).

This PRD defines the architectural conversion of [moha-abdi/tether-watcher](https://github.com/moha-abdi/tether-watcher) into a stateful, crash-resilient, high-throughput blockchain indexing and webhook delivery engine backed by **MySQL 8.0+ / 8.4 LTS (`InnoDB`)**. The redesigned system satisfies four core operational mandates:

1. **Zero Full-Memory Address Loading with Low-Latency InnoDB Clustered Lookups:** All watched addresses and webhook associations reside in MySQL `InnoDB` tables clustered directly on `PRIMARY KEY (network, address, webhook_id)` using single-byte `ascii_bin` encoding. Block scanners never load the full address table into Node.js memory; instead, they execute single-roundtrip block-level batch queries (`JSON_TABLE` with forced `STRAIGHT_JOIN` order) against InnoDB buffer-pool B+Tree pages.

2. **Atomic Block Checkpointing for Crash Recovery:** After every fully scanned block, the scanner commits the block number, matched delivery records, and quota updates in a single atomic MySQL transaction (`START TRANSACTION ... COMMIT`). Upon restart after a crash, the scanner resumes at `last_processed_block + 1` rather than the current head block.

3. **Subscription Plan Quota Enforcement:** Manually assigned subscription plans govern the maximum number of webhooks (`max_webhooks`), maximum assigned addresses (`max_addresses`, counting duplicate addresses across separate webhooks distinctly), and maximum deliverable events (`max_deliverable_events`). Events matching a subscription that has exhausted its event quota are ignored and recorded as dropped (`dropped_events_count`).

4. **Decoupled Transactional Outbox (`deliveries` Table) and Concurrent Workers:** Block scanning writes matched events in a standardized schema to the `deliveries` table and immediately advances to the next block. Dedicated asynchronous Delivery Workers poll due deliveries using MySQL 8's `SELECT ... FOR UPDATE SKIP LOCKED`, sign payloads via HMAC-SHA256 (`src/utils/generateSignature.ts`), manage retries with exponential backoff, and update delivery state independently without blocking scanners or contending on locked rows.

## 2. Current Architecture Audit: In-Memory Bottlenecks and Failure Modes in Tether-Watcher

An audit of the [moha-abdi/tether-watcher](https://github.com/moha-abdi/tether-watcher) codebase (verified at commit `72c5fe0`, branch `main`) highlights the exact modules that require refactoring to support persistent MySQL state, subscription quotas, and high-throughput address matching. **The current service watches individual transactions (exact sender + recipient + amount), not addresses — the target design below is an address-subscription redesign, not a storage swap (see §2.1).**

| Existing File / Component | Current Behavior at `72c5fe0` | Architectural Gap / Failure Mode | Required Target State in MySQL 8 Conversion | 
| ----- | ----- | ----- | ----- | 
| `src/services/TetherWatcher.ts` (`watchedTransactions` Map, `:96`) | Holds `WatchedTransaction` records keyed by a caller-supplied `id`: exact `fromAddress` + `expectedRecipientAddress` + `amount` (smallest unit), `webhookUrl`, `expiresAt = now + 5 min` (`:280`), `maxAttempts = 3` (`:282`). Every Transfer event is matched by scanning the entire Map (`processTransferEvent`, `:437` onward). No address-indexed structure and no multi-address subscription exist. | Watch targets and webhook URLs vanish on restart; per-event matching cost is O(watched transactions); one watched address per watch entry (either side); the 5-minute expiry ages out slow confirmations. | Replace with `webhooks` + `webhook_addresses` (`ENGINE=InnoDB`, clustered `PRIMARY KEY (network, address, webhook_id)`) queried by the forced-order `JSON_TABLE` + `STRAIGHT_JOIN` batch statement in §6.3. | 
| `src/services/TetherWatcher.ts` (`lastProcessedBlocks`, `:98–101`; exposed read-only via `getNetworkStatus`/`getLastProcessedBlock`) | Private `Record<Network, bigint>` initialized to `0`. TRON writes the head height after polling the latest confirmed event window (`:316`); BSC scans `max(currentBlock - 100, cursor)` (`:340–343`) and then writes the head (`:356`). It is not a `NetworkConfig` field. | Nothing survives a restart: BSC restarts at `currentBlock - 100` (\~5 min of blocks), TRON re-reads TronGrid's rolling confirmed-event window; downtime beyond that window is never scanned. The overlapping 100-block lookback also re-processes events on every tick. | Persist the per-network cursor in `block_checkpoints` with a single-scanner lease; advance it only inside the atomic per-block InnoDB commit (§8.2, §8.3). | 
| `src/services/TetherWatcher.ts` (`processedEvents` Map, `:97`) | Dedupe keys `TRON:<txid>:<eventIndex>` / `BSC:<txHash>:<logIndex>`; the maintenance sweep runs every 60 s (`:217–223`) but evicts only entries older than 24 h (`:737–743`). | A crash forgets every processed event, so re-scanned blocks can re-notify; resident memory tracks a rolling 24 h of block volume; TTL eviction silently re-enables duplicates after 24 h. | Enforce idempotency persistently via the `deliveries` `UNIQUE KEY uq_deliveries_idempotency (webhook_id, network, tx_hash, log_index, matched_address)` constraint plus the §8.2 batch dedupe guard. | 
| `src/utils/webhook.ts` (inline Axios dispatch) | `sendWebhook()` awaits `axios.post` directly inside the polling loops (`finalizeTransaction`, `handleFailedTransaction`, `cleanupExpiredTransactions`); errors are caught and logged — no retry, no persistence. The maintenance loop's `retryFailedTransactions()` (`:746`) only re-arms transaction *verification*; it never re-sends a webhook. | A slow or timing-out endpoint stalls the block scanner; a failed notification is lost permanently; no delivery audit trail. | Decouple via the `deliveries` transactional outbox drained by `DeliveryWorkerService` using `FOR UPDATE SKIP LOCKED` (§3, §9). | 
| `src/routes/watcherRoutes.ts` | 9 routes, API-key protected except `GET /health`: `POST /watch`, `DELETE /watch/:id`, `GET /watch/:id`, `GET /networks`, `GET /networks/:network`, `PATCH /admin/networks/:network`, `POST /admin/networks/:network/enable`, `POST /admin/networks/:network/disable`, `GET /stats`. Zod validates the transactional watch payload (`watchSchema`). | No plans, quotas, webhook/address CRUD, or delivery inspection; the entire surface is bound to the per-transaction model. | Replace with the plan/webhook/address/delivery API in §10; see the route migration table in §10.3. | 

### 2.1 Semantic Migration: Transaction Watches → Address Subscriptions

The target design changes *what* is watched, so this is a breaking redesign of the watch contract rather than a drop-in storage change:

| Legacy concept (`WatchedTransaction`, TRC20/BEP20) | Target equivalent | Notes | 
| ----- | ----- | ----- | 
| `id` (caller-supplied watch ID) | `webhooks.id` (server-generated ULID/UUID) | Callers manage a webhook, not a one-off watch. | 
| `fromAddress` / `expectedRecipientAddress` | two `webhook_addresses` rows (one per address) | Matching becomes address-based (`from` or `to`), not an exact triple; one transfer can now match several subscriptions (counted per pair, §7.1). | 
| `amount` (exact smallest-unit match) | dropped | Amounts no longer gate delivery. An optional per-address minimum-amount filter is a possible follow-up and is explicitly **out of scope** here. | 
| `webhookUrl` (per watch) | `webhooks.url` + per-webhook `secret` | Signing switches from the global `WEBHOOK_SECRET` to per-webhook secrets; `src/utils/generateSignature.ts` must accept the secret as an argument. | 
| 5-minute `expiresAt`, `maxAttempts = 3`, statuses `PENDING/COMPLETED/FAILED/EXPIRED` | outbox lifecycle `PENDING/PROCESSING/DELIVERED/FAILED/DEAD_LETTER` | Expiry notifications disappear: a subscription persists until deleted, and success/failure is tracked per delivery row (§9.2). | 
| Testnet mode (`NODE_ENV === "development"`, `isTestMode`) | unchanged boot-time switch | Testnet and mainnet remain separate deployments with different contracts/RPCs and their own MySQL schema/database (`MYSQL_DATABASE`). | 

### 2.2 Legacy Behaviors Deliberately Not Carried Over

* **Exact-amount matching and 5-minute expiry** — replaced by persistent subscriptions (§2.1).

* **Per-transaction verification retries (`maxAttempts = 3`)** — replaced by checkpoint resume plus RPC-level retries in the scanner; HTTP retries live in the outbox (§9.2).

* **In-memory 24 h processed-event TTL** — replaced by the persistent idempotency constraint on `deliveries` (§5.2, §8.2).

* **Runtime network admin endpoints** (`PATCH /admin/networks/:network`, `.../enable`, `.../disable`) — network enablement moves to configuration/env; a persistent `networks` configuration table is out of scope for this PRD. If runtime toggling is still required, it must be specified separately (including what happens to in-flight blocks and the scanner lease).

## 3. System Architecture: Decoupled Block Indexer and Outbox Delivery Pipeline

To maximize block-scanning throughput and guarantee zero missed blocks, the monolithic `TetherWatcher` loop is split into two decoupled subsystems sharing a **MySQL 8.0 / 8.4 (`InnoDB`)** database accessed via a `mysql2/promise` connection pool:

1. **Subsystem A — Chain Block Scanner (`BlockScannerService`):**

   * Runs one independent sequential loop per enabled network (`TRC20` via TronWeb, `BEP20` via Ethers.js). Unlike single-writer embedded databases, MySQL `InnoDB` uses row-level locking, allowing the `TRC20` and `BEP20` scanners to commit blocks concurrently without write-lock serialization.

   * Acquires the per-network scanner lease before doing any work, then reads the last committed block number from `block_checkpoints` (§8.1, §8.3).

   * Fetches full block transaction logs/events for block $N$ (`last_processed_block + 1`) once block $N$ has reached the network's confirmation depth (`latestChainBlock - networkConfirmations[network] >= N`; defaults TRC20 = 20, BEP20 = 12 — §8.1).

   * Extracts all USDT `Transfer(address indexed from, address indexed to, uint256 value)` events from block $N$ and verifies that block $N$'s parent hash equals `block_checkpoints.last_block_hash` (§8.3).

   * Executes one forced-order batch lookup against `webhook_addresses` (`JSON_TABLE` + `STRAIGHT_JOIN`, §6.3) to find all active subscribed webhooks matching either `from` or `to` addresses in block $N$, honoring each webhook's `network_filter`.

   * Evaluates each matched webhook's subscription plan event quota (`delivered_events_count < max_deliverable_events`).

   * Inside a single atomic MySQL transaction (`START TRANSACTION ... COMMIT` or stored procedure `CALL sp_commit_block(...)`): inserts permitted events into `deliveries`, increments `delivered_events_count` for queued events, increments `dropped_events_count` for over-quota matches, and updates `block_checkpoints.last_processed_block = N`.

2. **Subsystem B — Asynchronous Webhook Delivery Worker (`DeliveryWorkerService`):**

   * Polls the `deliveries` table for rows where `is_retryable = 1 AND next_attempt_at <= current_timestamp_ms`, using the generated-column composite index `idx_deliveries_worker_poll`.

   * Atomically claims batches of deliveries (`SELECT ... FOR UPDATE OF d SKIP LOCKED` followed by `UPDATE deliveries SET status = 'PROCESSING'`, §9.2) so multiple worker instances can scale horizontally without lock contention or double-sending; delivery is **at-least-once**, not exactly-once (§9.2).

   * Dispatches HTTP POST requests using Axios (`src/utils/webhook.ts`) with the standardized JSON payload and `X-Webhook-Signature` header (`src/utils/generateSignature.ts`).

   * Marks successful responses (`2xx`) as `DELIVERED` and schedules failed deliveries (`4xx`/`5xx`/timeout) for exponential backoff retry or `DEAD_LETTER` exhaustion.

## 4. MySQL 8 Engine Selection, Driver Architecture, and High-Performance Tuning

### 4.1 Driver Selection: `mysql2/promise` with Binary Protocol Prepared Statements

For a high-throughput blockchain scanner in Node.js targeting MySQL, **`mysql2` (`mysql2/promise@^3.11`)** is required over legacy `mysql` or heavy ORM layers (Prisma/TypeORM):

* **Native Binary Protocol (`COM_STMT_PREPARE` / `COM_STMT_EXECUTE`):** Calling `connection.execute(sql, params)` in `mysql2` compiles parameterized SQL once on the MySQL server and caches the statement handle per connection (`maxPreparedStatements: 256`). Subsequent executions transmit compact binary parameters without SQL lexing or query-tree parsing overhead.

* **Dedicated Connection Pooling with Zero Head-of-Line Blocking:** `mysql.createPool()` provisions dedicated connections for the two network scanners (`TRC20`, `BEP20`), concurrent `DeliveryWorker` claims, and Express API mutations. Because `InnoDB` locks at the row level rather than locking an entire database file, independent work does not serialize on a global writer lock. Contention is per row: a block commit holds an X-lock on its own `block_checkpoints` row and on every `subscription_plans` row it touches until `COMMIT`, so a concurrent `PATCH`/address-add on the *same* plan (or a second scanner of the *same* network) waits up to `innodb_lock_wait_timeout` (5 s, §4.2). Worker status updates touch `deliveries` rows only and therefore never block a checkpoint commit.

* **BigInt Fidelity Within the JavaScript Safe Range:** `supportBigNumbers: true` (with `bigNumberStrings: false`, as configured in §4.2) makes `mysql2` return `BIGINT`/`DECIMAL` values exact: values within ±2^53 − 1 arrive as JS `Number`, larger ones arrive as `String`. Every `BIGINT` in this schema — block heights, millisecond timestamps, quota counters — stays far below 2^53, so `Number` is exact; raw token amounts are already `VARCHAR`, so they never pass through the numeric path at all. If a counter ever exceeds 2^53, the value arrives as a string and must be handled with `BigInt`.

### 4.2 Mandatory MySQL Server (`my.cnf`) and Session Configuration

#### Pool Connection Initialization (`src/db/database.ts`)

When creating the `mysql2/promise` pool, configure the following pool options and run the session initialization hook on every newly opened connection (`pool.on('connection', ...)`):

```
import mysql from 'mysql2/promise';

export const pool = mysql.createPool({
  host: process.env.MYSQL_HOST ?? '127.0.0.1',
  port: Number(process.env.MYSQL_PORT ?? 3306),
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE ?? 'tether_watcher',
  waitForConnections: true,
  connectionLimit: Number(process.env.MYSQL_POOL_SIZE ?? 20),
  maxIdle: 10,
  idleTimeout: 60000,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 10000,
  maxPreparedStatements: 256,
  supportBigNumbers: true,
  bigNumberStrings: false, // §4.1: ≤ 2^53-1 arrives as Number, larger values as string
  timezone: 'Z'
});

```

Every connection established by the pool must execute the following session setup before entering service:

```
-- Enforce strict data validation (reject silent truncation, invalid dates, or out-of-range integers)
SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- Use READ COMMITTED instead of REPEATABLE READ to eliminate gap-lock / next-key-lock deadlocks
-- during concurrent webhook_addresses inserts and FOR UPDATE SKIP LOCKED worker claims
SET SESSION transaction_isolation = 'READ-COMMITTED';

-- Fail fast after 5 seconds if a row lock is held by a stalled transaction
SET SESSION innodb_lock_wait_timeout = 5;

-- Standardize session clock to UTC
SET SESSION time_zone = '+00:00';

```

#### MySQL 8.0 / 8.4 Server Tuning (`my.cnf`)

To achieve sub-3ms batch address lookups and block commits at 1,000,000+ watched addresses, configure the MySQL server with the following `InnoDB` parameters:

```
[mysqld]
# Default character set and collation for general text columns
character_set_server = utf8mb4
collation_server = utf8mb4_0900_ai_ci

# Allocate 60%-75% of dedicated host RAM (min 512M-1G for 1M-10M addresses)
# Keeps the entire webhook_addresses clustered B+Tree resident in memory
innodb_buffer_pool_size = 1G
innodb_buffer_pool_instances = 4

# Durability vs. Throughput trade-off (see §8.2):
# 2 = write redo log to OS cache on commit, flush to disk once per second (equivalent to SQLite synchronous = NORMAL)
# 1 = fsync redo log on every commit (strict power-loss durability, equivalent to SQLite synchronous = FULL)
innodb_flush_log_at_trx_commit = 2
# 0 = "Disables synchronization of the binary log to disk" (MySQL manual). Only valid
# while this server is a standalone single instance that is not a replication source:
# on a source, a crash can leave the binary log behind the redo log, and replicas then
# diverge. Set sync_binlog = 1 before this instance ever feeds a replica.
sync_binlog = 0

# Size redo log capacity (MySQL 8.0.30+) to absorb high-volume block bursts without checkpoint stalls
innodb_redo_log_capacity = 512M

# Bypass OS page-cache double buffering for InnoDB data files on Linux NVMe/SSD
innodb_flush_method = O_DIRECT

# Use READ COMMITTED globally as the default isolation level for high-concurrency OLTP/queue workloads
transaction_isolation = READ-COMMITTED
innodb_lock_wait_timeout = 5

```

**Optimizer Statistics Maintenance:** Run `ANALYZE TABLE webhook_addresses, webhooks, subscription_plans, deliveries;` after bulk address imports and via a daily maintenance job so the InnoDB persistent optimizer statistics (`innodb_stats_persistent = ON`) remain accurate.

## 5. Complete MySQL 8 (`InnoDB`) Database Schema and Indexing Specification

### 5.1 Entity Relationship Overview and MySQL Storage Conventions

The database schema consists of five core `InnoDB` tables designed to support manual plan assignment, strict quota accounting, $O(\log N)$ address matching without full-table memory loads, block checkpointing, and outbox delivery queuing:

1. `subscription_plans`: Reusable plan templates defining quota ceilings (`max_webhooks`, `max_addresses`, `max_deliverable_events`) or direct per-tenant plan records.

2. `webhooks`: Registered webhook endpoints. Each webhook references a `plan_id` representing the assigned subscription plan instance that tracks quota usage across its linked webhooks.

3. `webhook_addresses`: Clustered `InnoDB` junction table mapping `(network, address, webhook_id)`. In MySQL `InnoDB`, every table is automatically a clustered Index-Organized Table (IOT) ordered physically by its `PRIMARY KEY`. Assigning address `123` to `webhook1` and `webhook2` creates two distinct rows, counting as **2** against the plan's `max_addresses` limit.

4. `block_checkpoints`: Stores the last fully processed block height per network (`TRC20`, `BEP20`), the last processed block hash, and the single-scanner lease (`lease_owner`, `lease_expires_at`, §8.3).

5. `deliveries`: Outbox queue storing standardized matched blockchain events awaiting webhook dispatch, augmented with generated virtual columns (`is_retryable`, `processing_updated_at`) to emulate partial indexes in MySQL.

**Critical MySQL Collation Convention (`ascii_bin`):** MySQL's default `utf8mb4_0900_ai_ci` collation uses up to 4 bytes per character and performs case-insensitive comparisons. Case-insensitive indexing is **unsafe** for TRON Base58Check (`T...`) addresses (where upper and lower case characters are distinct alphabet symbols) and wastes 75% of index memory on ASCII identifiers. All IDs, blockchain addresses, and transaction hashes MUST use `CHARACTER SET ascii COLLATE ascii_bin` (1 byte per character, deterministic byte-exact equality).

### 5.2 DDL Schema Definition (`src/db/schema.sql`)

```
-- Require MySQL 8.0.16+ (native CHECK constraints, expression defaults, JSON_TABLE, SKIP LOCKED)
SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- 1. SUBSCRIPTION PLANS (Manual Plan / Quota Assignment)
-- Each row represents a manually provisioned subscription plan instance.
-- Can be assigned to a single webhook or shared across multiple webhooks belonging to one customer.
CREATE TABLE IF NOT EXISTS subscription_plans (
    id                      VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    name                    VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    max_webhooks            INT UNSIGNED NOT NULL,
    max_addresses           INT UNSIGNED NOT NULL,
    max_deliverable_events  BIGINT UNSIGNED NOT NULL,

    -- Materialized quota counters maintained atomically via transactions
    -- UNSIGNED prevents negative underflow at the storage engine level
    used_webhooks_count     INT UNSIGNED NOT NULL DEFAULT 0,
    used_addresses_count    INT UNSIGNED NOT NULL DEFAULT 0,
    delivered_events_count  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    dropped_events_count    BIGINT UNSIGNED NOT NULL DEFAULT 0,

    is_active               TINYINT(1) UNSIGNED NOT NULL DEFAULT 1,
    created_at              BIGINT UNSIGNED NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)),
    updated_at              BIGINT UNSIGNED NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)),

    PRIMARY KEY (id),
    CONSTRAINT chk_plans_is_active CHECK (is_active IN (0, 1))
) ENGINE=InnoDB;

-- 2. WEBHOOKS
-- Persistent storage for webhook endpoints; manually assigned to a subscription_plan.
CREATE TABLE IF NOT EXISTS webhooks (
    id                      VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    plan_id                 VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    url                     VARCHAR(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    secret                  VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    description              VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
    network_filter          ENUM('TRC20', 'BEP20', 'ALL') NOT NULL DEFAULT 'ALL',
    is_active               TINYINT(1) UNSIGNED NOT NULL DEFAULT 1,
    created_at              BIGINT UNSIGNED NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)),
    updated_at              BIGINT UNSIGNED NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)),

    PRIMARY KEY (id),
    KEY idx_webhooks_plan_id (plan_id),
    CONSTRAINT fk_webhooks_plan
        FOREIGN KEY (plan_id) REFERENCES subscription_plans (id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT chk_webhooks_is_active CHECK (is_active IN (0, 1))
) ENGINE=InnoDB;

-- 3. WEBHOOK ADDRESSES (Hot-Path Clustered B+Tree Table)
-- In MySQL InnoDB, tables are automatically clustered (Index-Organized) on PRIMARY KEY.
-- Ordering PRIMARY KEY as (network, address, webhook_id) physically sorts all rows on disk
-- and in the InnoDB Buffer Pool by (network, address), storing plan_id in the leaf page
-- with ZERO secondary heap lookups.
-- Using ENUM('TRC20', 'BEP20') encodes network in 1 byte; ascii_bin encodes address in 1 byte/char.
CREATE TABLE IF NOT EXISTS webhook_addresses (
    network                 ENUM('TRC20', 'BEP20') NOT NULL,
    address                 VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    webhook_id              VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    plan_id                 VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at              BIGINT UNSIGNED NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)),

    PRIMARY KEY (network, address, webhook_id),
    -- InnoDB automatically appends PRIMARY KEY columns to secondary index leaves,
    -- making both indexes below 100% covering for address listing and counting.
    KEY idx_webhook_addresses_webhook_id (webhook_id),
    KEY idx_webhook_addresses_plan_id (plan_id),
    CONSTRAINT fk_wa_webhook
        FOREIGN KEY (webhook_id) REFERENCES webhooks (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT fk_wa_plan
        FOREIGN KEY (plan_id) REFERENCES subscription_plans (id)
        ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- 4. BLOCK CHECKPOINTS (Crash Recovery State & Scanner Lease)
-- Updated atomically at the end of every full block scan.
CREATE TABLE IF NOT EXISTS block_checkpoints (
    network                 ENUM('TRC20', 'BEP20') NOT NULL,
    last_processed_block    BIGINT UNSIGNED NOT NULL,
    last_block_hash         VARCHAR(66) CHARACTER SET ascii COLLATE ascii_bin NULL,
    last_block_timestamp    BIGINT UNSIGNED NULL,
    -- Single-scanner lease (§8.3): prevents concurrent/overlapping scanners, which would double-count dropped events
    lease_owner             VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL,
    lease_expires_at        BIGINT UNSIGNED NULL,
    updated_at              BIGINT UNSIGNED NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)),

    PRIMARY KEY (network)
) ENGINE=InnoDB;

-- 5. DELIVERIES (Transactional Outbox Queue)
-- Stores matched events in a standard format for asynchronous webhook dispatch.
-- Because MySQL does not support SQLite-style partial indexes (CREATE INDEX ... WHERE ...),
-- we define lightweight VIRTUAL generated columns that evaluate to non-NULL only for active rows
-- or partition/index by status prefix so worker polling remains O(due_rows).
CREATE TABLE IF NOT EXISTS deliveries (
    id                      VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    plan_id                 VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    webhook_id              VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    network                 ENUM('TRC20', 'BEP20') NOT NULL,
    block_number            BIGINT UNSIGNED NOT NULL,
    tx_hash                 VARCHAR(66) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    log_index               INT UNSIGNED NOT NULL,
    from_address            VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    to_address              VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    matched_address         VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    direction               ENUM('INCOMING', 'OUTGOING') NOT NULL,
    amount_raw              VARCHAR(78) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    amount_decimal          VARCHAR(80) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    token_contract          VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    block_timestamp         BIGINT UNSIGNED NOT NULL,
    payload_json            TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,

    -- Delivery Worker State Machine
    status                  ENUM('PENDING', 'PROCESSING', 'DELIVERED', 'FAILED', 'DEAD_LETTER')
                            NOT NULL DEFAULT 'PENDING',
    attempt_count           INT UNSIGNED NOT NULL DEFAULT 0,
    max_attempts            INT UNSIGNED NOT NULL DEFAULT 5,
    next_attempt_at         BIGINT UNSIGNED NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)),
    last_http_status        SMALLINT UNSIGNED NULL,
    last_error              VARCHAR(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
    delivered_at            BIGINT UNSIGNED NULL,
    created_at              BIGINT UNSIGNED NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)),
    updated_at              BIGINT UNSIGNED NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)),

    -- Virtual generated column: 1 when status IN ('PENDING', 'FAILED'), NULL otherwise.
    -- Allows a single contiguous B+Tree range scan on (is_retryable, next_attempt_at, id)
    -- without merging separate 'PENDING' and 'FAILED' index ranges or scanning 'DELIVERED' rows.
    is_retryable            TINYINT(1) UNSIGNED GENERATED ALWAYS AS (
                                IF(status IN ('PENDING', 'FAILED'), 1, NULL)
                            ) VIRTUAL,

    PRIMARY KEY (id),

    -- Idempotency constraint: prevents duplicate webhook deliveries if a block is ever re-scanned
    UNIQUE KEY uq_deliveries_idempotency (webhook_id, network, tx_hash, log_index, matched_address),

    -- Worker polling index: queries WHERE is_retryable = 1 AND next_attempt_at <= :now
    -- Only probes the is_retryable = 1 subtree; millions of DELIVERED/DEAD_LETTER rows sit in the NULL prefix.
    KEY idx_deliveries_worker_poll (is_retryable, next_attempt_at, id),

    -- Stale-PROCESSING recovery index (§9.2 step 4) AND retention pruning index (§9.3):
    -- Leading status column isolates 'PROCESSING' (recovery) and 'DELIVERED'/'DEAD_LETTER' (pruning)
    -- into dedicated B+Tree subtrees ordered by updated_at.
    KEY idx_deliveries_status_updated (status, updated_at),

    KEY idx_deliveries_webhook_status (webhook_id, status),
    KEY idx_deliveries_block_number (network, block_number),
    KEY idx_deliveries_plan_id (plan_id),

    CONSTRAINT fk_deliveries_webhook
        FOREIGN KEY (webhook_id) REFERENCES webhooks (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT fk_deliveries_plan
        FOREIGN KEY (plan_id) REFERENCES subscription_plans (id)
        ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

```

## 6. High-Performance Address Lookup Architecture (Avoiding In-Memory Maps)

### 6.1 Why InnoDB Clustered Primary Keys + `ascii_bin` Maximize MySQL Speed

In MySQL `InnoDB`, every table is organized physically as a B+Tree clustered on its `PRIMARY KEY` (equivalent to SQLite's `WITHOUT ROWID` or Oracle's Index-Organized Table). If `webhook_addresses` had used a surrogate auto-increment `id` primary key and a secondary index on `(network, address)`, every match would require two B+Tree traversals: first searching the secondary index to find the primary key, and second traversing the clustered primary key B+Tree to fetch `webhook_id` and `plan_id`.

By defining `webhook_addresses` with no surrogate key and `PRIMARY KEY (network, address, webhook_id)`:

* **Single B+Tree Traversal (Zero Bookmark Lookups):** The primary key **is** the table storage. All addresses for `TRC20` (`ENUM` index `1`) and `BEP20` (`ENUM` index `2`) are physically sorted on disk and in the InnoDB Buffer Pool in byte-lexicographical order alongside their `webhook_id` and `plan_id`.

* **100% Covering Clustered Read:** Once InnoDB locates `(network, address)` in the clustered B+Tree leaf page (`16 KB` page size), `webhook_id` and `plan_id` are already in that exact CPU cache line.

* **Compact Key Width via `ENUM` + `ascii_bin`:** Encoding `network` as `ENUM('TRC20', 'BEP20')` (1 byte) and `address` / `webhook_id` / `plan_id` with `CHARACTER SET ascii COLLATE ascii_bin` (1 byte per char instead of 4 bytes in `utf8mb4`) shrinks the clustered row footprint by \~65%, fitting \~150–200 address subscriptions per 16 KB InnoDB page and keeping Node.js heap usage flat (\~50 MB) regardless of whether MySQL stores 100,000 or 10,000,000 watched addresses.

### 6.2 Address Normalization Rules

Before inserting into `webhook_addresses` or querying during block scanning, addresses must be deterministically normalized in `src/utils/addressNormalizer.ts`:

* **BEP20 (EVM / BSC):** Convert to lowercase 42-character hexadecimal string (`address.trim().toLowerCase()`). Do not store mixed-case EIP-55 checksum casing in the database index, as `COLLATE ascii_bin` performs raw byte comparisons (`0x41` $\neq$ `0x61`), which is both faster than case-folding collations and mandatory for TRON coexistence.

* **TRC20 (TRON):** Normalize all addresses to standard 34-character Base58Check (`T...`) format using `tronWeb.address.fromHex(rawHexAddress)` when extracting topics from TRON smart contract logs (which emit 20-byte/21-byte `41...` hex addresses in raw `Transfer` event topics). Because `ascii_bin` is case-sensitive, Base58Check characters are matched with 100% fidelity.

### 6.3 Query Strategy: Block-Level Batch Lookup with `JSON_TABLE` and `STRAIGHT_JOIN`

During block scanning, a single BSC or TRON block typically contains between 50 and 600 USDT `Transfer` events involving 80 to 1,000 unique sender/recipient addresses. Executing 1,000 individual MySQL queries over TCP/Unix sockets per block would incur 150–500 ms of network roundtrip overhead.

Instead, the scanner collects all unique normalized `from` and `to` addresses from all USDT `Transfer` events in block $N$ into a deduplicated JavaScript `Set<string>`, serializes that per-block set (e.g., 300 addresses, \~12 KB) as a JSON array, and executes **one single binary-protocol prepared statement per block** using MySQL 8.0's built-in C++ `JSON_TABLE()` table function with a forced join order:

```
-- Prepared once per connection via pool.execute(MATCH_BLOCK_ADDRESSES_SQL, [blockAddressesJson, network, network]).
-- Both the /*+ JOIN_ORDER(batch, wa, w, sp) */ optimizer hint and STRAIGHT_JOIN are MANDATORY,
-- and the JSON_TABLE column MUST declare CHARACTER SET ascii COLLATE ascii_bin.
SELECT /*+ JOIN_ORDER(batch, wa, w, sp) */
    wa.address            AS matched_address,
    wa.webhook_id         AS webhook_id,
    wa.plan_id            AS plan_id,
    w.url                 AS webhook_url,
    w.secret              AS webhook_secret
FROM JSON_TABLE(
    ?,
    '$[*]' COLUMNS (
        address VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin PATH '$'
    )
) AS batch
STRAIGHT_JOIN webhook_addresses AS wa
    ON wa.network = ?
   AND wa.address = batch.address
INNER JOIN webhooks AS w
    ON w.id = wa.webhook_id
   AND w.is_active = 1
   AND (w.network_filter = 'ALL' OR w.network_filter = ?)
INNER JOIN subscription_plans AS sp
    ON sp.id = wa.plan_id
   AND sp.is_active = 1;

```

**Why this query is optimal in MySQL 8 — and why both `STRAIGHT_JOIN` and `COLLATE ascii_bin` inside `JSON_TABLE` are mandatory:**

1. **Constant Prepared-Statement Cache Footprint (`1` Statement Handle):** A dynamic `WHERE wa.address IN (?, ?, ...)` clause changes its parameter count on every block (e.g., 142 addresses in block $N$, 389 in block $N+1$), thrashing `mysql2`'s `maxPreparedStatements` cache and forcing MySQL to re-parse and re-optimize SQL on every block. Passing a single JSON array string `?` to `JSON_TABLE` reuses the exact same compiled server-side prepared statement across millions of blocks.

2. **Explicit `CHARACTER SET ascii COLLATE ascii_bin` inside `JSON_TABLE` prevents index bypass:** A `JSON_TABLE` string column does not carry the `ascii_bin` collation of the indexed column. Comparing a `utf8mb4`-collated derived column to an `ascii_bin` primary key forces MySQL to apply a character-set conversion (`convert(wa.address using utf8mb4)`), which defeats the `PRIMARY KEY` lookup and leaves a full clustered-index scan on a 1M-row table. Declaring `VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin` inside `COLUMNS (...)` keeps the comparison byte-for-byte so `wa` is probed with a direct `ref` on `PRIMARY KEY (network, address)`. Assert the outcome (not the syntax) with §12.2 test 5; if the target server rejects the character-set clause inside `COLUMNS (...)`, the equivalent form is `wa.address = CONVERT(batch.address USING ascii)` — the indexed column stays untouched, so the `PRIMARY` probe is preserved.

3. **Forced Join Order (`STRAIGHT_JOIN` + `JOIN_ORDER` hint):** MySQL's cost-based optimizer assigns a fixed heuristic cardinality to `JSON_TABLE` derived tables. Without `STRAIGHT_JOIN`, if `subscription_plans` or `webhooks` contains only a few rows while `webhook_addresses` contains 1,000,000 rows, the optimizer may choose to scan `subscription_plans` $\rightarrow$ `webhooks` $\rightarrow$ `idx_webhook_addresses_webhook_id` and materialize `JSON_TABLE` on the inside of the loop:

   | Formulation in MySQL 8 | Execution Plan (`EXPLAIN FORMAT=TREE`) | Behavior at 1M Addresses | 
   | ----- | ----- | ----- | 
   | Naive `INNER JOIN` without `ascii_bin` on `JSON_TABLE` | Full scan or secondary index scan with per-row charset conversion (`convert(wa.address using utf8mb4)`) | **Multi-second full scan** | 
   | `INNER JOIN` with `ascii_bin` but no `STRAIGHT_JOIN` | Planner may drive from `subscription_plans` $\rightarrow$ `webhooks` when plan count is small | **Unpredictable (plan flip risk)** | 
   | `JSON_TABLE(... ascii_bin) STRAIGHT_JOIN wa ...` (**required**) | `Table scan on batch (JSON_TABLE) -> Index lookup on wa using PRIMARY (network=?, address=batch.address) -> Single-row index lookup on w using PRIMARY -> Single-row index lookup on sp using PRIMARY` | **target ≈1.5–2.8 ms per block — not yet measured** (no MySQL benchmark has been run; §12.2 test 5 pins the *plan*, a latency run on the target host is still required) | 

*(Fallback for Per-Transaction Streaming)*: If an API provider streams transactions individually rather than in block batches, use a pre-compiled 4-parameter prepared statement checking both `from` and `to` in a single call:

```
SELECT
    wa.address AS matched_address,
    wa.webhook_id,
    wa.plan_id,
    w.url AS webhook_url,
    w.secret AS webhook_secret
FROM webhook_addresses AS wa
INNER JOIN webhooks AS w ON w.id = wa.webhook_id AND w.is_active = 1
INNER JOIN subscription_plans AS sp ON sp.id = wa.plan_id AND sp.is_active = 1
WHERE wa.network = ?
  AND wa.address IN (?, ?)
  AND (w.network_filter = 'ALL' OR w.network_filter = ?);

```

This statement plans as an `InnoDB` `range` access on `PRIMARY KEY (network, address)` probing the two addresses directly with zero table scans.

## 7. Subscription Plan & Quota Enforcement Specification

### 7.1 Quota Rules and Edge Cases

Each subscription plan row in `subscription_plans` defines three hard limits (`max_webhooks`, `max_addresses`, `max_deliverable_events`):

| Quota Dimension | Enforcement Point | Counting Rule & Multi-Webhook Behavior | Action When Limit Reached | 
| ----- | ----- | ----- | ----- | 
| **Maximum Webhooks** (`max_webhooks`) | Synchronous check during `POST /api/watcher/webhooks` inside an InnoDB transaction. | Each row in `webhooks` referencing `plan_id` counts as `1` (`used_webhooks_count`). Deleting a webhook decrements `used_webhooks_count` by `1`. | Reject API request with `HTTP 403 Forbidden` (`error: "WEBHOOK_QUOTA_EXCEEDED"`). | 
| **Maximum Addresses** (`max_addresses`) | Synchronous check during `POST /api/watcher/webhooks/:id/addresses` inside an InnoDB transaction. | Each `(webhook_id, network, address)` row in `webhook_addresses` counts as `1` against `used_addresses_count`. **If address `123` is assigned to `webhook1` and `webhook2` under the same plan, it creates 2 rows and counts as 2 against `max_addresses`.** Assigning the exact same address twice to the *same* webhook on the same network is idempotent (the no-op `ON DUPLICATE KEY UPDATE` form, count unchanged). | Reject API request (or partial batch beyond remaining capacity) with `HTTP 403 Forbidden` (`error: "ADDRESS_QUOTA_EXCEEDED"`). | 
| **Maximum Deliverable Events** (`max_deliverable_events`) | Synchronous check during block scanning inside the per-block atomic transaction before inserting into `deliveries`. | Every matched `(transfer_event, webhook_address)` pair counts as `1` event. If a single transaction transfers USDT from subscribed `addrA` (on `webhook1`) to subscribed `addrB` (on `webhook2`), that represents **2** matching events. The quota is consumed at enqueue time, so an event that is queued and later fails permanently still counts. This counter requires the single-scanner lease of §8.3 so dropped events are never double-counted by overlapping scanners. | Do **not** insert into `deliveries`. Atomically increment `subscription_plans.dropped_events_count` by `1` and log a throttled warning. | 
| **Inactive plan or webhook** (`is_active = 0`) | Filtered out of the §6.3 match query (`w.is_active = 1`, `sp.is_active = 1`). | An inactive plan or webhook produces **no** candidates at all: nothing is queued and nothing is counted as dropped — deactivation is a silent pause, not a quota event. Deliveries already queued are still dispatched (the worker does not re-check `is_active`); in-flight `PROCESSING` rows are recovered per §9.2. | Documented behavior; no counter changes. Re-activation resumes matching at the next scanned block — there is **no backfill** of blocks scanned while inactive. | 

### 7.2 Atomic Quota Accounting in MySQL (`InnoDB`)

To avoid expensive `SELECT COUNT(*)` queries during high-speed block scanning or API mutations, `subscription_plans` maintains materialized counters (`used_webhooks_count`, `used_addresses_count`, `delivered_events_count`, `dropped_events_count`).

#### Enforcing Webhook & Address Quotas via Single-Roundtrip Multi-Row `INSERT … ON DUPLICATE KEY UPDATE` + Conditional `UPDATE`

When adding $K$ new addresses (up to 1,000 per request) to a webhook (`plan_id = :planId`), do **not** loop 1,000 individual `INSERT` statements over the network. Instead, execute a single multi-row `INSERT … ON DUPLICATE KEY UPDATE id = id` inside an InnoDB transaction and inspect `result.affectedRows`, which the manual defines per row as "1 if the row is inserted as a new row, 2 if an existing row is updated, and 0 if an existing row is set to its current values" — so a no-op assignment counts exactly the newly inserted rows:

**Do not use `INSERT IGNORE` for this.** `IGNORE` downgrades *every* ignorable error to a warning — "invalid values are adjusted to the closest values and inserted; warnings are produced but the statement does not abort" — so an over-length address would be silently truncated *and* counted in `affectedRows` as if it were a valid new subscription. The `ON DUPLICATE KEY UPDATE` no-op only absorbs duplicate-key conflicts and still aborts on truncation or `NOT NULL` violations.

```
-- Step 1: Multi-row bulk insert. The no-op assignment ignores duplicate-key conflicts only;
-- unlike INSERT IGNORE it still raises truncation / NOT NULL errors.
INSERT INTO webhook_addresses (network, address, webhook_id, plan_id)
VALUES (?, ?, ?, ?), (?, ?, ?, ?), ...
ON DUPLICATE KEY UPDATE network = network;

-- Step 2: Let :newlyInsertedCount = insertResult.affectedRows.
-- If :newlyInsertedCount > 0, execute atomic conditional quota increment on subscription_plans.
-- In InnoDB, this UPDATE acquires a row-level X-lock on the single subscription_plans row,
-- serializing concurrent address additions for the same plan safely without locking other plans.
UPDATE subscription_plans
SET used_addresses_count = used_addresses_count + ?,
    updated_at = CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)
WHERE id = ?
  AND is_active = 1
  AND (used_addresses_count + ?) <= max_addresses;

```

If `:newlyInsertedCount > 0` and the `UPDATE` returns `affectedRows === 0`, the code throws `AddressQuotaExceededError` and calls `await conn.rollback()`, undoing the multi-row insert cleanly.

Webhook creation uses the same conditional-`UPDATE` pattern:

```
UPDATE subscription_plans
SET used_webhooks_count = used_webhooks_count + 1,
    updated_at = CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)
WHERE id = ?
  AND is_active = 1
  AND (used_webhooks_count + 1) <= max_webhooks;

```

If `affectedRows === 0`, throw `WebhookQuotaExceededError` ($\rightarrow$ `403 WEBHOOK_QUOTA_EXCEEDED`) and roll back. On webhook deletion, lock the plan row (`SELECT id FROM subscription_plans WHERE id = ? FOR UPDATE`), count the webhook's address rows (`SELECT COUNT(*) FROM webhook_addresses WHERE webhook_id = ?`), `DELETE` the webhook (cascading to `webhook_addresses`), and decrement `used_webhooks_count` and `used_addresses_count` inside the same transaction using `used_addresses_count - LEAST(used_addresses_count, ?)` — never `GREATEST(0, used_addresses_count - ?)`, because the subtraction raises `ER_DATA_OUT_OF_RANGE` before `GREATEST` can clamp it (§7.2 counters bullet).

#### Enforcing Deliverable Event Quota During Block Scanning

During block scanning, multiple events in block $N$ may match webhooks tied to `plan_id`. Inside the per-block commit (§8.2), for each non-duplicate candidate delivery:

```
-- Prepared statement: tryConsumeEventQuotaSql
UPDATE subscription_plans
SET delivered_events_count = delivered_events_count + 1,
    updated_at = CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)
WHERE id = ?
  AND is_active = 1
  AND delivered_events_count < max_deliverable_events;

```

* **If `affectedRows === 1`:** Quota is available and reserved. Queue the webhook notification in `deliveries`.

* **If `affectedRows === 0`:** The plan has reached `max_deliverable_events`. Increment `dropped_events_count`:

```
-- Prepared statement: incrementDroppedEventsSql
UPDATE subscription_plans
SET dropped_events_count = dropped_events_count + 1,
    updated_at = CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)
WHERE id = ?;

```

#### Counter Integrity (Derived State)

The four counters are **derived state** maintained atomically for speed; they are not the source of truth:

* **Reconciliation:** `POST /api/watcher/plans/:planId/reconcile` locks the plan row (`SELECT ... FOR UPDATE`), recomputes `used_webhooks_count` from `COUNT(*)` over `webhooks` and `used_addresses_count` from `COUNT(*)` over `webhook_addresses` inside one transaction, writes the corrected values, and returns both the stored and recomputed values. Run it after manual SQL, partial restores, or a suspected drift. Event counters (`delivered_events_count`, `dropped_events_count`) are monotonic by design and are only reset via `resetEventQuota` (§10.1).

* **Limits below usage:** `PATCH /api/watcher/plans/:planId` may set any `max_*` below the current `used_*` (to freeze an over-provisioned plan). Because `used_addresses_count` and `max_addresses` are `INT UNSIGNED`, all quota checks MUST use the addition form `(used_addresses_count + :newCount) <= max_addresses` rather than subtraction (`max_addresses - used_addresses_count`): MySQL raises `ER_DATA_OUT_OF_RANGE` (`BIGINT UNSIGNED value is out of range`) whenever an unsigned subtraction would go negative. The manual states this for the `-` operator itself ("Subtraction between integer values, where one is of type UNSIGNED, produces an unsigned result by default. If the result would otherwise have been negative, an error results"), so it happens **regardless of strict mode** and **before** any `GREATEST(0, …)` wrapper can clamp the result. Decrements must therefore use `used_x - LEAST(used_x, :n)` or `GREATEST(0, CAST(used_x AS SIGNED) - :n)`.

* **Re-planning:** Moving a webhook to another plan must lock both plan rows in deterministic alphabetical `id` order (`ORDER BY id ASC FOR UPDATE` to prevent AB-BA deadlocks), re-check the target plan's `max_webhooks` / `max_addresses` quotas, decrement the source plan's counters, increment the target plan's counters, and update `webhooks.plan_id` *plus* the denormalized `webhook_addresses.plan_id` rows in the same transaction (§10.2).

## 8. Block Checkpointing and Crash Recovery Specification

### 8.1 Startup & Crash Recovery Lifecycle

The scanner acquires its network lease first and refuses to run if another instance holds it (§8.3), then performs the recovery procedure below.

**Confirmation depth is per network, not per webhook.** `networkConfirmations` defaults to `TRC20 = 20` and `BEP20 = 12` — the same values as today's `networkConfigs` — overridable via `TRC20_CONFIRMATIONS` / `BEP20_CONFIRMATIONS`. Rationale: one sequential cursor per network cannot honor differing per-webhook depths at insert time without deferred dispatch (a `deliverable_at_block` column plus a worker gate), which is out of scope. Every matched event is emitted at the network's depth, and reorgs deeper than that depth are out of scope (§8.3).

When `BlockScannerService` starts for a network (`TRC20` or `BEP20`):

1. **Query Checkpoint:** Execute `SELECT last_processed_block, last_block_hash FROM block_checkpoints WHERE network = ?`.

2. **Case A — Checkpoint Exists (`last_processed_block = B`):**

   * Log recovery event: `Resuming [network] scanner from checkpoint block ${B + 1}`.

   * Query the RPC provider for the current chain head (`latestBlock`) and compute `safeTargetBlock = latestBlock - networkConfirmations[network]`.

   * Iterate sequentially for `blockNum = B + 1` up to `safeTargetBlock`.

3. **Case B — No Checkpoint Exists (First Boot on Fresh Database):**

   * If environment variable `START_BLOCK_${NETWORK}` is configured, initialize `block_checkpoints` with `START_BLOCK_${NETWORK} - 1`.

   * Otherwise, fetch `latestBlock` from the RPC provider, compute `initialSafeBlock = latestBlock - networkConfirmations[network]`, insert `last_processed_block = initialSafeBlock` into `block_checkpoints`, and begin scanning from `initialSafeBlock + 1`.

4. **Catch-up semantics:** The loop always walks `B + 1 → safeTargetBlock` one block at a time, so downtime is fully backfilled; the cursor never jumps to the chain head.

### 8.2 Atomic Per-Block Commit Protocol (Single-Roundtrip Stored Procedure & Transaction Spec)

**Critical MySQL vs. SQLite Optimization (Eliminating Per-Match Network Roundtrips):** In an embedded database like SQLite, looping over 50 candidate matches and running 3 statements per match inside a transaction takes <1 ms because every call is an in-process memory read. In client-server MySQL, executing `150` sequential `await conn.execute(...)` calls inside an async loop over TCP/Unix sockets incurs **150 network roundtrips** (\~15–45 ms), holding row locks on `subscription_plans` the entire time.

To achieve `< 3.0 ms` per-block commit latency in MySQL 8, the per-block commit is encapsulated in a **stored procedure (`sp_commit_block`)** (or an equivalent single-roundtrip batched transaction) that accepts the block metadata and the JSON array of `candidateMatches` and executes the entire check-consume-insert-checkpoint sequence **inside the MySQL server kernel in a single network roundtrip**:

```
DELIMITER $$

CREATE PROCEDURE sp_commit_block(
    IN p_network ENUM('TRC20', 'BEP20'),
    IN p_block_number BIGINT UNSIGNED,
    IN p_block_hash VARCHAR(66) CHARACTER SET ascii COLLATE ascii_bin,
    IN p_block_timestamp BIGINT UNSIGNED,
    IN p_lease_owner VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin,
    IN p_candidates_json JSON
)
BEGIN
    DECLARE v_now BIGINT UNSIGNED DEFAULT CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED);
    DECLARE v_lease_valid INT DEFAULT 0;
    DECLARE v_done INT DEFAULT FALSE;

    DECLARE v_delivery_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE v_plan_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE v_webhook_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE v_tx_hash VARCHAR(66) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE v_log_index INT UNSIGNED;
    DECLARE v_from_address VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE v_to_address VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE v_matched_address VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE v_direction VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE v_amount_raw VARCHAR(78) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE v_amount_decimal VARCHAR(80) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE v_token_contract VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE v_payload_json TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

    -- Cursor over JSON_TABLE preserves exact candidate order inside InnoDB
    DECLARE cur_candidates CURSOR FOR
        SELECT
            c.delivery_id, c.plan_id, c.webhook_id, c.tx_hash, c.log_index,
            c.from_address, c.to_address, c.matched_address, c.direction,
            c.amount_raw, c.amount_decimal, c.token_contract, c.payload_json
        FROM JSON_TABLE(
            p_candidates_json,
            '$[*]' COLUMNS (
                delivery_id     VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PATH '$.deliveryId',
                plan_id         VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PATH '$.planId',
                webhook_id      VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PATH '$.webhookId',
                tx_hash         VARCHAR(66) CHARACTER SET ascii COLLATE ascii_bin PATH '$.txHash',
                log_index       INT UNSIGNED PATH '$.logIndex',
                from_address    VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin PATH '$.fromAddress',
                to_address      VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin PATH '$.toAddress',
                matched_address VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin PATH '$.matchedAddress',
                direction       VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin PATH '$.direction',
                amount_raw      VARCHAR(78) CHARACTER SET ascii COLLATE ascii_bin PATH '$.amountRaw',
                amount_decimal  VARCHAR(80) CHARACTER SET ascii COLLATE ascii_bin PATH '$.amountDecimal',
                token_contract  VARCHAR(42) CHARACTER SET ascii COLLATE ascii_bin PATH '$.tokenContract',
                payload_json    TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci PATH '$.payloadJson'
            )
        ) AS c;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    -- 0. Verify this scanner instance still holds the active lease before mutating state
    SELECT COUNT(*) INTO v_lease_valid
    FROM block_checkpoints
    WHERE network = p_network
      AND lease_owner = p_lease_owner
      AND lease_expires_at >= v_now
    FOR UPDATE;

    IF v_lease_valid = 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'SCANNER_LEASE_LOST: Lease expired or stolen before block commit';
    END IF;

    OPEN cur_candidates;

    read_loop: LOOP
        FETCH cur_candidates INTO
            v_delivery_id, v_plan_id, v_webhook_id, v_tx_hash, v_log_index,
            v_from_address, v_to_address, v_matched_address, v_direction,
            v_amount_raw, v_amount_decimal, v_token_contract, v_payload_json;

        IF v_done THEN
            LEAVE read_loop;
        END IF;

        -- 1. Check if this exact delivery already exists (idempotency guard using uq_deliveries_idempotency)
        IF NOT EXISTS (
            SELECT 1 FROM deliveries
            WHERE webhook_id = v_webhook_id
              AND network = p_network
              AND tx_hash = v_tx_hash
              AND log_index = v_log_index
              AND matched_address = v_matched_address
        ) THEN
            -- 2. Attempt to consume 1 deliverable event from the plan's quota
            UPDATE subscription_plans
            SET delivered_events_count = delivered_events_count + 1,
                updated_at = v_now
            WHERE id = v_plan_id
              AND is_active = 1
              AND delivered_events_count < max_deliverable_events;

            IF ROW_COUNT() = 1 THEN
                -- Quota available -> Insert into deliveries table
                INSERT INTO deliveries (
                    id, plan_id, webhook_id, network, block_number, tx_hash, log_index,
                    from_address, to_address, matched_address, direction,
                    amount_raw, amount_decimal, token_contract, block_timestamp,
                    payload_json, status, next_attempt_at, created_at, updated_at
                ) VALUES (
                    v_delivery_id, v_plan_id, v_webhook_id, p_network, p_block_number, v_tx_hash, v_log_index,
                    v_from_address, v_to_address, v_matched_address, v_direction,
                    v_amount_raw, v_amount_decimal, v_token_contract, p_block_timestamp,
                    v_payload_json, 'PENDING', v_now, v_now, v_now
                );
            ELSE
                -- Quota exhausted -> Ignore event and increment dropped counter
                UPDATE subscription_plans
                SET dropped_events_count = dropped_events_count + 1,
                    updated_at = v_now
                WHERE id = v_plan_id;
            END IF;
        END IF;
    END LOOP;

    CLOSE cur_candidates;

    -- 3. Advance the block checkpoint last inside the same atomic InnoDB transaction
    UPDATE block_checkpoints
    SET last_processed_block = p_block_number,
        last_block_hash = p_block_hash,
        last_block_timestamp = p_block_timestamp,
        updated_at = v_now
    WHERE network = p_network
      AND lease_owner = p_lease_owner;

    COMMIT;
END$$

DELIMITER ;

```

**Migration-runner note (`DELIMITER` is a client directive):** `DELIMITER` is not server SQL — the manual describes it as the way to make "the `;` delimiter used in the procedure body ... passed through to the server rather than being interpreted by **mysql** itself". A Node migration runner must therefore strip the `DELIMITER $$` / `DELIMITER ;` lines and send the whole `CREATE PROCEDURE ... BEGIN ... END` text as **one statement** via `connection.query()` (text protocol): `CREATE PROCEDURE` is **not** in the manual's "SQL Syntax Permitted in Prepared Statements" list, so `execute()` cannot create it. `CALL` **is** in that list, so the invocation below is valid on `pool.execute()`, and placeholders for `IN` parameters are supported in every 8.x version (only `OUT`/`INOUT` placeholders require 8.4).

In Node.js (`BlockScannerService.ts`), committing block $N$ requires a single `mysql2` call:

```
await pool.execute(
  'CALL sp_commit_block(?, ?, ?, ?, ?, ?)',
  [
    network,
    blockNumber,
    blockHash,
    blockTimestamp,
    instanceId,
    JSON.stringify(
      candidateMatches.map((m) => ({
        ...m,
        payloadJson: JSON.stringify(m.payload)
      }))
    )
  ]
);

```

**Crash Consistency Proof (process and database crashes):**

* If the Node.js process crashes (`SIGKILL`, OOM) or the TCP connection drops **before** `COMMIT` completes inside `sp_commit_block`, MySQL automatically rolls back the active transaction when the socket closes or inside the `EXIT HANDLER`. `block_checkpoints.last_processed_block` remains at $N - 1$, `delivered_events_count` and `dropped_events_count` remain unchanged, and zero partial rows exist in `deliveries`.

* If the Node.js process crashes **after** `COMMIT` completes, `last_processed_block` is $N$, all deliveries for block $N$ are persisted in `deliveries` with `status = 'PENDING'`, and the `DeliveryWorker` picks them up while the restarted scanner begins at block $N + 1$.

* **Durability boundary:** With `innodb_flush_log_at_trx_commit = 2`, a MySQL *process* crash or Node.js crash loses zero committed transactions (because the redo log is written to the OS kernel page cache on every commit), while a full *operating-system* crash or power outage can lose up to \~1 second of redo log flushing. Because checkpoint, quota counters, and delivery inserts are in the same redo log group, a power-loss rollback rewinds all three atomically so the block is simply re-scanned upon reboot (at-least-once, §9.2). Set `innodb_flush_log_at_trx_commit = 1` if strict power-loss durability on every block is required.

### 8.3 Scanner Lease, Exactly-Once Scan Invariant, and Reorg Guard

**Lease (single scanner per network).** Quota accounting assumes each block is scanned exactly once, because `deliveries` insertion is deduplicated by `uq_deliveries_idempotency` but **dropped-event counting is not** (§7.1). The lease lives in `block_checkpoints` (and verified atomically inside `sp_commit_block` via `FOR UPDATE`):

```
-- Initial seed on boot (idempotent: the no-op assignment absorbs only the duplicate-key case)
INSERT INTO block_checkpoints (network, last_processed_block)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE network = network;

-- Acquire lease (succeeds only when no live owner holds it)
UPDATE block_checkpoints
SET lease_owner = ?,
    lease_expires_at = CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED) + 30000,
    updated_at = CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)
WHERE network = ?
  AND (
    lease_owner IS NULL
    OR lease_owner = ?
    OR lease_expires_at < CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)
  );
-- affectedRows === 0 -> another scanner is live -> log CRITICAL and exit non-zero

```

Renew every 10 s:

```
UPDATE block_checkpoints
SET lease_expires_at = CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED) + 30000,
    updated_at = CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)
WHERE network = ?
  AND lease_owner = ?;

```

If `affectedRows === 0`, the lease was taken over and the scanner must stop immediately. Furthermore, because `sp_commit_block` verifies `lease_owner = p_lease_owner AND lease_expires_at >= v_now FOR UPDATE` before committing any block, a paused/GC-stalled scanner whose lease expired mid-block is **fenced off at the database level** and cannot commit stale block state. Release on graceful shutdown (`SET lease_owner = NULL, lease_expires_at = NULL`).

**Parent-continuity check (reorg guard).** Before committing block $N$, compare `getBlock(N).parentHash` (returned alongside the block's own hash and timestamp, §11) with `block_checkpoints.last_block_hash`. On mismatch, the chain reorganized deeper than the previous block: stop this network's scanner, log CRITICAL with both hashes, and leave the checkpoint untouched. Never continue on a fork: a partially correct rewind would corrupt `dropped_events_count`, and deliveries already sent cannot be recalled. Operator options: let the provider settle (TRON solid blocks; BSC $\ge$ 12 blocks) and restart, or manually rewind `last_processed_block` / `last_block_hash` to a known common ancestor and reconcile counters before resuming.

**Invariants and explicit non-goals:**

* Reorgs deeper than `networkConfirmations` are out of scope; confirmation depth plus the parent check is the whole reorg defense.

* Adding an address or re-activating a webhook never backfills historical blocks — matching starts from the next scanned block (§7.1).

* Never replay an already-committed block range (e.g. by manually rewinding the checkpoint) without reconciling `dropped_events_count` first; §8.1's sequential catch-up from the committed checkpoint is the only replay path.

## 9. Standardized Delivery Table Payload & Asynchronous Worker Specification

### 9.1 Standardized Event Payload Format (`payload_json`)

Every row inserted into `deliveries` stores a canonical JSON payload in `payload_json` (`TEXT` column — stored as `TEXT` rather than MySQL binary `JSON` so the exact UTF-8 byte sequence signed by HMAC-SHA256 is preserved verbatim without key re-ordering or whitespace normalization by MySQL's binary JSON serializer):

```
{
  "eventId": "del_01J9Z8K4M2N6P8Q0R2S4T6V8W0",
  "eventType": "USDT_TRANSFER",
  "network": "TRC20",
  "networkMode": "mainnet",
  "webhookId": "wh_01J9Z8J0A1B2C3D4E5F6G7H8J9",
  "planId": "plan_starter_50",
  "matchedAddress": "TXYZopYRdj2D9XRtbG411XZZ3kM5VkAeBf",
  "direction": "INCOMING",
  "transaction": {
    "txHash": "8f3c2b1a9e7d6c5b4a3f2e1d0c9b8a7f6e5d4c3b2a1f0e9d8c7b6a5f4e3d2c1b",
    "logIndex": 0,
    "blockNumber": 65432198,
    "blockTimestamp": 1760458200000,
    "fromAddress": "TFromAddressExample123456789012345",
    "toAddress": "TXYZopYRdj2D9XRtbG411XZZ3kM5VkAeBf",
    "tokenContract": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
    "tokenSymbol": "USDT",
    "decimals": 6,
    "amountRaw": "100500000",
    "amountDecimal": "100.5"
  },
  "createdAt": "2026-10-14T16:50:00.000Z"
}

```

### 9.2 Delivery Worker Polling, `FOR UPDATE SKIP LOCKED` Concurrency, and Retry State Machine

The `DeliveryWorkerService` runs an asynchronous loop (configurable poll interval, default `500ms`, with configurable HTTP concurrency, e.g., `CONCURRENCY = 20` using `Promise.allSettled`):

1. **Atomic Lock-Free Batch Claim (`claimDueDeliveries` via `FOR UPDATE OF d SKIP LOCKED`):** Unlike SQLite, MySQL does not support `UPDATE ... RETURNING`, but MySQL 8.0+ provides native **`SELECT ... FOR UPDATE OF d SKIP LOCKED`**, which is superior for concurrent queue workers: if multiple worker instances poll simultaneously, each worker immediately skips rows locked by another worker without waiting on lock timeouts. Using `idx_deliveries_worker_poll (is_retryable, next_attempt_at, id)`, the worker claims up to `batchSize` (e.g., 50) due deliveries inside a short transaction:

   ```
   -- Step 1 (inside conn.beginTransaction()):
   -- Lock up to :batchSize due rows in deliveries (skipping any rows currently being claimed by peer workers)
   -- and fetch the webhook target URL and secret in the same join.
   SELECT
       d.id,
       d.webhook_id      AS webhookId,
       d.payload_json    AS payloadJson,
       d.attempt_count + 1 AS attemptCount,
       d.max_attempts    AS maxAttempts,
       w.url             AS webhookUrl,
       w.secret          AS webhookSecret
   FROM deliveries AS d
   INNER JOIN webhooks AS w ON w.id = d.webhook_id
   WHERE d.is_retryable = 1
     AND d.next_attempt_at <= ?
   ORDER BY d.next_attempt_at ASC, d.id ASC
   LIMIT ?
   FOR UPDATE OF d SKIP LOCKED;
   
   -- Step 2 (only if Step 1 returned rows):
   -- Transition the claimed IDs to 'PROCESSING' (which automatically sets virtual column is_retryable = NULL,
   -- removing them from idx_deliveries_worker_poll) and commit immediately to release row locks before HTTP I/O.
   UPDATE deliveries
   SET status = 'PROCESSING',
       attempt_count = attempt_count + 1,
       updated_at = ?
   WHERE id IN (?, ?, ...);
   
   -- Step 3: COMMIT;
   
   ```

   Because `FOR UPDATE OF d SKIP LOCKED` locks only the `deliveries` rows (`OF d` avoids locking the shared `webhooks` row!) and commits before any Axios HTTP call starts, row locks are held for `< 1 ms`.

2. **Webhook Dispatch & HMAC Signing:** For each claimed delivery, the worker already has `webhookUrl` and `webhookSecret` from the claim join, computes `X-Webhook-Signature = HMAC_SHA256(webhookSecret, payloadJson)` via `src/utils/generateSignature.ts`, and sends an HTTP `POST` request with a `5000ms` timeout:

   * Headers:

     * `Content-Type: application/json`

     * `X-Webhook-Signature: <hmac-sha256-hex>`

     * `X-Webhook-Id: <webhook_id>`

     * `X-Delivery-Id: <delivery_id>`

     * `X-Delivery-Attempt: <attempt_count>`

3. **Outcome Handling & Exponential Backoff:**

   * **Success (`HTTP 200–299`):**

     ```
     UPDATE deliveries
     SET status = 'DELIVERED',
         last_http_status = ?,
         delivered_at = ?,
         updated_at = ?
     WHERE id = ?;
     
     ```

   * **Transient Failure (`HTTP 408`, `429`, `500–599`, or Network/Timeout Error) where `attempt_count < max_attempts`:** Compute exponential backoff with full jitter: 

     $$
     \text{delayMs} = \min\left(3600000,\; 2000 \times 2^{\text{attempt\_count}}\right) + \text{random}(0, 1000)
     $$

     ```
     UPDATE deliveries
     SET status = 'FAILED',
         next_attempt_at = ? + ?,
         last_http_status = ?,
         last_error = ?,
         updated_at = ?
     WHERE id = ?;
     
     ```

   * **Permanent Exhaustion (`attempt_count >= max_attempts` or non-retryable `HTTP 400/401/404/410`):**

     ```
     UPDATE deliveries
     SET status = 'DEAD_LETTER',
         last_http_status = ?,
         last_error = ?,
         updated_at = ?
     WHERE id = ?;
     
     ```

4. **Stale `PROCESSING` Recovery (Worker Crash Recovery):** On startup and every 60 seconds, the worker resets any row stuck in `status = 'PROCESSING'` where `updated_at < now - 60000` back to `status = 'FAILED'` so deliveries in flight during a process crash are automatically retried:

   ```
   UPDATE deliveries
   SET status = 'FAILED',
       updated_at = ?
   WHERE status = 'PROCESSING'
     AND updated_at < ? - 60000;
   
   ```

   This sweep uses the composite index `idx_deliveries_status_updated (status, updated_at)` (§5.2), probing only the `'PROCESSING'` prefix in sub-millisecond time even when `deliveries` holds millions of `'DELIVERED'` rows.

5. **Delivery semantics — at-least-once.** A delivery can be sent twice when the worker crashes after the endpoint returned `2xx` but before the `DELIVERED` update commits (step 4 then retries it). Consumers must deduplicate on `X-Delivery-Id` (stable per `deliveries.id`; the payload's `eventId` is the same value). Exactly-once end-to-end is explicitly not promised.

### 9.3 Retention, Pruning, and InnoDB Storage Maintenance

`deliveries` is both the queue and the audit log, so it grows forever unless pruned. Specify:

* **Batched Prune Job (`ORDER BY updated_at ASC LIMIT 5000`):** Daily, in bounded batches (5,000 rows per statement, looped until `affectedRows < 5000`), run:

  ```
  DELETE FROM deliveries
  WHERE status IN ('DELIVERED', 'DEAD_LETTER')
    AND updated_at < ?
  ORDER BY status, updated_at ASC
  LIMIT 5000;
  
  ```

  With `DELIVERY_RETENTION_DAYS` default `30`. Never prune `PENDING` / `FAILED` / `PROCESSING` rows — they are live work. Unlike SQLite, MySQL natively supports `ORDER BY ... LIMIT` directly on `DELETE` statements, using `idx_deliveries_status_updated (status, updated_at)` to delete expired rows in short `< 20 ms` transactions without table locks.

* **Why:** Secondary indexes (`idx_deliveries_webhook_status`, `idx_deliveries_block_number`, `uq_deliveries_idempotency`) grow with the table; pruning keeps B+Tree depth shallow, bounds InnoDB buffer-pool churn, and keeps logical/physical backups small. InnoDB automatically reuses deleted page slots (`ROW_FORMAT=DYNAMIC`) for future inserts without requiring `OPTIMIZE TABLE` lockouts.

* **Maintenance cadence:** Run `ANALYZE TABLE deliveries, webhook_addresses, webhooks, subscription_plans;` after daily pruning so optimizer histograms and index cardinality estimates stay fresh. Take backups using `mysqldump --single-transaction` or Percona XtraBackup / MySQL Enterprise Backup for hot non-blocking snapshots.

## 10. REST API Specification (`src/routes/watcherRoutes.ts` Refactor)

All endpoints remain protected by `authenticateApiKey` (`src/middleware/auth.ts`, `X-API-Key`) and validated with `zod`; `GET /api/watcher/health` stays unauthenticated. Because `mysql2/promise` is asynchronous, all route handlers must be `async (req, res, next)` wrapped with Express async error forwarding to `errorHandler.ts`. A single shared API key means there is no per-tenant authorization — any caller can manage any plan or webhook, which matches the "no user signup, manual plan assignment" model.

### 10.1 Subscription Plan Management Endpoints

| Method & Path | Description | Request Body (Zod Schema) | Response / Behavior | 
| ----- | ----- | ----- | ----- | 
| `POST /api/watcher/plans` | Create a subscription plan (or manual plan assignment record). | `{ id?: string, name: string, maxWebhooks: number, maxAddresses: number, maxDeliverableEvents: number }` | `201 Created`: Returns created plan object with current counters (`0`). | 
| `GET /api/watcher/plans/:planId` | Retrieve plan quota limits, used counts, and dropped event counts. | None | `200 OK`: Returns `{ id, name, maxWebhooks, usedWebhooksCount, maxAddresses, usedAddressesCount, maxDeliverableEvents, deliveredEventsCount, droppedEventsCount, isActive }`. | 
| `PATCH /api/watcher/plans/:planId` | Update plan limits, activation, or reset the event counters. | `{ maxWebhooks?: number, maxAddresses?: number, maxDeliverableEvents?: number, resetEventQuota?: boolean, isActive?: boolean }` | `200 OK`: Updates `subscription_plans`. Limits may be lowered below current usage — the plan then rejects all new additions until usage drops below the limit (§7.2), and existing webhooks/addresses are never removed implicitly. `resetEventQuota: true` zeroes `deliveredEventsCount` / `droppedEventsCount` in the same transaction and logs the previous values. | 
| `POST /api/watcher/plans/:planId/reconcile` | Recompute derived counters from the source tables. | None | `200 OK`: Locks the plan row (`FOR UPDATE`), recomputes `usedWebhooksCount` from `COUNT(*)` over `webhooks` and `usedAddressesCount` from `COUNT(*)` over `webhook_addresses`, writes them, and returns `{ stored, recomputed }` (§7.2). | 

### 10.2 Webhook & Address Registration Endpoints

| Method & Path | Description | Request Body (Zod Schema) | Quota & Validation Rules | 
| ----- | ----- | ----- | ----- | 
| `POST /api/watcher/webhooks` | Create a new webhook and manually assign a `planId`. Optionally attach an initial array of addresses. | `{ planId: string, url: string, secret?: string, networkFilter?: 'TRC20' | 'BEP20' | 'ALL', addresses?: Array<{ network: 'TRC20' | 'BEP20', address: string }> }` | Verifies `used_webhooks_count + 1 <= max_webhooks` AND `used_addresses_count + newAddresses <= max_addresses` atomically via conditional `UPDATE`s inside an InnoDB transaction (§7.2). Returns `403` if either limit is exceeded. Any address whose `network` contradicts the webhook's `networkFilter` (when not `ALL`) is rejected with `400 ADDRESS_NETWORK_MISMATCH`. | 
| `POST /api/watcher/webhooks/:webhookId/addresses` | Bulk-assign watched addresses to an existing webhook. | `{ addresses: Array<{ network: 'TRC20' | 'BEP20', address: string }> }` (up to 1,000 per request) | Normalizes addresses (§6.2); rejects any address whose `network` contradicts the webhook's `networkFilter` unless it is `ALL` (`400 ADDRESS_NETWORK_MISMATCH`); executes a single multi-row `INSERT … ON DUPLICATE KEY UPDATE` no-op and checks `used_addresses_count + affectedRows <= max_addresses` (§7.2). If `address 123` is already on `webhook1` and is now added to `webhook2`, it increments `used_addresses_count` again. Assigning an address never backfills already-scanned blocks (§7.1). | 
| `DELETE /api/watcher/webhooks/:webhookId/addresses` | Remove specific addresses from a webhook and free address quota. | `{ addresses: Array<{ network: 'TRC20' | 'BEP20', address: string }> }` | Deletes matching rows from `webhook_addresses` and decrements `used_addresses_count` by the exact `affectedRows` count via `used_addresses_count - LEAST(used_addresses_count, :affectedRows)` (§7.2). | 
| `DELETE /api/watcher/webhooks/:webhookId` | Delete a webhook and all of its associated addresses. | None | Locks the plan row, counts the webhook's address rows first, cascades deletion to `webhook_addresses`, and decrements `used_webhooks_count` by `1` and `used_addresses_count` by the counted rows in the same InnoDB transaction (§7.2). | 
| `PATCH /api/watcher/webhooks/:webhookId` | Update URL, secret, `networkFilter`, `isActive`, or move the webhook to another plan. | `{ url?: string, secret?: string, networkFilter?: 'TRC20' | 'BEP20' | 'ALL', isActive?: boolean, planId?: string }` | One InnoDB transaction. Narrowing `networkFilter` while conflicting addresses exist is rejected with `409 NETWORK_FILTER_CONFLICT` listing them (the client must remove the addresses first). Moving to another plan locks both plans in ascending `id` order, re-checks the target plan's `max_webhooks` / `max_addresses`, and updates `webhooks.plan_id` plus the denormalized `webhook_addresses.plan_id` together (§7.2). Setting `isActive: false` pauses matching without affecting quotas (§7.1). | 
| `GET /api/watcher/webhooks/:webhookId` | Inspect one webhook: config, plan assignment, and address counts. | Query params: `?includeAddresses=true&limit=200&cursor=...` | `200 OK`: Webhook row, `plan_id`, `is_active`, `network_filter`, and address counts (plus a keyset-paginated address list when requested via `idx_webhook_addresses_webhook_id`). | 
| `GET /api/watcher/deliveries` | Inspect delivery queue status and filter by `webhookId`, `planId`, or `status`. | Query params: `?webhookId=...&status=PENDING|DELIVERED|FAILED|DEAD_LETTER&limit=50` | Returns paginated delivery logs and retry errors. | 
| `GET /api/watcher/stats` | Enhanced system health, checkpoint heights, queue depth, and scanner-lease state. | None | Returns `block_checkpoints` per network (including `lease_owner` / `lease_expires_at`), pending delivery count (`WHERE is_retryable = 1`), retention horizon, MySQL pool connection metrics, and aggregate plan metrics. | 

### 10.3 Route Migration (Legacy → Target)

| Legacy route (`72c5fe0`) | Target | Notes | 
| ----- | ----- | ----- | 
| `POST /watch` | `POST /api/watcher/webhooks` (+ `POST …/:webhookId/addresses`) | The per-transaction watch contract has no direct equivalent; callers create a webhook and attach addresses (§2.1). | 
| `DELETE /watch/:id` | `DELETE /api/watcher/webhooks/:webhookId` | — | 
| `GET /watch/:id` | `GET /api/watcher/webhooks/:webhookId` | Returns webhook config + address counts instead of a transaction status. | 
| `GET /networks`, `GET /networks/:network` | folded into `GET /api/watcher/stats` | Checkpoint heights, lease state, and queue metrics. | 
| `PATCH /admin/networks/:network`, `POST /admin/networks/:network/enable`, `POST /admin/networks/:network/disable` | removed | Network enablement moves to configuration/env (§2.2). | 
| `GET /stats` | `GET /api/watcher/stats` | Same role, new metrics. | 
| `GET /health` | `GET /api/watcher/health` | Unauthenticated; add MySQL pool liveness (`SELECT 1`) and scanner-lease state. | 

## 11. Proposed Repository File Structure & Migration Plan

To cleanly refactor [moha-abdi/tether-watcher](https://github.com/moha-abdi/tether-watcher), update `package.json` with pinned runtime dependencies — `mysql2@^3.11.0` and `ulid` (or Node's native `crypto.randomUUID()`) — and add `"engines": { "node": ">=20" }`. Keep the existing stack (`express`, `tronweb`, `ethers` v6, `axios`, `zod`, `winston`). Reorganize `src/` as follows:

```
src/
├── index.ts                          # Express app init, MySQL pool + migration init, starts BlockScanner & DeliveryWorker
├── db/
│   ├── database.ts                   # Singleton mysql2/promise Pool, session hook (§4.2), transaction helper (withTransaction), migration runner (schema_migrations table)
│   ├── schema.sql                    # 001: InnoDB DDL for subscription_plans, webhooks, webhook_addresses, block_checkpoints, deliveries + sp_commit_block procedure
│   └── queries.ts                    # Parameterized SQL constants for binary-protocol pool.execute() (incl. JSON_TABLE + STRAIGHT_JOIN lookup, §6.3)
├── middleware/
│   ├── auth.ts                       # Existing API key authentication (X-API-Key)
│   └── errorHandler.ts               # Extended to map QuotaExceededError -> HTTP 403 and MySQL ER_LOCK_DEADLOCK / ER_LOCK_WAIT_TIMEOUT
├── repositories/
│   ├── PlanRepository.ts             # Async CRUD & atomic quota updates for subscription_plans; counter reconciliation (§7.2)
│   ├── WebhookRepository.ts          # Async CRUD for webhooks & multi-row INSERT … ON DUPLICATE KEY UPDATE address management
│   ├── CheckpointRepository.ts       # Async read/write block_checkpoints + scanner lease acquire/renew/release + CALL sp_commit_block (§8.2, §8.3)
│   └── DeliveryRepository.ts         # Outbox insertion, FOR UPDATE OF d SKIP LOCKED batch claiming, status transitions, batched DELETE retention pruning (§9.3)
├── services/
│   ├── BlockScannerService.ts        # Per-network sequential scanner: lease (§8.3), JSON_TABLE + STRAIGHT_JOIN batch lookup (§6.3), single-roundtrip sp_commit_block (§8.2)
│   ├── DeliveryWorkerService.ts      # Claims deliveries via FOR UPDATE SKIP LOCKED (§9.2), signs payloads, Axios POSTs with backoff, stale-PROCESSING recovery
│   └── providers/
│       ├── TronBlockProvider.ts      # Per-height TRC20 fetch (block hash/parent/timestamp + Transfer events for exactly that height, fingerprint-paged); normalizes 41-hex -> Base58
│       └── BscBlockProvider.ts       # Per-height BEP20 fetch: getBlock(N) + getLogs({address: USDT, topics: [Transfer], fromBlock: N, toBlock: N}); retries per provider caps
├── routes/
│   └── watcherRoutes.ts              # Async Express routes for /plans, /webhooks, /deliveries, /stats, /health
└── utils/
    ├── addressNormalizer.ts          # Lowercase EVM hex and Tron Base58 normalization + validation
    ├── generateSignature.ts          # Existing HMAC-SHA256 generator (updated to accept per-webhook secret)
    ├── logger.ts                     # Existing Winston logger
    └── webhook.ts                    # Axios HTTP dispatcher returning { success, status, error }

```

**Provider fetch contract (required for the "0 missed blocks" target):**

* **TRC20 (`TronBlockProvider`):** Fetch exactly one height per request — TronGrid's contract-events endpoint filtered to `min_block_number = max_block_number = N`, `only_confirmed = false` (finality is enforced by `networkConfirmations`, §8.1), `limit = 200`, paging on the returned `fingerprint` until exhausted. Assert `block_number === N` on every returned event; reject and retry the block otherwise, never partially accept it. Fetch the block itself for `hash`, `parentHash`, and `timestamp` (needed by the §8.3 continuity check). TRON solidity depth (\~19 blocks) is the reference for `TRC20_CONFIRMATIONS = 20`.

* **BEP20 (`BscBlockProvider`):** `getBlock(N)` plus `getLogs({ address: USDT, topics: [Transfer], fromBlock: N, toBlock: N })`. One-block ranges avoid the common `eth_getLogs` block/result caps of public endpoints; if a provider still rejects the range (result cap or rate limit), back off exponentially and retry the same block — never skip it and never advance the checkpoint on a partial result.

* **Resume depth:** Catch-up (§8.1) can span thousands of blocks, so the configured endpoints must be archive-capable for the chosen `START_BLOCK_*`; otherwise the initial deployment must start near the safe head.

* **Rate limits:** One in-flight block per network, processed sequentially; on `429`/timeouts, back off and retry the same block. Quota/checkpoint writes happen only inside `sp_commit_block` (§8.2).

## 12. Performance Benchmarks, Testing, and Acceptance Criteria

### 12.1 Performance & Scalability Targets

| Metric | Target Requirement | Architectural Mechanism | 
| ----- | ----- | ----- | 
| **Node.js Memory Usage with 1,000,000 Watched Addresses** | `< 150 MB` RSS (constant $O(1)$ relative to address-table size) | Zero in-memory address `Map`; addresses reside in MySQL `InnoDB` clustered B+Tree (`PRIMARY KEY (network, address, webhook_id)`) cached inside `innodb_buffer_pool_size`. | 
| **Address Lookup Latency per Block (**$\le 500$ **unique block addresses)** | `< 3.0 ms` per block at 1,000,000 stored addresses over local socket / same-VPC MySQL | Single `JSON_TABLE()` batch query with `CHARACTER SET ascii COLLATE ascii_bin` and `STRAIGHT_JOIN` probing `PRIMARY KEY (network, address, webhook_id)` (§6.3). | 
| **Block Commit Latency (Checkpoint + 50 Matched Deliveries)** | `< 3.0 ms` per block on local NVMe/SSD (`innodb_flush_log_at_trx_commit = 2`) | Single-roundtrip stored procedure `CALL sp_commit_block(...)` under `READ COMMITTED` isolation (§8.2), eliminating per-match network RTT. | 
| **Crash Recovery Accuracy (process crashes)** | `0` skipped blocks, `0` duplicate `deliveries` rows (delivery to the endpoint is at-least-once, §9.2) | Atomic InnoDB transaction in `sp_commit_block`; `UNIQUE KEY uq_deliveries_idempotency`; fenced scanner lease prevents overlapping scans (§8.3). | 

### 12.2 Verification & Acceptance Test Suite

1. **Duplicate Address Across Webhooks Quota Test (plus Case-Sensitive TRON Base58 Test):**

   * Create a plan with `max_webhooks = 50, max_addresses = 2, max_deliverable_events = 50`.

   * Create `webhook1` and `webhook2` under that plan.

   * Assign address `0xabc...123` to `webhook1` (`used_addresses_count` becomes `1`).

   * Assign the same address `0xabc...123` to `webhook2` (`used_addresses_count` becomes `2`).

   * Attempt to assign address `0xdef...456` to `webhook1` $\rightarrow$ Verify API rejects with `403 ADDRESS_QUOTA_EXCEEDED`.

   * Verify on a separate plan that two TRON Base58 strings differing only in letter case do **not** collide under `COLLATE ascii_bin`.

2. **Event Quota Exhaustion & Drop Counting Test:**

   * Create a plan with `max_deliverable_events = 2`.

   * Simulate a block containing 5 USDT transfers to a watched address under that plan via `sp_commit_block`.

   * Verify that exactly **2** rows are inserted into `deliveries`, `delivered_events_count` equals `2`, and `dropped_events_count` equals `3`.

3. **Mid-Block Crash & Lease-Fencing Recovery Test:**

   * Process block `100` (checkpoint `last_processed_block = 100`).

   * Simulate a failure or stolen lease during block `101` before transaction commit.

   * Restart `BlockScannerService` $\rightarrow$ Verify scanner resumes at block `101` (not `102` and not `latestBlock`) and that partial quota increments from the aborted block `101` were rolled back.

4. **Delivery Worker Outbox, `SKIP LOCKED` Concurrency & Retry Test:**

   * Configure a mock webhook endpoint that returns `HTTP 502` twice and `HTTP 200` on the third attempt.

   * Run two concurrent `DeliveryWorkerService` instances against 100 queued deliveries and verify zero duplicate claims (`FOR UPDATE OF d SKIP LOCKED`).

   * Verify the target row in `deliveries` transitions `PENDING` $\rightarrow$ `PROCESSING` $\rightarrow$ `FAILED` (attempt 1) $\rightarrow$ `PROCESSING` $\rightarrow$ `FAILED` (attempt 2) $\rightarrow$ `PROCESSING` $\rightarrow$ `DELIVERED` (attempt 3) with valid `X-Webhook-Signature`.

5. **MySQL Optimizer Plan Regression Test (blocking):**

   * Seed $\ge 100,000$ `webhook_addresses` rows (multiple plans/webhooks), run `ANALYZE TABLE webhook_addresses, webhooks, subscription_plans, deliveries;`, and run `EXPLAIN FORMAT=JSON` (and `EXPLAIN FORMAT=TREE`) for the §6.3 batch statement with a representative block-address JSON array.

   * Assert the join order starts with `batch` (`JSON_TABLE`), probes `wa` via `PRIMARY` (`key: "PRIMARY"`), and does **not** use `idx_webhook_addresses_plan_id` or `idx_webhook_addresses_webhook_id` for `wa` or perform a `convert()` charset cast; also assert the §9.2 claim statement uses `idx_deliveries_worker_poll`. Fail CI otherwise.

6. **Scanner Lease & Database-Level Fencing Test:**

   * Start scanner A for `BEP20` and verify it acquires the lease; start scanner B for the same network and verify it fails fast while A heartbeats.

   * Expire A's lease artificially, allow B to acquire it, and verify that if A attempts to call `sp_commit_block`, MySQL rejects A's commit with `SQLSTATE '45000' (SCANNER_LEASE_LOST)`.

7. **Network-Filter Enforcement Test:**

   * Create `webhook1` with `networkFilter = 'TRC20'`; verify `POST …/webhook1/addresses` rejects a BEP20 address with `400 ADDRESS_NETWORK_MISMATCH` and that no BEP20 transfer ever produces a candidate for `webhook1` in the §6.3 batch query.

   * Set the filter to `ALL`, add the BEP20 address, and verify BEP20 matches now appear.

### 12.3 Open Verification Items (need a live MySQL 8.0.30+/8.4 before implementation)

1. **`CHARACTER SET ... COLLATE ...` inside `JSON_TABLE ... COLUMNS (...)`** (§6.3). The manual documents the column grammar as `name type PATH path` with "type: any MySQL scalar data type" but says nothing about a collation clause inside `COLUMNS`. Confirm the target server accepts it; §12.2 test 5 already asserts the *result* (a `PRIMARY` probe on `wa` with no `convert()`). Fallback if rejected: `wa.address = CONVERT(batch.address USING ascii)`.
2. **`SELECT COUNT(*) ... FOR UPDATE` as the lease fence** in `sp_commit_block` (§8.2). The manual does not state whether a locking clause is permitted on an aggregate query. If it is rejected, use `SELECT lease_owner, lease_expires_at INTO v_owner, v_expires FROM block_checkpoints WHERE network = p_network FOR UPDATE` and test both values in an `IF`.
3. **Malformed candidate JSON** (§8.2). `JSON_TABLE` defaults are `NULL ON EMPTY` and `NULL ON ERROR` (documented), so a missing field arrives as `NULL`, the `INSERT` fails on a `NOT NULL` column, and the `EXIT HANDLER` rolls the whole block back. Decide whether that loud failure is acceptable or declare `ERROR ON ERROR` for a clearer diagnostic.
4. **Latency targets** (§6.3 table, §12.1) are unmeasured — run the §12.2 benchmark on the target host before treating `< 3.0 ms` as a commitment.