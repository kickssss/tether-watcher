# Product Requirements Document: Converting Tether-Watcher to High-Performance Stateful SQLite 3 Architecture

**Target Repository:** [moha-abdi/tether-watcher](https://github.com/moha-abdi/tether-watcher) (formerly `TronWatch`; canonical remote `git@github.com:moha-abdi/tether-watcher.git`)  
**Document Version:** 1.2  
**Target Stack:** Node.js ≥ 22, TypeScript 5.x or newer, Express.js, `@libsql/client@^0.18` (libSQL core = SQLite 3.45.1, providing `unixepoch('subsec')` and `json_each`), TronWeb, Ethers.js v6, Axios, Zod  
**Revision 1.1 (validation pass):** §2 audit corrected against the actual source tree (`72c5fe0`); §6.3 batch query must force the join order (`CROSS JOIN` — measured 15.99 s → 2.21 ms at 1M addresses); per-webhook `confirmations_required` replaced by per-network confirmation depth (§8.1); added scanner lease and exactly-once scan invariant (§8.3), delivery retention (§9.3), counter reconciliation and webhook re-planning (§10), provider fetch contracts (§11), and durability/at-least-once/reorg statements (§8.2, §8.3, §9.2).  

**Revision 1.2 (driver decision):** the driver is `@libsql/client` (libSQL core, SQLite 3.45.1) instead of `better-sqlite3`, and §4.1 now documents the three driver behaviors the design depends on — writes must be serialized in-process (overlapping write transactions fail fast with `SQLITE_BUSY`), `rowsAffected` is 0 for `RETURNING` statements (use `rows.length`), and there are no application-level statement handles. `Database.write(fn)` exposes a `query`/`run` transaction facade (`tx.run` → affected rows, `tx.query` → rows); §7.2, §8.2, §9.2, and §11 were updated to match, and `engines.node` is now `>=22`.

## 1. Executive Summary: Stateful Persistence Decouples Block Scanning from Webhook Delivery

The existing [moha-abdi/tether-watcher](https://github.com/moha-abdi/tether-watcher) service monitors USDT transactions across **TRON (TRC20)** and **BNB Smart Chain (BEP20)** using an entirely stateless, in-memory design. When the Express server boots (`src/index.ts`), `TetherWatcher` (`src/services/TetherWatcher.ts`) initializes in-memory `Map` structures for *watched transactions* (`watchedTransactions`, `TetherWatcher.ts:96` — each entry holds an exact `fromAddress` + `expectedRecipientAddress` + `amount` triple, a 5-minute `expiresAt` and `maxAttempts = 3`) and for *processed events* (`processedEvents`, `:97`), initializes per-network cursors to zero (`lastProcessedBlocks`, `:98–101`), then starts two polling loops that dispatch webhooks inline via Axios (`src/utils/webhook.ts`). TRON polls TronGrid's *latest confirmed* Transfer-event window rather than an explicit block range (`onlyConfirmed: true`, `:303`); BSC scans `max(currentBlock - 100, cursor)` on every tick (`:340–343`). Any process restart or crash wipes all registered watch targets and loses block progress; webhook dispatch failures are caught and only logged — they are never retried (`src/utils/webhook.ts`).

This PRD defines the architectural conversion of `tether-watcher` into a stateful, crash-resilient, high-throughput blockchain indexing and webhook delivery engine backed by **SQLite 3**. The redesigned system satisfies four core operational mandates:

1. **Zero Full-Memory Address Loading with Sub-Millisecond SQLite Lookups:** All watched addresses and webhook associations reside on disk in a clustered `WITHOUT ROWID` SQLite B-Tree index. Block scanners never load the full address table into Node.js memory; instead, they execute single-roundtrip block-level batch queries against memory-mapped SQLite index pages.
2. **Atomic Block Checkpointing for Crash Recovery:** After every fully scanned block, the scanner commits the block number, matched delivery records, and quota updates in a single atomic SQLite transaction (`BEGIN IMMEDIATE ... COMMIT`). Upon restart after a crash, the scanner resumes at `last_processed_block + 1` rather than the current head block.
3. **Subscription Plan Quota Enforcement:** Manually assigned subscription plans govern the maximum number of webhooks (`max_webhooks`), maximum assigned addresses (`max_addresses`, counting duplicate addresses across separate webhooks distinctly), and maximum deliverable events (`max_deliverable_events`). Events matching a subscription that has exhausted its event quota are ignored and recorded as dropped (`dropped_events_count`).
4. **Decoupled Transactional Outbox (`deliveries` Table) and Worker:** Block scanning writes matched events in a standardized schema to the `deliveries` table and immediately advances to the next block. A dedicated asynchronous Delivery Worker polls pending deliveries, signs payloads via HMAC-SHA256 (`src/utils/generateSignature.ts`), manages retries with exponential backoff, and updates delivery state independently.

## 2. Current Architecture Audit: In-Memory Bottlenecks and Failure Modes in Tether-Watcher

An audit of the [moha-abdi/tether-watcher](https://github.com/moha-abdi/tether-watcher) codebase (verified at commit `72c5fe0`, branch `main`) highlights the exact modules that require refactoring to support persistent SQLite state, subscription quotas, and high-throughput address matching. **The current service watches individual transactions (exact sender + recipient + amount), not addresses — the target design below is an address-subscription redesign, not a storage swap (see §2.1).**

| Existing File / Component                                    | Current Behavior at `72c5fe0`                                | Architectural Gap / Failure Mode                             | Required Target State in SQLite 3 Conversion                 |
| :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| `src/services/TetherWatcher.ts` (`watchedTransactions` Map, `:96`) | Holds `WatchedTransaction` records keyed by a caller-supplied `id`: exact `fromAddress` + `expectedRecipientAddress` + `amount` (smallest unit), `webhookUrl`, `expiresAt = now + 5 min` (`:280`), `maxAttempts = 3` (`:282`). Every Transfer event is matched by scanning the entire Map (`processTransferEvent`, `:437` onward). No address-indexed structure and no multi-address subscription exist. | Watch targets and webhook URLs vanish on restart; per-event matching cost is O(watched transactions); one watched address per watch entry (either side); the 5-minute expiry ages out slow confirmations. | Replace with `webhooks` + `webhook_addresses` (`PRIMARY KEY (network, address, webhook_id)`) queried by the forced-order batch statement in §6.3. |
| `src/services/TetherWatcher.ts` (`lastProcessedBlocks`, `:98–101`; exposed read-only via `getNetworkStatus`/`getLastProcessedBlock`) | Private `Record<Network, bigint>` initialized to `0`. TRON writes the head height after polling the latest confirmed event window (`:316`); BSC scans `max(currentBlock - 100, cursor)` (`:340–343`) and then writes the head (`:356`). It is not a `NetworkConfig` field. | Nothing survives a restart: BSC restarts at `currentBlock - 100` (~5 min of blocks), TRON re-reads TronGrid's rolling confirmed-event window; downtime beyond that window is never scanned. The overlapping 100-block lookback also re-processes events on every tick. | Persist the per-network cursor in `block_checkpoints` with a single-scanner lease; advance it only inside the atomic per-block commit (§8.2, §8.3). |
| `src/services/TetherWatcher.ts` (`processedEvents` Map, `:97`) | Dedupe keys `TRON:<txid>:<eventIndex>` / `BSC:<txHash>:<logIndex>`; the maintenance sweep runs every 60 s (`:217–223`) but evicts only entries older than 24 h (`:737–743`). | A crash forgets every processed event, so re-scanned blocks can re-notify; resident memory tracks a rolling 24 h of block volume; TTL eviction silently re-enables duplicates after 24 h. | Enforce idempotency persistently via the `deliveries` `UNIQUE (webhook_id, network, tx_hash, log_index, matched_address)` constraint plus the §8.2 guard. |
| `src/utils/webhook.ts` (inline Axios dispatch)               | `sendWebhook()` awaits `axios.post` directly inside the polling loops (`finalizeTransaction`, `handleFailedTransaction`, `cleanupExpiredTransactions`); errors are caught and logged — no retry, no persistence. The maintenance loop's `retryFailedTransactions()` (`:746`) only re-arms transaction *verification*; it never re-sends a webhook. | A slow or timing-out endpoint stalls the block scanner; a failed notification is lost permanently; no delivery audit trail. | Decouple via the `deliveries` transactional outbox drained by `DeliveryWorkerService` (§3, §9). |
| `src/routes/watcherRoutes.ts`                                | 9 routes, API-key protected except `GET /health`: `POST /watch`, `DELETE /watch/:id`, `GET /watch/:id`, `GET /networks`, `GET /networks/:network`, `PATCH /admin/networks/:network`, `POST /admin/networks/:network/enable`, `POST /admin/networks/:network/disable`, `GET /stats`. Zod validates the transactional watch payload (`watchSchema`). | No plans, quotas, webhook/address CRUD, or delivery inspection; the entire surface is bound to the per-transaction model. | Replace with the plan/webhook/address/delivery API in §10; see the route migration table in §10.3. |

### 2.1 Semantic Migration: Transaction Watches → Address Subscriptions

The target design changes *what* is watched, so this is a breaking redesign of the watch contract rather than a drop-in storage change:

| Legacy concept (`WatchedTransaction`, TRC20/BEP20)          | Target equivalent                                            | Notes                                                        |
| :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| `id` (caller-supplied watch ID)                              | `webhooks.id` (server-generated)                              | Callers manage a webhook, not a one-off watch.               |
| `fromAddress` / `expectedRecipientAddress`                   | two `webhook_addresses` rows (one per address)                | Matching becomes address-based (`from` or `to`), not an exact triple; one transfer can now match several subscriptions (counted per pair, §7.1). |
| `amount` (exact smallest-unit match)                         | dropped                                                       | Amounts no longer gate delivery. An optional per-address minimum-amount filter is a possible follow-up and is explicitly **out of scope** here. |
| `webhookUrl` (per watch)                                     | `webhooks.url` + per-webhook `secret`                         | Signing switches from the global `WEBHOOK_SECRET` to per-webhook secrets; `src/utils/generateSignature.ts` must accept the secret as an argument. |
| 5-minute `expiresAt`, `maxAttempts = 3`, statuses `PENDING/COMPLETED/FAILED/EXPIRED` | outbox lifecycle `PENDING/PROCESSING/DELIVERED/FAILED/DEAD_LETTER` | Expiry notifications disappear: a subscription persists until deleted, and success/failure is tracked per delivery row (§9.2). |
| Testnet mode (`NODE_ENV === "development"`, `isTestMode`)     | unchanged boot-time switch                                    | Testnet and mainnet remain separate deployments with different contracts/RPCs and their own database file. |

### 2.2 Legacy Behaviors Deliberately Not Carried Over

- **Exact-amount matching and 5-minute expiry** — replaced by persistent subscriptions (§2.1).
- **Per-transaction verification retries (`maxAttempts = 3`)** — replaced by checkpoint resume plus RPC-level retries in the scanner; HTTP retries live in the outbox (§9.2).
- **In-memory 24 h processed-event TTL** — replaced by the persistent idempotency constraint on `deliveries` (§5.2, §8.2).
- **Runtime network admin endpoints** (`PATCH /admin/networks/:network`, `.../enable`, `.../disable`) — network enablement moves to configuration/env; a persistent `networks` configuration table is out of scope for this PRD. If runtime toggling is still required, it must be specified separately (including what happens to in-flight blocks and the scanner lease).

## 3. System Architecture: Decoupled Block Indexer and Outbox Delivery Pipeline

To maximize block-scanning throughput and guarantee zero missed blocks, the monolithic `TetherWatcher` loop is split into two decoupled subsystems sharing a single local SQLite 3 database configured in **Write-Ahead Logging (`WAL`)** mode:

1. **Subsystem A — Chain Block Scanner (`BlockScannerService`):**
   - Runs one independent sequential loop per enabled network (`TRC20` via TronWeb, `BEP20` via Ethers.js).
   - Acquires the per-network scanner lease before doing any work, then reads the last committed block number from `block_checkpoints` (§8.1, §8.3).
   - Fetches full block transaction logs/events for block $N$ (`last_processed_block + 1`) once block $N$ has reached the network's confirmation depth (`latestChainBlock - networkConfirmations[network] >= N`; defaults TRC20 = 20, BEP20 = 12 — §8.1).
   - Extracts all USDT `Transfer(address indexed from, address indexed to, uint256 value)` events from block $N$ and verifies that block $N$'s parent hash equals `block_checkpoints.last_block_hash` (§8.3).
   - Executes one forced-order batch lookup against `webhook_addresses` (the `CROSS JOIN` form in §6.3) to find all active subscribed webhooks matching either `from` or `to` addresses in block $N$, honoring each webhook's `network_filter`.
   - Evaluates each matched webhook's subscription plan event quota (`delivered_events_count < max_deliverable_events`).
   - Inside a single `BEGIN IMMEDIATE` transaction: inserts permitted events into `deliveries`, increments `delivered_events_count` for queued events, increments `dropped_events_count` for over-quota matches, and updates `block_checkpoints.last_processed_block = N`.
2. **Subsystem B — Asynchronous Webhook Delivery Worker (`DeliveryWorkerService`):**
   - Polls the `deliveries` table for rows where `status IN ('PENDING', 'FAILED') AND next_attempt_at <= current_timestamp`, using partial index `idx_deliveries_worker_poll`.
   - Atomically claims batches of deliveries (`status = 'PROCESSING'`, §9.2) so one or more worker processes cannot double-send; delivery is **at-least-once**, not exactly-once (§9.2).
   - Dispatches HTTP POST requests using Axios (`src/utils/webhook.ts`) with the standardized JSON payload and `X-Webhook-Signature` header (`src/utils/generateSignature.ts`).
   - Marks successful responses (`2xx`) as `DELIVERED` and schedules failed deliveries (`4xx`/`5xx`/timeout) for exponential backoff retry or `DEAD_LETTER` exhaustion.

## 4. SQLite 3 Database Engine Selection and High-Performance Tuning

### 4.1 Driver Selection: `@libsql/client` (libSQL) with an Application-Enforced Single Writer

The implementation uses **`@libsql/client` (`^0.18`)** against a local `file:` database. libSQL's core is a SQLite fork (`sqlite_version()` = **3.45.1**), so `STRICT`, `WITHOUT ROWID`, `json_each()`, `RETURNING`, and `unixepoch('subsec')` (requires SQLite ≥ 3.42) are all available, and the §6.3 query plan was verified identical on this engine (`SEARCH wa USING PRIMARY KEY`).

The client is **asynchronous**, which changes three things the design must handle explicitly. All three were measured on the installed driver before any code was written:

- **Writes are serialized in the process.** Two overlapping write transactions — or any statement issued while a write transaction is open — fail *immediately* with `SQLITE_BUSY`; the connection's `busy_timeout` does not make the driver wait. `Database.write(fn)` therefore funnels every write through one promise queue and retries once on a genuine `SQLITE_BUSY` (i.e. another process holding the lock). Reads never take that queue: WAL readers are not blocked and do not observe uncommitted rows.
- **`rowsAffected` is unreliable for `RETURNING`.** Plain `INSERT`/`UPDATE`/`DELETE` report their change counts correctly, but libsql returns `rowsAffected === 0` for `UPDATE … RETURNING` even when rows were updated. The `Tx` facade encodes the distinction: `tx.run(sql, args)` returns the affected-row count for conditional quota updates, while `tx.query(sql, args)` returns rows and the §9.2 claim reads `rows.length`.
- **No application-level statement handles.** The client exposes `execute`/`batch`/`transaction` rather than prepared-statement objects, so hot-path SQL lives as constants in `src/db/statements.ts` and is executed with named parameters.

Transaction control is explicit: `Database.write` opens `client.transaction("write")`, awaits the callback, commits, and always closes the transaction (`close()` rolls back when no commit happened). Because every write — block commits, API mutations, worker state transitions — shares the same queue, a multi-statement block commit can never be interleaved with an API mutation.

### 4.2 Mandatory SQLite `PRAGMA` Configuration at Startup

When initializing the database connection in `src/db/database.ts`, the service must execute the following `PRAGMA` statements immediately upon opening the database file:

```sql
-- Enable Write-Ahead Logging for concurrent reads during writes
PRAGMA journal_mode = WAL;

-- Balance crash-safety with high write throughput (fsync only at WAL checkpoints)
PRAGMA synchronous = NORMAL;

-- Allocate 64 MB of RAM for the SQLite page cache (negative value = KiB)
PRAGMA cache_size = -64000;

-- Memory-map up to 256 MB of the database file to eliminate read() syscalls on index B-Trees.
-- NOTE: memory-mapped pages are file-backed and reclaimable, but they still count toward process RSS.
-- If a hard RSS bound is required (see §12.1), lower this to 67108864 (64 MB) or omit mmap entirely.
PRAGMA mmap_size = 268435456;

-- Store temporary tables, indices, and json_each() virtual tables in RAM
PRAGMA temp_store = MEMORY;

-- Enforce referential integrity across plans, webhooks, addresses, and deliveries
PRAGMA foreign_keys = ON;

-- Wait up to 5000ms if another connection holds a write lock before throwing SQLITE_BUSY
PRAGMA busy_timeout = 5000;

-- Optimize WAL auto-checkpoint threshold to 4000 pages (~16 MB)
PRAGMA wal_autocheckpoint = 4000;
```

**Engine/driver pin:** `unixepoch('subsec')` requires SQLite ≥ 3.42.0; pin `@libsql/client@^0.18`, whose libSQL core reports **3.45.1** (verified with `select sqlite_version()`), and keep the JSON SQL functions available (`json_each`, §6.3). The driver bundles its own engine, so the exact query plan is pinned by the regression test in §12.2 (test 5) rather than by the runtime version. Execute `PRAGMA optimize;` after schema or bulk-data changes and periodically (e.g. daily) so planner statistics stay current for the hot-path statements.

## 5. Complete SQLite 3 Database Schema and Indexing Specification

### 5.1 Entity Relationship Overview

The database schema consists of five core tables designed to support manual plan assignment, strict quota accounting, $O(\log N)$ address matching without full-table memory loads, block checkpointing, and outbox delivery queuing:

1. `subscription_plans`: Reusable plan templates defining quota ceilings (`max_webhooks`, `max_addresses`, `max_deliverable_events`) or direct per-tenant plan records.
2. `webhooks`: Registered webhook endpoints. In accordance with the requirement to manually assign a plan to each webhook (or group of webhooks under a plan instance), each webhook references a `plan_id` (representing the assigned subscription plan instance that tracks quota usage across its linked webhooks).
3. `webhook_addresses`: Clustered `WITHOUT ROWID` junction table mapping `(network, address, webhook_id)`. Assigning address `123` to `webhook1` and `webhook2` creates two distinct rows, counting as **2** against the plan's `max_addresses` limit.
4. `block_checkpoints`: Stores the last fully processed block height per network (`TRC20`, `BEP20`), the last processed block hash, and the single-scanner lease (`lease_owner`, `lease_expires_at`, §8.3).
5. `deliveries`: Outbox queue storing standardized matched blockchain events awaiting webhook dispatch.

### 5.2 DDL Schema Definition (`src/db/migrations.ts`)

```sql
-- 1. SUBSCRIPTION PLANS (Manual Plan / Quota Assignment)
-- Each row represents a manually provisioned subscription plan instance.
-- Can be assigned to a single webhook or shared across multiple webhooks belonging to one customer.
CREATE TABLE IF NOT EXISTS subscription_plans (
    id                      TEXT PRIMARY KEY,               -- e.g., 'plan_ent_01' or UUID
    name                    TEXT NOT NULL,                  -- e.g., 'Starter 50x50x50', 'Pro Tier'
    max_webhooks            INTEGER NOT NULL CHECK (max_webhooks >= 0),
    max_addresses           INTEGER NOT NULL CHECK (max_addresses >= 0),
    max_deliverable_events  INTEGER NOT NULL CHECK (max_deliverable_events >= 0),
    
    -- Materialized quota counters maintained atomically via triggers / transactions
    used_webhooks_count     INTEGER NOT NULL DEFAULT 0 CHECK (used_webhooks_count >= 0),
    used_addresses_count    INTEGER NOT NULL DEFAULT 0 CHECK (used_addresses_count >= 0),
    delivered_events_count  INTEGER NOT NULL DEFAULT 0 CHECK (delivered_events_count >= 0),
    dropped_events_count    INTEGER NOT NULL DEFAULT 0 CHECK (dropped_events_count >= 0),
    
    is_active               INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 ),
    updated_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 )
) STRICT;

-- 2. WEBHOOKS
-- Persistent storage for webhook endpoints; manually assigned to a subscription_plan.
CREATE TABLE IF NOT EXISTS webhooks (
    id                      TEXT PRIMARY KEY,               -- e.g., 'wh_01J...'
    plan_id                 TEXT NOT NULL,                  -- Foreign key to subscription_plans(id)
    url                     TEXT NOT NULL,                  -- Target HTTPS endpoint
    secret                  TEXT NOT NULL,                  -- HMAC-SHA256 signing secret
    description             TEXT,
    network_filter          TEXT CHECK (network_filter IN ('TRC20', 'BEP20', 'ALL')) DEFAULT 'ALL',  -- enforced in the §6.3 match query and at address-assignment time (§10.2)
    is_active               INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 ),
    updated_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 ),
    FOREIGN KEY (plan_id) REFERENCES subscription_plans(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_webhooks_plan_id ON webhooks(plan_id);

-- 3. WEBHOOK ADDRESSES (Hot-Path Clustered B-Tree Table)
-- WITHOUT ROWID stores the table directly as a clustered B-Tree ordered by (network, address, webhook_id).
-- Looking up (network, address) reads contiguous leaf pages with ZERO secondary heap lookups.
CREATE TABLE IF NOT EXISTS webhook_addresses (
    network                 TEXT NOT NULL CHECK (network IN ('TRC20', 'BEP20')),
    address                 TEXT NOT NULL,                  -- Normalized: lowercase hex for BEP20, exact Base58 for TRC20
    webhook_id              TEXT NOT NULL,
    plan_id                 TEXT NOT NULL,                  -- Denormalized from webhooks.plan_id for O(1) quota checks & fast joins
    created_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 ),
    PRIMARY KEY (network, address, webhook_id),
    FOREIGN KEY (webhook_id) REFERENCES webhooks(id) ON DELETE CASCADE,
    FOREIGN KEY (plan_id) REFERENCES subscription_plans(id) ON DELETE RESTRICT
) WITHOUT ROWID, STRICT;

-- Secondary index to support fast deletion/listing of addresses by webhook_id or plan_id
CREATE INDEX IF NOT EXISTS idx_webhook_addresses_webhook_id ON webhook_addresses(webhook_id);
CREATE INDEX IF NOT EXISTS idx_webhook_addresses_plan_id ON webhook_addresses(plan_id);

-- 4. BLOCK CHECKPOINTS (Crash Recovery State)
-- Updated atomically at the end of every full block scan.
CREATE TABLE IF NOT EXISTS block_checkpoints (
    network                 TEXT PRIMARY KEY CHECK (network IN ('TRC20', 'BEP20')),
    last_processed_block    INTEGER NOT NULL CHECK (last_processed_block >= 0),
    last_block_hash         TEXT,                           -- hash of block last_processed_block; used for parent-continuity check (§8.3)
    last_block_timestamp    INTEGER,
    -- Single-scanner lease (§8.3): prevents concurrent/overlapping scanners, which would double-count dropped events
    lease_owner             TEXT,
    lease_expires_at        INTEGER,
    updated_at              INTEGER NOT NULL DEFAULT ( unixepoch('subsec') * 1000 )
) WITHOUT ROWID, STRICT;

-- 5. DELIVERIES (Transactional Outbox Queue)
-- Stores matched events in a standard format for asynchronous webhook dispatch.
CREATE TABLE IF NOT EXISTS deliveries (
    id                      TEXT PRIMARY KEY,               -- UUIDv4 or ULID (e.g., 'del_01J...')
    plan_id                 TEXT NOT NULL,
    webhook_id              TEXT NOT NULL,
    network                 TEXT NOT NULL CHECK (network IN ('TRC20', 'BEP20')),
    block_number            INTEGER NOT NULL,
    tx_hash                 TEXT NOT NULL,
    log_index               INTEGER NOT NULL,               -- Event index within the transaction/block
    from_address            TEXT NOT NULL,
    to_address              TEXT NOT NULL,
    matched_address         TEXT NOT NULL,                  -- Which subscribed address triggered this match
    direction               TEXT NOT NULL CHECK (direction IN ('INCOMING', 'OUTGOING')),
    amount_raw              TEXT NOT NULL,                  -- Raw integer string in smallest token unit (sun/wei)
    amount_decimal          TEXT NOT NULL,                  -- Human-readable decimal string (e.g., "100.500000")
    token_contract          TEXT NOT NULL,                  -- USDT contract address on TRC20 or BEP20
    block_timestamp         INTEGER NOT NULL,               -- Block unix timestamp (ms)
    payload_json            TEXT NOT NULL,                  -- Standardized serialized JSON body to POST
    
    -- Delivery Worker State Machine
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
    
    -- Idempotency constraint: prevents duplicate webhook deliveries if a block is ever re-scanned
    UNIQUE (webhook_id, network, tx_hash, log_index, matched_address)
) STRICT;

-- Partial index optimized specifically for the Delivery Worker polling loop.
-- Only indexes active rows ('PENDING', 'FAILED'), keeping the index tiny even with millions of historical 'DELIVERED' rows.
CREATE INDEX IF NOT EXISTS idx_deliveries_worker_poll
ON deliveries (next_attempt_at ASC, id ASC)
WHERE status IN ('PENDING', 'FAILED');

CREATE INDEX IF NOT EXISTS idx_deliveries_webhook_status
ON deliveries (webhook_id, status);

CREATE INDEX IF NOT EXISTS idx_deliveries_block_number
ON deliveries (network, block_number);

-- Partial index for the stale-PROCESSING recovery sweep (§9.2 step 4).
-- Without it that sweep is an O(table) full scan every 60 s (measured: 30.8 ms at 200k rows).
CREATE INDEX IF NOT EXISTS idx_deliveries_processing_recovery
ON deliveries (updated_at)
WHERE status = 'PROCESSING';
```

## 6. High-Performance Address Lookup Architecture (Avoiding In-Memory Maps)

### 6.1 Why `WITHOUT ROWID` and Composite Clustering Maximize SQLite Speed

In a naive SQLite table, looking up a blockchain address via `WHERE network = ? AND address = ?` requires two B-Tree traversals: first searching the secondary index to find the 64-bit `rowid`, and second jumping to random disk pages in the main table heap to fetch `webhook_id` and `plan_id`.

By defining `webhook_addresses` as `WITHOUT ROWID` with `PRIMARY KEY (network, address, webhook_id)` and including `plan_id` directly in the leaf node:

- **Single B-Tree Traversal:** The primary key **is** the table storage. All addresses for `TRC20` and `BEP20` are physically sorted on disk in lexicographical order alongside their `webhook_id` and `plan_id`.
- **Zero Heap Lookups (100% Covering Read):** Once SQLite locates `(network, address)` in the B-Tree leaf page, `webhook_id` and `plan_id` are already in that exact CPU cache line.
- **Bounded Memory Footprint:** Even with **10,000,000 watched addresses** (~800 MB on disk), Node.js heap usage stays flat (~50 MB) while SQLite's 64 MB page cache (`PRAGMA cache_size = -64000`) and OS page cache hold the upper B-Tree interior nodes in RAM, requiring at most 1 page read per address lookup.

### 6.2 Address Normalization Rules

Before inserting into `webhook_addresses` or querying during block scanning, addresses must be deterministically normalized:

- **BEP20 (EVM / BSC):** Convert to lowercase 42-character hexadecimal string (`address.trim().toLowerCase()`). Do not use mixed-case EIP-55 checksum casing in the database index, as string equality comparisons with `=` in SQLite are case-sensitive by default and faster than `COLLATE NOCASE`.
- **TRC20 (TRON):** Normalize all addresses to standard 34-character Base58Check (`T...`) format using `tronWeb.address.fromHex(rawHexAddress)` when extracting topics from TRON smart contract logs (which emit 20-byte/21-byte `41...` hex addresses in raw `Transfer` event topics).

### 6.3 Query Strategy: Block-Level Batch Lookup vs. Per-Transaction Lookup

During block scanning, a single BSC or TRON block typically contains between 50 and 600 USDT `Transfer` events involving 80 to 1,000 unique sender/recipient addresses. While a prepared single-address lookup takes ~15 µs, executing 1,000 individual FFI calls per block adds unnecessary loop overhead.

Instead, the scanner collects all unique normalized `from` and `to` addresses from all USDT `Transfer` events in block $N$ into a deduplicated JavaScript `Set<string>`, serializes that small per-block set (e.g., 300 addresses, ~12 KB) as a JSON array, and executes **one single prepared SQLite query per block** using SQLite's built-in C-level `json_each()` table-valued function:

```sql
-- Executed once per block with named parameters (src/db/statements.ts). CROSS JOIN is MANDATORY:
-- it is the only reliable way to force json_each() to be the outer loop (see the note below).
SELECT
    wa.address            AS matched_address,
    wa.webhook_id         AS webhook_id,
    wa.plan_id            AS plan_id,
    w.url                 AS webhook_url,
    w.secret              AS webhook_secret
FROM json_each(:blockAddressesJson) AS batch
CROSS JOIN webhook_addresses AS wa
    ON wa.network = :network
   AND wa.address = batch.value
INNER JOIN webhooks AS w
    ON w.id = wa.webhook_id
   AND w.is_active = 1
   AND (w.network_filter = 'ALL' OR w.network_filter = :network)
INNER JOIN subscription_plans AS sp
    ON sp.id = wa.plan_id
   AND sp.is_active = 1;
```

**Why this query is optimal — and why `CROSS JOIN` is required:**

1. **Zero Full-Memory Load:** Only the unique addresses present in the *current block* (`:blockAddressesJson`, typically <500 strings) are passed into the query. The database table of millions of watched addresses stays on disk.
2. **The join order must be forced.** SQLite's planner does **not** automatically drive from the JSON list. Measured on SQLite 3.53.4 with 1,000,000 `webhook_addresses` rows, 50 plans, 1,000 webhooks, a 300-address block and 1,200 matches:

   | Formulation | Plan chosen by SQLite | Median latency |
   | :--- | :--- | ---: |
   | `INNER JOIN json_each` (naive) | `SCAN subscription_plans → SEARCH wa USING COVERING INDEX idx_webhook_addresses_plan_id → … → SCAN json_each` (json_each innermost, unindexed, re-scanned per row) | **15,991 ms** |
   | `CROSS JOIN json_each` (**required**) | `SCAN json_each → SEARCH wa USING PRIMARY KEY (network=? AND address=?) → SEARCH webhooks … → SEARCH subscription_plans …` | **2.21 ms** |
   | `WHERE address IN (SELECT value FROM json_each(:json))` | `SCAN subscription_plans → SEARCH wa USING COVERING INDEX idx_webhook_addresses_plan_id (plan_id=? AND network=? AND address=?)` | 22.6 ms |

   All three return identical rows. The naive form scans the address table with a per-row JSON probe — at 1M addresses it cannot meet any latency target. The `CROSS JOIN … ON` form is verified accepted by SQLite and is part of the required statement; §12.2 (test 5) asserts the plan in CI so a planner or stats change cannot silently reintroduce the slow form.
3. **Few-millisecond execution at 1M+ addresses:** Probing 300 block addresses against a 1,000,000-row `WITHOUT ROWID` table in WAL mode with the 64 MB page cache completes in **~2.2 ms** per block (median of 50 runs on the validation host); the fallback point-lookup statement below measures the same plan shape. Latency scales with the number of block addresses/matches, not with the size of `webhook_addresses`.

*(Fallback for Per-Transaction Streaming)*: If an API provider streams transactions individually rather than in block batches, use a single two-parameter statement checking both `from` and `to` in one call:

```sql
SELECT
    wa.address AS matched_address,
    wa.webhook_id,
    wa.plan_id,
    w.url AS webhook_url,
    w.secret AS webhook_secret
FROM webhook_addresses AS wa
INNER JOIN webhooks AS w ON w.id = wa.webhook_id AND w.is_active = 1
INNER JOIN subscription_plans AS sp ON sp.id = wa.plan_id AND sp.is_active = 1
WHERE wa.network = :network
  AND wa.address IN (:fromAddress, :toAddress)
  AND (w.network_filter = 'ALL' OR w.network_filter = :network);
```

This statement was measured to plan as `SEARCH wa USING PRIMARY KEY (network=? AND address=?)` — the clustered B-Tree is probed once per address with no table scan — and it honors the same `is_active` / `network_filter` semantics as the block-level query above.

## 7. Subscription Plan & Quota Enforcement Specification

### 7.1 Quota Rules and Edge Cases

Each subscription plan row in `subscription_plans` defines three hard limits (`max_webhooks`, `max_addresses`, `max_deliverable_events`):

| Quota Dimension                                           | Enforcement Point                                            | Counting Rule & Multi-Webhook Behavior                       | Action When Limit Reached                                    |
| :-------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| **Maximum Webhooks** (`max_webhooks`)                     | Synchronous check during `POST /api/watcher/webhooks` inside a write transaction. | Each row in `webhooks` referencing `plan_id` counts as `1` (`used_webhooks_count`). Deleting a webhook decrements `used_webhooks_count` by `1`. | Reject API request with `HTTP 403 Forbidden` (`error: "WEBHOOK_QUOTA_EXCEEDED"`). |
| **Maximum Addresses** (`max_addresses`)                   | Synchronous check during `POST /api/watcher/webhooks/:id/addresses` inside a write transaction. | Each `(webhook_id, network, address)` row in `webhook_addresses` counts as `1` against `used_addresses_count`. **If address `123` is assigned to `webhook1` and `webhook2` under the same plan, it creates 2 rows and counts as 2 against `max_addresses`.** Assigning the exact same address twice to the *same* webhook on the same network is idempotent (`INSERT OR IGNORE`, count unchanged). | Reject API request (or partial batch beyond remaining capacity) with `HTTP 403 Forbidden` (`error: "ADDRESS_QUOTA_EXCEEDED"`). |
| **Maximum Deliverable Events** (`max_deliverable_events`) | Synchronous check during block scanning inside the per-block atomic transaction before inserting into `deliveries`. | Every matched `(transfer_event, webhook_address)` pair counts as `1` event. If a single transaction transfers USDT from subscribed `addrA` (on `webhook1`) to subscribed `addrB` (on `webhook2`), that represents **2** matching events. The quota is consumed at enqueue time, so an event that is queued and later fails permanently still counts. This counter requires the exactly-once scan invariant of §8.3: `deliveries` rows are deduplicated by the `UNIQUE` key, but an already-counted **dropped** event cannot be recognized again, so an overlapping or replayed scan re-counts it as dropped. | Do **not** insert into `deliveries`. Atomically increment `subscription_plans.dropped_events_count` by `1` and log a throttled warning. |
| **Inactive plan or webhook** (`is_active = 0`)              | Filtered out of the §6.3 match query (`w.is_active = 1`, `sp.is_active = 1`). | An inactive plan or webhook produces **no** candidates at all: nothing is queued and nothing is counted as dropped — deactivation is a silent pause, not a quota event. Deliveries already queued are still dispatched (the worker does not re-check `is_active`); in-flight `PROCESSING` rows are recovered per §9.2. | Documented behavior; no counter changes. Re-activation resumes matching at the next scanned block — there is **no backfill** of blocks scanned while inactive. |

### 7.2 Atomic Quota Accounting in SQLite

To avoid expensive `SELECT COUNT(*)` queries during high-speed block scanning or API mutations, `subscription_plans` maintains materialized counters (`used_webhooks_count`, `used_addresses_count`, `delivered_events_count`, `dropped_events_count`).

#### Enforcing Webhook & Address Quotas via Atomic Conditional Updates

When adding $K$ new addresses to a webhook (`plan_id = :planId`), execute inside one write transaction (`Database.write`, §4.1):

```sql
-- Step 1: Insert addresses (ignoring exact duplicates on the same webhook + network)
INSERT OR IGNORE INTO webhook_addresses (network, address, webhook_id, plan_id)
VALUES (:network, :address, :webhookId, :planId);

-- Step 2: Let N be the actual number of newly inserted rows (sum of info.changes).
-- Attempt conditional quota increment on subscription_plans:
UPDATE subscription_plans
SET used_addresses_count = used_addresses_count + :newlyInsertedCount,
    updated_at = unixepoch('subsec') * 1000
WHERE id = :planId
  AND is_active = 1
  AND (used_addresses_count + :newlyInsertedCount) <= max_addresses;
```

If the `UPDATE` reports zero affected rows, the transaction throws `AddressQuotaExceededError`, which rolls back the `INSERT` statements.

Webhook creation uses the same conditional-`UPDATE` pattern: `UPDATE subscription_plans SET used_webhooks_count = used_webhooks_count + 1, updated_at = … WHERE id = :planId AND is_active = 1 AND (used_webhooks_count + 1) <= max_webhooks`; `changes === 0` throws `WebhookQuotaExceededError` (→ `403 WEBHOOK_QUOTA_EXCEEDED`) and rolls back the `webhooks` insert. On webhook deletion, count the webhook's address rows first, then `DELETE` the webhook (the `webhook_addresses` cascade removes them) and decrement `used_webhooks_count` / `used_addresses_count` by the counted values inside the same transaction.

#### Enforcing Deliverable Event Quota During Block Scanning

During block scanning, multiple events in block $N$ may match webhooks tied to `plan_id`. For each candidate delivery in the block transaction:

```sql
-- Prepared statement: tryConsumeEventQuotaStmt
UPDATE subscription_plans
SET delivered_events_count = delivered_events_count + 1,
    updated_at = unixepoch('subsec') * 1000
WHERE id = :planId
  AND is_active = 1
  AND delivered_events_count < max_deliverable_events;
```

- **If `tryConsumeEventQuotaStmt.run({ planId }).changes === 1`:** Quota is available and reserved. Immediately execute `insertDeliveryStmt.run(...)` to queue the webhook notification in `deliveries`. This is only correct in the exact order defined in §8.2 (existence check → consume → insert), which is the single canonical sequence for this document; §8.2 also gives the equivalent `INSERT … ON CONFLICT DO NOTHING` + `changes`-gated variant to use if the block commit is ever executed without the single-writer guarantee.
- **If `tryConsumeEventQuotaStmt.run({ planId }).changes === 0`:** The plan has reached `max_deliverable_events`. Execute `incrementDroppedEventsStmt.run({ planId })`:

```sql
-- Prepared statement: incrementDroppedEventsStmt
UPDATE subscription_plans
SET dropped_events_count = dropped_events_count + 1,
    updated_at = unixepoch('subsec') * 1000
WHERE id = :planId;
```

#### Counter Integrity (Derived State)

The four counters are **derived state** maintained atomically for speed; they are not the source of truth:

- **Reconciliation:** `POST /api/watcher/plans/:planId/reconcile` recomputes `used_webhooks_count` from `COUNT(*)` over `webhooks` and `used_addresses_count` from `COUNT(*)` over `webhook_addresses` inside one transaction, writes the corrected values, and returns both the stored and recomputed values. Run it after manual SQL, partial restores, or a suspected drift. Event counters (`delivered_events_count`, `dropped_events_count`) are monotonic by design and are only reset via `resetEventQuota` (§10.1).
- **Limits below usage:** `PATCH /api/watcher/plans/:planId` may set any `max_*` below the current `used_*` (to freeze an over-provisioned plan). The plan then rejects every new addition until usage drops below the limit; no existing webhook or address is removed implicitly.
- **Re-planning:** moving a webhook to another plan must update `webhooks.plan_id` *and* the denormalized `webhook_addresses.plan_id` rows in the same transaction, and must re-check the target plan's `max_webhooks` / `max_addresses` quotas first (§10.2).

## 8. Block Checkpointing and Crash Recovery Specification

### 8.1 Startup & Crash Recovery Lifecycle

The scanner acquires its network lease first and refuses to run if another instance holds it (§8.3), then performs the recovery procedure below.

**Confirmation depth is per network, not per webhook.** `networkConfirmations` defaults to `TRC20 = 20` and `BEP20 = 12` — the same values as today's `networkConfigs` — overridable via `TRC20_CONFIRMATIONS` / `BEP20_CONFIRMATIONS`. Rationale: one sequential cursor per network cannot honor differing per-webhook depths at insert time without deferred dispatch (a `deliverable_at_block` column plus a worker gate), which is out of scope. Every matched event is emitted at the network's depth, and reorgs deeper than that depth are out of scope (§8.3).

When `BlockScannerService` starts for a network (`TRC20` or `BEP20`):

1. **Query Checkpoint:** Execute `SELECT last_processed_block, last_block_hash FROM block_checkpoints WHERE network = :network`.
2. **Case A — Checkpoint Exists (`last_processed_block = B`):**
   - Log recovery event: `Resuming [network] scanner from checkpoint block ${B + 1}`.
   - Query the RPC provider for the current chain head (`latestBlock`) and compute `safeTargetBlock = latestBlock - networkConfirmations[network]`.
   - Iterate sequentially for `blockNum = B + 1` up to `safeTargetBlock`.
3. **Case B — No Checkpoint Exists (First Boot on Fresh Database):**
   - If environment variable `START_BLOCK_${NETWORK}` is configured, initialize `block_checkpoints` with `START_BLOCK_${NETWORK} - 1`.
   - Otherwise, fetch `latestBlock` from the RPC provider, compute `initialSafeBlock = latestBlock - networkConfirmations[network]`, insert `last_processed_block = initialSafeBlock` into `block_checkpoints`, and begin scanning from `initialSafeBlock + 1`.
4. **Catch-up semantics:** the loop always walks `B + 1 → safeTargetBlock` one block at a time, so downtime is fully backfilled; the cursor never jumps to the chain head.

### 8.2 Atomic Per-Block Commit Protocol

To guarantee that a crash mid-block never results in skipped transactions, double-counted quotas, or duplicate webhook deliveries, **all state changes for block $N$ execute inside a single write transaction** (`Database.write` on libsql, §4.1). The sketch below still shows the exact statement sequence; the implemented form awaits `tx.query` (existence check) and `tx.run` (quota consume/drop/increment, delivery insert, checkpoint upsert) of the corresponding statements:

```typescript
const commitBlockTransaction = db.transaction((
  network: 'TRC20' | 'BEP20',
  blockNumber: number,
  blockHash: string,
  blockTimestamp: number,
  candidateMatches: CandidateMatch[]
) => {
  for (const match of candidateMatches) {
    // 1. Check if this exact delivery already exists (idempotency guard)
    const existing = checkDeliveryExistsStmt.get({
      webhookId: match.webhookId,
      network,
      txHash: match.txHash,
      logIndex: match.logIndex,
      matchedAddress: match.matchedAddress
    });
    if (existing) {
      continue;
    }

    // 2. Attempt to consume 1 deliverable event from the plan's quota
    const quotaResult = tryConsumeEventQuotaStmt.run({ planId: match.planId });

    if (quotaResult.changes === 1) {
      // Quota available -> Insert into deliveries table
      insertDeliveryStmt.run({
        id: match.deliveryId,
        planId: match.planId,
        webhookId: match.webhookId,
        network,
        blockNumber,
        txHash: match.txHash,
        logIndex: match.logIndex,
        fromAddress: match.fromAddress,
        toAddress: match.toAddress,
        matchedAddress: match.matchedAddress,
        direction: match.direction,
        amountRaw: match.amountRaw,
        amountDecimal: match.amountDecimal,
        tokenContract: match.tokenContract,
        blockTimestamp,
        payloadJson: JSON.stringify(match.payload)
      });
    } else {
      // Quota exhausted -> Ignore event and increment dropped counter
      incrementDroppedEventsStmt.run({ planId: match.planId });
    }
  }

  // 3. Advance the block checkpoint last inside the same atomic transaction
  upsertCheckpointStmt.run({
    network,
    lastProcessedBlock: blockNumber,
    lastBlockHash: blockHash,
    lastBlockTimestamp: blockTimestamp
  });
});
```

**Crash Consistency Proof (process crashes):**

- If the Node.js process crashes (OOM, `SIGKILL`, `SIGABRT`, uncaught RPC exception) **before** `commitBlockTransaction` finishes, SQLite rolls back the uncommitted WAL frames on next open. `block_checkpoints.last_processed_block` remains at $N - 1$, `delivered_events_count` and `dropped_events_count` remain unchanged, and zero partial rows exist in `deliveries`.
- If the process crashes **after** `commitBlockTransaction` commits, `last_processed_block` is $N$, all deliveries for block $N$ are persisted in `deliveries` with `status = 'PENDING'`, and the `DeliveryWorker` picks them up on restart while the scanner begins at block $N + 1$.
- **Durability boundary:** with `PRAGMA synchronous = NORMAL` (WAL), an *operating-system* crash or power loss can lose the most recently committed transactions. That is safe here — the checkpoint, quota counters, and delivery rows of a lost block roll back together and the block is re-scanned (at-least-once, §9.2) — but the guarantee is "no committed state is corrupted and no block is skipped", **not** "every commit survives power loss". Use `PRAGMA synchronous = FULL` if the stricter guarantee is required (at a throughput cost).

### 8.3 Scanner Lease, Exactly-Once Scan Invariant, and Reorg Guard

**Lease (single scanner per network).** Quota accounting assumes each block is scanned exactly once, because `deliveries` insertion is deduplicated by its `UNIQUE` key but **dropped-event counting is not** (§7.1). The lease lives in `block_checkpoints`:

```sql
-- Acquire (succeeds only when no live owner holds it)
UPDATE block_checkpoints
SET lease_owner = :instanceId, lease_expires_at = :now + 30000, updated_at = :now
WHERE network = :network
  AND (lease_owner IS NULL OR lease_owner = :instanceId OR lease_expires_at < :now);
-- changes === 0 → another scanner is live → log CRITICAL and exit non-zero
```

Renew every 10 s: `UPDATE block_checkpoints SET lease_expires_at = :now + 30000 WHERE network = :network AND lease_owner = :instanceId`; if `changes === 0` the lease was taken over and the scanner must stop immediately. Release on graceful shutdown (`SET lease_owner = NULL, lease_expires_at = NULL`). A crashed scanner is replaced automatically once the 30 s TTL expires.

**Parent-continuity check (reorg guard).** Before committing block $N$, compare `getBlock(N).parentHash` (returned alongside the block's own hash and timestamp, §11) with `block_checkpoints.last_block_hash`. On mismatch the chain reorganized deeper than the previous block: stop this network's scanner, log CRITICAL with both hashes, and leave the checkpoint untouched. Never continue on a fork: a partially correct rewind would corrupt `dropped_events_count`, and deliveries already sent cannot be recalled. Operator options: let the provider settle (TRON solid blocks; BSC ≥ 12 blocks) and restart, or manually rewind `last_processed_block` / `last_block_hash` to a known common ancestor and reconcile counters before resuming.

**Invariants and explicit non-goals:**

- Reorgs deeper than `networkConfirmations` are out of scope; confirmation depth plus the parent check is the whole reorg defense.
- Adding an address or re-activating a webhook never backfills historical blocks — matching starts from the next scanned block (§7.1).
- Never replay an already-committed block range (e.g. by manually rewinding the checkpoint) without reconciling `dropped_events_count` first; §8.1's sequential catch-up from the committed checkpoint is the only replay path.

## 9. Standardized Delivery Table Payload & Asynchronous Worker Specification

### 9.1 Standardized Event Payload Format (`payload_json`)

Every row inserted into `deliveries` stores a canonical JSON payload in `payload_json`, so the `DeliveryWorker` never re-parses blockchain data — it only needs the target `url` and `secret` from `webhooks` (joined at claim time) to sign and POST the stored payload:

```json
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

### 9.2 Delivery Worker Polling, Concurrency, and Retry State Machine

The `DeliveryWorkerService` runs an asynchronous loop (configurable poll interval, default `500ms`, with configurable HTTP concurrency, e.g., `CONCURRENCY = 20` using `Promise.allSettled`):

1. **Atomic Batch Claim (`claimDueDeliveries`):**
   Using `idx_deliveries_worker_poll`, the worker selects and locks up to `batchSize` (e.g., 50) due deliveries in a single synchronous SQLite transaction:

   ```sql
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
   RETURNING id, webhook_id, payload_json, attempt_count, max_attempts;
   ```

   Execute this with `tx.query(...)` and read `rows.length` — libsql reports `rowsAffected === 0` for `RETURNING` statements even when rows are updated (§4.1), so the returned rows, not the change count, are the result. Measured on a 200,000-row `deliveries` table: the claim uses `idx_deliveries_worker_poll (next_attempt_at<?)` and returns 50 rows in **0.79 ms**. Because the claim is a single atomic `UPDATE … RETURNING`, several worker processes may run concurrently; only the scanner is single-instance (§8.3).

2. **Webhook Dispatch & HMAC Signing:**
   For each claimed delivery, the worker looks up the target `url` and `secret` from `webhooks` (or joins it during claim), computes `X-Webhook-Signature = HMAC_SHA256(secret, payload_json)` via `src/utils/generateSignature.ts`, and sends an HTTP `POST` request with a `5000ms` timeout:

   - Headers:
     - `Content-Type: application/json`
     - `X-Webhook-Signature: <hmac-sha256-hex>`
     - `X-Webhook-Id: <webhook_id>`
     - `X-Delivery-Id: <delivery_id>`
     - `X-Delivery-Attempt: <attempt_count>`

3. **Outcome Handling & Exponential Backoff:**

   - **Success (`HTTP 200–299`):**

     ```sql
     UPDATE deliveries
     SET status = 'DELIVERED',
         last_http_status = :httpStatus,
         delivered_at = :now,
         updated_at = :now
     WHERE id = :id;
     ```

   - **Transient Failure (`HTTP 408`, `429`, `500–599`, or Network/Timeout Error) where `attempt_count < max_attempts`:**
     Compute exponential backoff with full jitter:
     $$\text{delayMs} = \min\left(3600000,\; 2000 \times 2^{\text{attempt\_count}}\right) + \text{random}(0, 1000)$$

     ```sql
     UPDATE deliveries
     SET status = 'FAILED',
         next_attempt_at = :now + :delayMs,
         last_http_status = :httpStatus,
         last_error = :errorMessage,
         updated_at = :now
     WHERE id = :id;
     ```

   - **Permanent Exhaustion (`attempt_count >= max_attempts` or non-retryable `HTTP 400/401/404/410`):**

     ```sql
     UPDATE deliveries
     SET status = 'DEAD_LETTER',
         last_http_status = :httpStatus,
         last_error = :errorMessage,
         updated_at = :now
     WHERE id = :id;
     ```

4. **Stale `PROCESSING` Recovery (Worker Crash Recovery):**
   On startup and every 60 seconds, the worker resets any row stuck in `status = 'PROCESSING'` where `updated_at < now - 60000` back to `status = 'FAILED'` so deliveries in flight during a process crash are automatically retried. This sweep uses the partial index `idx_deliveries_processing_recovery` (§5.2); without it the sweep is an O(table) full scan every 60 s (measured: 30.8 ms at 200k rows).

5. **Delivery semantics — at-least-once.** A delivery can be sent twice when the worker crashes after the endpoint returned `2xx` but before the `DELIVERED` update commits (step 4 then retries it). Consumers must deduplicate on `X-Delivery-Id` (stable per `deliveries.id`; the payload's `eventId` is the same value). Exactly-once end-to-end is explicitly not promised.

### 9.3 Retention, Pruning, and Maintenance

`deliveries` is both the queue and the audit log, so it grows forever unless pruned. Specify:

- **Prune job:** daily, in bounded batches (e.g. 5,000 rows per statement, looped) run `DELETE FROM deliveries WHERE status IN ('DELIVERED', 'DEAD_LETTER') AND updated_at < :now - :retentionMs` with `DELIVERY_RETENTION_DAYS` default 30. Never prune `PENDING` / `FAILED` / `PROCESSING` rows — they are live work.
- **Why:** the non-partial indexes (`idx_deliveries_webhook_status`, `idx_deliveries_block_number`) grow with the table and `GET /api/watcher/deliveries` pagination reads them; pruning also keeps WAL checkpoints and backup size bounded.
- **Maintenance cadence:** run `PRAGMA optimize;` after pruning and after schema or bulk changes; `PRAGMA wal_autocheckpoint = 4000` (§4.2) bounds WAL growth during normal operation. Take backups with the SQLite online backup API (`db.backup()`) instead of copying the file, and validate with `PRAGMA integrity_check`.
- **Non-blocking:** pruning must use short transactions so it never blocks the scanner's `BEGIN IMMEDIATE` commits for long (`busy_timeout` applies, §4.2).

## 10. REST API Specification (`src/routes/watcherRoutes.ts` Refactor)

All endpoints remain protected by `authenticateApiKey` (`src/middleware/auth.ts`, `X-API-Key`) and validated with `zod`; `GET /health` stays unauthenticated. A single shared API key means there is no per-tenant authorization — any caller can manage any plan or webhook, which matches the "no user signup, manual plan assignment" model. If per-tenant isolation is ever required, add an owner column and scope every query by it (out of scope here).

### 10.1 Subscription Plan Management Endpoints

| Method & Path                      | Description                                                  | Request Body (Zod Schema)                                    | Response / Behavior                                          |
| :--------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| `POST /api/watcher/plans`          | Create a subscription plan (or manual plan assignment record). | `{ id?: string, name: string, maxWebhooks: number, maxAddresses: number, maxDeliverableEvents: number }` | `201 Created`: Returns created plan object with current counters (`0`). |
| `GET /api/watcher/plans/:planId`   | Retrieve plan quota limits, used counts, and dropped event counts. | None                                                         | `200 OK`: Returns `{ id, name, maxWebhooks, usedWebhooksCount, maxAddresses, usedAddressesCount, maxDeliverableEvents, deliveredEventsCount, droppedEventsCount, isActive }`. |
| `PATCH /api/watcher/plans/:planId` | Update plan limits, activation, or reset the event counters. | `{ maxWebhooks?: number, maxAddresses?: number, maxDeliverableEvents?: number, resetEventQuota?: boolean, isActive?: boolean }` | `200 OK`: updates `subscription_plans`. Limits may be lowered below current usage — the plan then rejects all new additions until usage drops below the limit (§7.2), and existing webhooks/addresses are never removed implicitly. `resetEventQuota: true` zeroes `deliveredEventsCount` / `droppedEventsCount` in the same transaction and logs the previous values. |
| `POST /api/watcher/plans/:planId/reconcile` | Recompute derived counters from the source tables. | None                                                         | `200 OK`: recomputes `usedWebhooksCount` from `COUNT(*)` over `webhooks` and `usedAddressesCount` from `COUNT(*)` over `webhook_addresses`, writes them, and returns `{ stored, recomputed }` (§7.2). |

### 10.2 Webhook & Address Registration Endpoints

| Method & Path                                       | Description                                                  | Request Body (Zod Schema)                                    | Quota & Validation Rules                                     |
| :-------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| `POST /api/watcher/webhooks`                        | Create a new webhook and manually assign a `planId`. Optionally attach an initial array of addresses. | `{ planId: string, url: string, secret?: string, networkFilter?: 'TRC20' \| 'BEP20' \| 'ALL', addresses?: Array<{ network: 'TRC20' \| 'BEP20', address: string }> }` | Verifies `used_webhooks_count + 1 <= max_webhooks` AND `used_addresses_count + addresses.length <= max_addresses` atomically via conditional `UPDATE`s (§7.2). Returns `403` if either limit is exceeded. Any address whose `network` contradicts the webhook's `networkFilter` (when not `ALL`) is rejected with `400 ADDRESS_NETWORK_MISMATCH`. |
| `POST /api/watcher/webhooks/:webhookId/addresses`   | Bulk-assign watched addresses to an existing webhook.        | `{ addresses: Array<{ network: 'TRC20' \| 'BEP20', address: string }> }` (up to 1,000 per request) | Normalizes addresses (§6.2); rejects any address whose `network` contradicts the webhook's `networkFilter` unless it is `ALL` (`400 ADDRESS_NETWORK_MISMATCH`); checks `used_addresses_count + newCount <= max_addresses` (§7.2). If `address 123` is already on `webhook1` and is now added to `webhook2`, it increments `used_addresses_count` again. Assigning an address never backfills already-scanned blocks (§7.1). |
| `DELETE /api/watcher/webhooks/:webhookId/addresses` | Remove specific addresses from a webhook and free address quota. | `{ addresses: Array<{ network: 'TRC20' \| 'BEP20', address: string }> }` | Deletes matching rows from `webhook_addresses` and decrements `used_addresses_count` by the exact number of deleted rows. |
| `DELETE /api/watcher/webhooks/:webhookId`           | Delete a webhook and all of its associated addresses.        | None                                                         | Counts the webhook's address rows first, cascades deletion to `webhook_addresses`, and decrements `used_webhooks_count` by `1` and `used_addresses_count` by the counted rows in the same transaction (§7.2). |
| `PATCH /api/watcher/webhooks/:webhookId`            | Update URL, secret, `networkFilter`, `isActive`, or move the webhook to another plan. | `{ url?: string, secret?: string, networkFilter?: 'TRC20' \| 'BEP20' \| 'ALL', isActive?: boolean, planId?: string }` | One transaction. Narrowing `networkFilter` while conflicting addresses exist is rejected with `409 NETWORK_FILTER_CONFLICT` listing them (default behavior; the client must remove the addresses first). Moving to another plan re-checks the target plan's `max_webhooks` / `max_addresses` and updates `webhooks.plan_id` plus the denormalized `webhook_addresses.plan_id` together (§7.2). Setting `isActive: false` pauses matching without affecting quotas (§7.1). |
| `GET /api/watcher/webhooks/:webhookId`              | Inspect one webhook: config, plan assignment, and address counts. | Query params: `?includeAddresses=true&limit=200&cursor=...` | `200 OK`: webhook row, `plan_id`, `is_active`, `network_filter`, and address counts (plus a paginated address list when requested via `idx_webhook_addresses_webhook_id`). |
| `GET /api/watcher/deliveries`                       | Inspect delivery queue status and filter by `webhookId`, `planId`, or `status`. | Query params: `?webhookId=...&status=PENDING\|DELIVERED\|FAILED\|DEAD_LETTER&limit=50` | Returns paginated delivery logs and retry errors.            |
| `GET /api/watcher/stats`                            | Enhanced system health, checkpoint heights, queue depth, and scanner-lease state. | None                                                         | Returns `block_checkpoints` per network (including `lease_owner` / `lease_expires_at`), pending delivery count, retention horizon, and aggregate plan metrics. |

### 10.3 Route Migration (Legacy → Target)

| Legacy route (`72c5fe0`)                                     | Target                                                       | Notes                                                        |
| :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| `POST /watch`                                                | `POST /api/watcher/webhooks` (+ `POST …/:webhookId/addresses`) | The per-transaction watch contract has no direct equivalent; callers create a webhook and attach addresses (§2.1). |
| `DELETE /watch/:id`                                          | `DELETE /api/watcher/webhooks/:webhookId`                    | —                                                            |
| `GET /watch/:id`                                             | `GET /api/watcher/webhooks/:webhookId`                       | Returns webhook config + address counts instead of a transaction status. |
| `GET /networks`, `GET /networks/:network`                    | folded into `GET /api/watcher/stats`                          | Checkpoint heights, lease state, and queue metrics.          |
| `PATCH /admin/networks/:network`, `POST /admin/networks/:network/enable`, `POST /admin/networks/:network/disable` | removed                                                      | Network enablement moves to configuration/env (§2.2).        |
| `GET /stats`                                                 | `GET /api/watcher/stats`                                     | Same role, new metrics.                                      |
| `GET /health`                                                | `GET /api/watcher/health`                                    | Unauthenticated; add SQLite liveness (`SELECT 1`) and scanner-lease state. |

## 11. Proposed Repository File Structure & Migration Plan

To cleanly refactor [moha-abdi/tether-watcher](https://github.com/moha-abdi/tether-watcher), update `package.json` with the runtime dependency `@libsql/client@^0.18` (dev: `typescript`, `vitest`, `@types/node`) and set `"engines": { "node": ">=22" }`; identifiers use Node's native `crypto.randomUUID()`. Keep the existing stack (`express`, `tronweb`, `ethers` v6, `axios`, `zod`, `winston`). Reorganize `src/` as follows:

```text
src/
├── config.ts                         # zod-validated environment configuration (defaults for both networks)
├── errors.ts                         # AppError taxonomy (quota, mismatch, lease, reorg) + errorMessage()
├── types.ts                          # Domain types: Network, Webhook, SubscriptionPlan, DeliveryRecord, EventPayload
├── index.ts                          # Express app init, libSQL init, starts BlockScanner & DeliveryWorker
├── db/
│   ├── database.ts                   # libSQL client, PRAGMA setup (§4.2), serialized write queue + Tx facade (§4.1), migration runner (PRAGMA user_version)
│   ├── migrations.ts                 # 001: DDL for subscription_plans, webhooks, webhook_addresses, block_checkpoints, deliveries (+ index list)
│   └── statements.ts                 # Hot-path SQL constants (incl. the forced-CROSS-JOIN lookup, §6.3)
├── middleware/
│   ├── auth.ts                       # Existing API key authentication (X-API-Key)
│   └── errorHandler.ts               # Extended to map QuotaExceededError -> HTTP 403
├── repositories/
│   ├── PlanRepository.ts             # CRUD & atomic quota updates for subscription_plans; counter reconciliation (§7.2)
│   ├── WebhookRepository.ts          # CRUD for webhooks & batch address management in webhook_addresses (quota-checked)
│   ├── CheckpointRepository.ts       # Read/write block_checkpoints + scanner lease acquire/renew/release (§8.3)
│   ├── DeliveryRepository.ts         # Outbox insertion, atomic worker batch claiming, status transitions, retention pruning (§9.3)
│   └── MatchRepository.ts            # findBlockMatches(tx, network, addresses): the §6.3 CROSS-JOIN batch lookup
├── services/
│   ├── BlockScannerService.ts        # Per-network sequential scanner: lease (§8.3), forced-CROSS-JOIN batch lookup (§6.3), atomic per-block commit (§8.2)
│   ├── DeliveryWorkerService.ts      # Claims deliveries atomically (§9.2), signs payloads, Axios POSTs with backoff, stale-PROCESSING recovery
│   └── providers/
│       ├── TronBlockProvider.ts      # Per-height TRC20 fetch (block hash/parent/timestamp + Transfer events for exactly that height, fingerprint-paged); normalizes 41-hex → Base58
│       └── BscBlockProvider.ts       # Per-height BEP20 fetch: getBlock(N) + getLogs({address: USDT, topics: [Transfer], fromBlock: N, toBlock: N}); retries per provider caps
├── routes/
│   └── watcherRoutes.ts              # Express routes for /plans, /webhooks, /deliveries, /stats, /health
└── utils/
    ├── addressNormalizer.ts          # Lowercase EVM hex and Tron Base58 normalization + validation
    ├── amount.ts                     # BigInt-exact raw→decimal formatting (formatUnits)
    ├── async.ts                      # sleep() + retry() with backoff (used by the scanner and provider calls)
    ├── generateSignature.ts          # HMAC-SHA256 generator over the per-webhook secret
    ├── id.ts                         # Prefixed UUIDv4 identifiers (plan_, wh_, del_)
    ├── logger.ts                     # Winston logger factory + Express request logging
    └── webhook.ts                    # Axios HTTP dispatcher returning { success, status, error }
```

Test layout (`npm test`):

```text
tests/
├── helpers.ts                        # temp-database harness, fake block provider, scripted HTTP receiver
├── database.test.ts                  # schema/PRAGMAs, query-plan assertion, quota, lease, counter reconciliation
└── pipeline.test.ts                  # scanner → outbox → worker → receiver: signature, retry, reorg, filters, pruning
```

**Provider fetch contract (required for the "0 missed blocks" target):**

- **TRC20 (`TronBlockProvider`):** fetch exactly one height per request — TronGrid's contract-events endpoint filtered to `min_block_number = max_block_number = N`, `only_confirmed = false` (finality is enforced by `networkConfirmations`, §8.1), `limit = 200`, paging on the returned `fingerprint` until exhausted. Assert `block_number === N` on every returned event; reject and retry the block otherwise, never partially accept it. Fetch the block itself for `hash`, `parentHash`, and `timestamp` (needed by the §8.3 continuity check). TRON solidity depth (~19 blocks) is the reference for `TRC20_CONFIRMATIONS = 20`.
- **BEP20 (`BscBlockProvider`):** `getBlock(N)` plus `getLogs({ address: USDT, topics: [Transfer], fromBlock: N, toBlock: N })`. One-block ranges avoid the common `eth_getLogs` block/result caps of public endpoints; if a provider still rejects the range (result cap or rate limit), back off exponentially and retry the same block — never skip it and never advance the checkpoint on a partial result.
- **Resume depth:** catch-up (§8.1) can span thousands of blocks, so the configured endpoints must be archive-capable for the chosen `START_BLOCK_*`; otherwise the initial deployment must start near the safe head.
- **Rate limits:** one in-flight block per network, processed sequentially; on `429`/timeouts, back off and retry the same block. Quota/checkpoint writes happen only inside the per-block transaction (§8.2).

## 12. Performance Benchmarks, Testing, and Acceptance Criteria

### 12.1 Performance & Scalability Targets

| Metric                                                       | Target Requirement                                           | Architectural Mechanism                                      |
| :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| **Node.js Memory Usage with 1,000,000 Watched Addresses**    | `< 150 MB` RSS excluding reclaimable memory-mapped DB pages (constant $O(1)$ relative to address-table size) | Zero in-memory address `Map`; addresses stay in SQLite `WITHOUT ROWID` B-Tree with a 64 MB page cache. `mmap_size` pages count toward RSS while resident — cap `mmap_size` at 64 MB (§4.2) if a hard RSS bound is required. |
| **Address Lookup Latency per Block (≤500 unique block addresses)** | `< 3.0 ms` per block at 1,000,000 stored addresses (validation measured **2.21 ms** median: 1M address rows, 300-address block, 1,200 matches) | Single `json_each()` batch query with forced `CROSS JOIN` order probing `PRIMARY KEY (network, address, webhook_id)` (§6.3). The naive `INNER JOIN` form measures 15,991 ms on the same data, so the plan shape is asserted by test 5. |
| **Block Commit Latency (Checkpoint + 50 Matched Deliveries)** | `< 3.0 ms` per block on local NVMe/SSD                       | Single `BEGIN IMMEDIATE ... COMMIT` transaction under `PRAGMA journal_mode = WAL` and `synchronous = NORMAL`. |
| **Crash Recovery Accuracy (process crashes)**                | `0` skipped blocks, `0` duplicate `deliveries` rows (delivery to the endpoint is at-least-once, §9.2) | Atomic per-block transaction; `UNIQUE(webhook_id, network, tx_hash, log_index, matched_address)`; scanner lease prevents overlapping scans (§8.3). Power-loss durability is bounded by `synchronous = NORMAL` (§8.2). |

### 12.2 Verification & Acceptance Test Suite

1. **Duplicate Address Across Webhooks Quota Test:**
   - Create a plan with `max_webhooks = 50, max_addresses = 2, max_deliverable_events = 50`.
   - Create `webhook1` and `webhook2` under that plan.
   - Assign address `0xabc...123` to `webhook1` (`used_addresses_count` becomes `1`).
   - Assign the same address `0xabc...123` to `webhook2` (`used_addresses_count` becomes `2`).
   - Attempt to assign address `0xdef...456` to `webhook1` $\rightarrow$ Verify API rejects with `403 ADDRESS_QUOTA_EXCEEDED`.
2. **Event Quota Exhaustion & Drop Counting Test:**
   - Create a plan with `max_deliverable_events = 2`.
   - Simulate a block containing 5 USDT transfers to a watched address under that plan.
   - Verify that exactly **2** rows are inserted into `deliveries`, `delivered_events_count` equals `2`, and `dropped_events_count` equals `3`.
3. **Mid-Block Crash Recovery Test:**
   - Process block `100` (checkpoint `last_processed_block = 100`).
   - Simulate an exception during block `101` before transaction commit.
   - Restart `BlockScannerService` $\rightarrow$ Verify scanner resumes at block `101` (not `102` and not `latestBlock`) and that partial quota increments from the aborted block `101` were rolled back.
4. **Delivery Worker Outbox & Retry Test:**
   - Configure a mock webhook endpoint that returns `HTTP 502` twice and `HTTP 200` on the third attempt.
   - Verify the row in `deliveries` transitions `PENDING` $\rightarrow$ `PROCESSING` $\rightarrow$ `FAILED` (attempt 1) $\rightarrow$ `PROCESSING` $\rightarrow$ `FAILED` (attempt 2) $\rightarrow$ `PROCESSING` $\rightarrow$ `DELIVERED` (attempt 3) with valid `X-Webhook-Signature`.
5. **Query-Plan Regression Test (blocking):**
   - Seed ≥ 100,000 `webhook_addresses` rows (multiple plans/webhooks) and run `EXPLAIN QUERY PLAN` for the §6.3 batch statement with a representative block-address JSON array.
   - Assert the plan contains `SEARCH wa USING PRIMARY KEY (network=? AND address=?)` and does **not** reference `idx_webhook_addresses_plan_id` for `wa`; also assert the §6.3 fallback statement and the §9.2 claim statement use `PRIMARY KEY` and `idx_deliveries_worker_poll` respectively. Fail CI otherwise — the naive `INNER JOIN` form plans to 15,991 ms on the validation dataset.
6. **Scanner Lease Test:**
   - Start scanner A for `BEP20` and verify it acquires the lease; start scanner B for the same network and verify it fails fast while A heartbeats.
   - Verify B takes over only after A stops and the TTL expires, and that `lease_owner` / `lease_expires_at` are cleared on graceful shutdown (and left to expire after a `SIGKILL`).
7. **Network-Filter Enforcement Test:**
   - Create `webhook1` with `networkFilter = 'TRC20'`; verify `POST …/webhook1/addresses` rejects a BEP20 address with `400 ADDRESS_NETWORK_MISMATCH` and that no BEP20 transfer ever produces a candidate for `webhook1` in the §6.3 batch query.
   - Set the filter to `ALL`, add the BEP20 address, and verify BEP20 matches now appear.