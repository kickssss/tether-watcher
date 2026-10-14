# Tether-Watcher

Address-subscription watcher for USDT on TRON (TRC20) and BNB Smart Chain (BEP20).
Subscribed addresses are matched against every confirmed block; matches are queued in a
local SQLite outbox and delivered to webhook endpoints asynchronously.

## Architecture

```text
BlockScannerService ─── per network, one sequential loop, holds a lease
        │  1 block  =  1 write transaction
        ▼
   SQLite (WAL)  ── webhook_addresses (clustered B-Tree) · deliveries (outbox) · quotas
        ▲
        │  atomic claim / retry / backoff
DeliveryWorkerService ─── at-least-once POSTs with HMAC-SHA256 signatures
```

- **Scanner** – walks `checkpoint + 1 → head - confirmations` one block at a time, performs one
  `json_each` batch lookup per block against millions of addresses (no in-memory maps), and commits
  matched events, quota counters, and the new checkpoint atomically.
- **Worker** – claims due deliveries with a single `UPDATE … RETURNING`, signs each payload with the
  webhook's own secret, retries transient failures with exponential backoff, and dead-letters
  permanent ones.
- **Quotas** – every webhook belongs to a subscription plan (`max_webhooks`, `max_addresses`,
  `max_deliverable_events`). Counters are materialized and maintained atomically; events beyond the
  plan's event quota are ignored and counted as dropped.

Full design: [`PRD.md`](./PRD.md).

## Requirements

- Node.js **≥ 22**
- A TronGrid API key (recommended) and a BSC JSON-RPC endpoint

## Setup

```bash
npm install
npm run build
cp .env.example .env   # then edit it
npm start
```

Development (`ts-node-dev`, no build step): `npm run dev`

### Configuration

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `PORT` | `3000` | HTTP port |
| `API_KEY` | *(required)* | Shared key for `X-API-Key` |
| `DATABASE_URL` | `file:./data/tether-watcher.db` | libSQL/SQLite URL |
| `NETWORK_MODE` | `mainnet` | `mainnet` or `testnet` (echoed in payloads) |
| `LOG_LEVEL` | `info` | `error` \| `warn` \| `info` \| `debug` |
| `TRC20_ENABLED` / `BEP20_ENABLED` | `true` | Disable a network's scanner |
| `TRC20_CONFIRMATIONS` / `BEP20_CONFIRMATIONS` | `20` / `12` | Depth before a block is scanned |
| `TRC20_TOKEN_CONTRACT` | USDT TRC20 | Token contract to index |
| `BEP20_TOKEN_CONTRACT` | USDT BEP20 | Token contract to index |
| `TRC20_DECIMALS` / `BEP20_DECIMALS` | `6` / `18` | Decimals used for `amountDecimal` |
| `TRC20_START_BLOCK` / `BEP20_START_BLOCK` | *(unset)* | First height on a fresh database; otherwise start at the safe head |
| `TRONGRID_URL` / `TRON_API_KEY` | `https://api.trongrid.io` | TronGrid endpoint and key |
| `BSC_RPC_URL` | `https://bsc-dataseed.bnbchain.org` | BSC JSON-RPC endpoint (must be archive-capable for deep catch-up) |
| `SCANNER_IDLE_INTERVAL_MS` | `3000` | Sleep when the scanner is caught up |
| `WORKER_POLL_INTERVAL_MS` | `500` | Outbox poll interval |
| `WORKER_BATCH_SIZE` | `50` | Deliveries claimed per poll |
| `WORKER_CONCURRENCY` | `20` | Parallel HTTP dispatches |
| `WORKER_HTTP_TIMEOUT_MS` | `5000` | Per-request timeout |
| `WORKER_BACKOFF_BASE_MS` | `2000` | First retry delay (`base · 2^attempt`, capped at 1 h) |
| `DELIVERY_RETENTION_DAYS` | `30` | Retention for `DELIVERED` / `DEAD_LETTER` rows |
| `PRUNE_INTERVAL_MS` | `86400000` | Retention sweep cadence |

## API

All routes live under `/api/watcher` and require `X-API-Key`, except `GET /health`.

| Method & Path | Purpose |
| :--- | :--- |
| `GET /health` | Public liveness: SQLite probe + per-network checkpoint/lease state |
| `POST /plans` | Create a subscription plan (quota template) |
| `GET /plans/:planId` | Plan limits and current counters |
| `PATCH /plans/:planId` | Update limits, `isActive`, or `resetEventQuota` |
| `POST /plans/:planId/reconcile` | Recompute derived counters from the source tables |
| `POST /webhooks` | Create a webhook for a plan, optionally with initial addresses |
| `GET /webhooks/:webhookId` | Webhook config, address count, `?includeAddresses=true` for the list |
| `PATCH /webhooks/:webhookId` | Update URL/secret/filter/`isActive`, or move it to another plan |
| `DELETE /webhooks/:webhookId` | Delete the webhook with its addresses and queued deliveries |
| `POST /webhooks/:webhookId/addresses` | Bulk-add addresses (up to 1000) |
| `DELETE /webhooks/:webhookId/addresses` | Remove addresses and release quota |
| `GET /deliveries` | Inspect the outbox (`?webhookId=&planId=&status=&limit=&offset=`) |
| `GET /stats` | Scanner state, plan aggregates, queue depth, retention |

Quota failures answer `403` with `WEBHOOK_QUOTA_EXCEEDED` / `ADDRESS_QUOTA_EXCEEDED`;
an address that contradicts the webhook's `networkFilter` answers
`400 ADDRESS_NETWORK_MISMATCH`; narrowing a filter while conflicting addresses exist answers
`409 NETWORK_FILTER_CONFLICT`.

### Example

```bash
curl -s -X POST http://localhost:3000/api/watcher/plans -H "X-API-Key: $API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"starter","maxWebhooks":2,"maxAddresses":50,"maxDeliverableEvents":10000}'

curl -s -X POST http://localhost:3000/api/watcher/webhooks -H "X-API-Key: $API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"planId":"plan_...","url":"https://example.com/hook","networkFilter":"TRC20",
       "addresses":[{"network":"TRC20","address":"TXYZopYRdj2D9XRtbG411XZZ3kM5VkAeBf"}]}'
```

The response includes the webhook `secret`; store it to verify signatures.

## Webhook deliveries

Each POST carries the stored payload and these headers:

| Header | Meaning |
| :--- | :--- |
| `X-Webhook-Signature` | `HMAC-SHA256(secret, raw body)` hex digest |
| `X-Webhook-Id` | Webhook that matched |
| `X-Delivery-Id` | Stable delivery id (`eventId` in the body) |
| `X-Delivery-Attempt` | 1-based attempt counter |

```typescript
import crypto from "crypto";

export function verify(rawBody: string, signature: string, secret: string): boolean {
  const expected = crypto.createHmac("sha256", secret).update(rawBody, "utf8").digest("hex");
  return expected.length === signature.length &&
    crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected));
}
```

Payload (stored verbatim in `deliveries.payload_json`):

```json
{
  "eventId": "del_...",
  "eventType": "USDT_TRANSFER",
  "network": "TRC20",
  "networkMode": "mainnet",
  "webhookId": "wh_...",
  "planId": "plan_...",
  "matchedAddress": "TXYZopYRdj2D9XRtbG411XZZ3kM5VkAeBf",
  "direction": "INCOMING",
  "transaction": {
    "txHash": "…", "logIndex": 0, "blockNumber": 65432198, "blockTimestamp": 1760458200000,
    "fromAddress": "T…", "toAddress": "T…", "tokenContract": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
    "tokenSymbol": "USDT", "decimals": 6, "amountRaw": "100500000", "amountDecimal": "100.5"
  },
  "createdAt": "2026-10-14T16:50:00.000Z"
}
```

Retries: `408`, `429`, `5xx`, and network errors back off exponentially up to `max_attempts` (5);
other `4xx` responses and exhausted attempts move the delivery to `DEAD_LETTER`.
**Delivery is at-least-once** — deduplicate on `X-Delivery-Id` (`eventId`).

## Operations

- **WAL + `synchronous = NORMAL`**: no committed state is corrupted on crash, but an OS-level power
  loss can drop the most recent commits; the affected block is simply re-scanned. Use
  `PRAGMA synchronous = FULL` if every commit must survive power loss.
- **Single scanner per network**: the scanner takes a 30 s lease (`block_checkpoints.lease_owner`)
  and heartbeats every 10 s; a second instance exits non-zero rather than double-counting quotas.
- **Reorgs**: each block's `parentHash` must match the committed `last_block_hash`. A mismatch stops
  that network's scanner and leaves the checkpoint untouched.
- **Retention**: terminal deliveries older than `DELIVERY_RETENTION_DAYS` are pruned in bounded
  batches; `PENDING`/`FAILED`/`PROCESSING` rows are never pruned.
- **Backups**: use the SQLite online backup API (or stop the process) rather than copying a live
  WAL file set.

## Development

```bash
npm run typecheck   # tsc --noEmit
npm test            # vitest: schema invariants, quota accounting, scanner/worker pipeline
```

## License

[MIT](LICENSE)
