import type { Tx } from "../db/database";
import { matchBlockAddresses } from "../db/statements";
import type { Network } from "../types";

export interface MatchRow {
  address: string;
  webhook_id: string;
  plan_id: string;
  webhook_url: string;
  webhook_secret: string;
}

export interface AddressMatch {
  address: string;
  webhookId: string;
  planId: string;
  webhookUrl: string;
  webhookSecret: string;
}

/**
 * §6.3 block-level batch lookup: one statement per block, driven by the JSON list of
 * addresses in that block. Runs inside the scanner's per-block transaction so matches,
 * quota counters, and the checkpoint all see a single snapshot.
 *
 * The statement's `CROSS JOIN` is load-bearing — see `src/db/statements.ts` and the
 * query-plan assertion in `tests/`.
 */
export async function findBlockMatches(
  tx: Tx,
  network: Network,
  addresses: string[],
): Promise<AddressMatch[]> {
  if (addresses.length === 0) return [];
  const rows = await tx.query<MatchRow>(matchBlockAddresses, {
    addressesJson: JSON.stringify(addresses),
    network,
  });
  return rows.map((row) => ({
    address: row.address,
    webhookId: row.webhook_id,
    planId: row.plan_id,
    webhookUrl: row.webhook_url,
    webhookSecret: row.webhook_secret,
  }));
}
