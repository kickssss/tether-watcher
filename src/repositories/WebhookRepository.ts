import { randomBytes } from "crypto";
import type { Database, Queryable, Tx } from "../db/database";
import {
  AddressNetworkMismatchError,
  AddressQuotaExceededError,
  NetworkFilterConflictError,
  NotFoundError,
  PlanInactiveError,
  WebhookQuotaExceededError,
} from "../errors";
import type { Network, NetworkFilter, WatchedAddress, Webhook } from "../types";
import { normalizeAddress } from "../utils/addressNormalizer";
import { generateId } from "../utils/id";
import type { PlanRepository } from "./PlanRepository";

export interface WebhookRow {
  id: string;
  plan_id: string;
  url: string;
  secret: string;
  description: string | null;
  network_filter: NetworkFilter;
  is_active: number;
  created_at: number;
  updated_at: number;
}

export interface AddressRow {
  network: Network;
  address: string;
  webhook_id: string;
  plan_id: string;
  created_at: number;
}

export function toWebhook(row: WebhookRow): Webhook {
  return {
    id: row.id,
    planId: row.plan_id,
    url: row.url,
    secret: row.secret,
    description: row.description,
    networkFilter: row.network_filter,
    isActive: row.is_active === 1,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export function toWatchedAddress(row: AddressRow): WatchedAddress {
  return {
    network: row.network,
    address: row.address,
    webhookId: row.webhook_id,
    planId: row.plan_id,
    createdAt: row.created_at,
  };
}

export interface AddressInput {
  network: Network;
  address: string;
}

export interface CreateWebhookInput {
  planId: string;
  url: string;
  secret?: string;
  description?: string | null;
  networkFilter?: NetworkFilter;
  addresses?: AddressInput[];
}

export interface UpdateWebhookInput {
  url?: string;
  secret?: string;
  description?: string | null;
  networkFilter?: NetworkFilter;
  isActive?: boolean;
  /** Moves the webhook (and its addresses) to another plan (§7.2, §10.2). */
  planId?: string;
}

export interface AddressPage {
  addresses: WatchedAddress[];
  nextCursor: string | null;
}

/** Normalizes, deduplicates, and filter-checks a batch of address inputs (§6.2, §10.2). */
function normalizeAddressInputs(
  filter: NetworkFilter,
  inputs: AddressInput[],
): Array<{ network: Network; address: string }> {
  const seen = new Set<string>();
  const normalized: Array<{ network: Network; address: string }> = [];
  for (const input of inputs) {
    if (filter !== "ALL" && input.network !== filter) {
      throw new AddressNetworkMismatchError(
        input.address,
        input.network,
        filter,
      );
    }
    const address = normalizeAddress(input.network, input.address);
    const key = `${input.network}:${address}`;
    if (seen.has(key)) continue;
    seen.add(key);
    normalized.push({ network: input.network, address });
  }
  return normalized;
}

/**
 * `webhooks` + `webhook_addresses` access. Every mutation owns one transaction and
 * keeps the plan's materialized counters consistent with the rows it writes (§7.2).
 */
export class WebhookRepository {
  constructor(
    private readonly db: Database,
    private readonly plans: PlanRepository,
  ) {}

  async create(
    input: CreateWebhookInput,
  ): Promise<{ webhook: Webhook; createdAddresses: number }> {
    const filter: NetworkFilter = input.networkFilter ?? "ALL";
    const addresses = normalizeAddressInputs(filter, input.addresses ?? []);
    const id = generateId("wh");
    const secret = input.secret ?? randomBytes(32).toString("hex");

    return this.db.write(async (tx) => {
      const plan = await this.plans.findById(tx, input.planId);
      if (!plan) throw new NotFoundError("plan", input.planId);
      if (!plan.isActive) throw new PlanInactiveError(input.planId);

      if (!(await this.plans.tryConsumeWebhookQuota(tx, input.planId))) {
        throw new WebhookQuotaExceededError(input.planId);
      }
      await tx.run(
        `INSERT INTO webhooks (id, plan_id, url, secret, description, network_filter)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [id, input.planId, input.url, secret, input.description ?? null, filter],
      );

      const createdAddresses = await this.insertAddressRows(
        tx,
        id,
        input.planId,
        addresses,
      );
      if (!(await this.plans.tryAddAddressQuota(tx, input.planId, createdAddresses))) {
        throw new AddressQuotaExceededError(input.planId);
      }

      const webhook = await this.findById(tx, id);
      if (!webhook) throw new NotFoundError("webhook", id);
      return { webhook, createdAddresses };
    });
  }

  async findById(
    reader: Queryable,
    id: string,
  ): Promise<Webhook | null> {
    const rows = await reader.query<WebhookRow>(
      "SELECT * FROM webhooks WHERE id = ?",
      [id],
    );
    return rows[0] ? toWebhook(rows[0]) : null;
  }

  async getById(id: string): Promise<Webhook> {
    const webhook = await this.findById(this.db, id);
    if (!webhook) throw new NotFoundError("webhook", id);
    return webhook;
  }

  async countAddresses(reader: Queryable, webhookId: string): Promise<number> {
    const rows = await reader.query<{ n: number }>(
      "SELECT COUNT(*) AS n FROM webhook_addresses WHERE webhook_id = ?",
      [webhookId],
    );
    return Number(rows[0]?.n ?? 0);
  }

  /** Keyset pagination over `idx_webhook_addresses_webhook_id`. */
  async listAddresses(params: {
    webhookId: string;
    limit: number;
    cursor?: string | null;
  }): Promise<AddressPage> {
    const { webhookId, limit, cursor } = params;
    const base = "SELECT * FROM webhook_addresses WHERE webhook_id = ?";
    const order = "ORDER BY network ASC, address ASC LIMIT ?";
    const rows = cursor
      ? await this.db.query<AddressRow>(
          `${base} AND network = ? AND address > ? ${order}`,
          [webhookId, cursor.slice(0, cursor.indexOf(":")), cursor.slice(cursor.indexOf(":") + 1), limit + 1],
        )
      : await this.db.query<AddressRow>(`${base} ${order}`, [webhookId, limit + 1]);

    const hasMore = rows.length > limit;
    const page = hasMore ? rows.slice(0, limit) : rows;
    const last = page.at(-1);
    return {
      addresses: page.map(toWatchedAddress),
      nextCursor: hasMore && last ? `${last.network}:${last.address}` : null,
    };
  }

  async addAddresses(
    webhookId: string,
    inputs: AddressInput[],
  ): Promise<{ added: number }> {
    return this.db.write(async (tx) => {
      const webhook = await this.findById(tx, webhookId);
      if (!webhook) throw new NotFoundError("webhook", webhookId);

      const addresses = normalizeAddressInputs(webhook.networkFilter, inputs);
      const added = await this.insertAddressRows(
        tx,
        webhookId,
        webhook.planId,
        addresses,
      );
      if (!(await this.plans.tryAddAddressQuota(tx, webhook.planId, added))) {
        throw new AddressQuotaExceededError(webhook.planId);
      }
      return { added };
    });
  }

  async removeAddresses(
    webhookId: string,
    inputs: AddressInput[],
  ): Promise<{ removed: number }> {
    return this.db.write(async (tx) => {
      const webhook = await this.findById(tx, webhookId);
      if (!webhook) throw new NotFoundError("webhook", webhookId);

      const addresses = normalizeAddressInputs("ALL", inputs);
      if (addresses.length === 0) return { removed: 0 };

      const placeholders = addresses.map(() => "(?, ?)").join(", ");
      const args = addresses.flatMap((entry) => [entry.network, entry.address]);
      const removed = await tx.run(
        `DELETE FROM webhook_addresses
         WHERE webhook_id = ? AND (network, address) IN (VALUES ${placeholders})`,
        [webhookId, ...args],
      );
      await this.plans.releaseAddressQuota(tx, webhook.planId, removed);
      return { removed };
    });
  }

  async update(id: string, patch: UpdateWebhookInput): Promise<Webhook> {
    return this.db.write(async (tx) => {
      const webhook = await this.findById(tx, id);
      if (!webhook) throw new NotFoundError("webhook", id);

      if (patch.networkFilter !== undefined && patch.networkFilter !== "ALL") {
        const conflicts = await tx.query<{ address: string }>(
          `SELECT address FROM webhook_addresses
           WHERE webhook_id = ? AND network <> ? AND network <> 'ALL'
           LIMIT 50`,
          [id, patch.networkFilter],
        );
        if (conflicts.length > 0) {
          throw new NetworkFilterConflictError(conflicts.map((row) => row.address));
        }
      }

      const movingPlan =
        patch.planId !== undefined && patch.planId !== webhook.planId;
      if (movingPlan) {
        const target = await this.plans.findById(tx, patch.planId!);
        if (!target) throw new NotFoundError("plan", patch.planId!);
        if (!target.isActive) throw new PlanInactiveError(patch.planId!);

        const addressCount = await this.countAddresses(tx, id);
        if (!(await this.plans.tryConsumeWebhookQuota(tx, patch.planId!))) {
          throw new WebhookQuotaExceededError(patch.planId!);
        }
        if (!(await this.plans.tryAddAddressQuota(tx, patch.planId!, addressCount))) {
          throw new AddressQuotaExceededError(patch.planId!);
        }
        await this.plans.releaseWebhookQuota(tx, webhook.planId);
        await this.plans.releaseAddressQuota(tx, webhook.planId, addressCount);
        await tx.run(
          "UPDATE webhook_addresses SET plan_id = ? WHERE webhook_id = ?",
          [patch.planId!, id],
        );
      }

      const assignments: string[] = [];
      const args: Array<string | number | null> = [];
      if (patch.url !== undefined) {
        assignments.push("url = ?");
        args.push(patch.url);
      }
      if (patch.secret !== undefined) {
        assignments.push("secret = ?");
        args.push(patch.secret);
      }
      if (patch.description !== undefined) {
        assignments.push("description = ?");
        args.push(patch.description);
      }
      if (patch.networkFilter !== undefined) {
        assignments.push("network_filter = ?");
        args.push(patch.networkFilter);
      }
      if (patch.isActive !== undefined) {
        assignments.push("is_active = ?");
        args.push(patch.isActive ? 1 : 0);
      }
      if (movingPlan) {
        assignments.push("plan_id = ?");
        args.push(patch.planId!);
      }
      if (assignments.length > 0) {
        assignments.push("updated_at = ?");
        args.push(Date.now(), id);
        await tx.run(
          `UPDATE webhooks SET ${assignments.join(", ")} WHERE id = ?`,
          args,
        );
      }

      const updated = await this.findById(tx, id);
      if (!updated) throw new NotFoundError("webhook", id);
      return updated;
    });
  }

  /** Deletes a webhook with its addresses and queued deliveries, releasing quotas (§10.2). */
  async delete(id: string): Promise<{ deletedAddresses: number; deletedDeliveries: number }> {
    return this.db.write(async (tx) => {
      const webhook = await this.findById(tx, id);
      if (!webhook) throw new NotFoundError("webhook", id);

      const addressCount = await this.countAddresses(tx, id);
      const deletedDeliveries = await tx.run(
        "DELETE FROM deliveries WHERE webhook_id = ?",
        [id],
      );
      await tx.run("DELETE FROM webhook_addresses WHERE webhook_id = ?", [id]);
      await tx.run("DELETE FROM webhooks WHERE id = ?", [id]);
      await this.plans.releaseWebhookQuota(tx, webhook.planId);
      await this.plans.releaseAddressQuota(tx, webhook.planId, addressCount);
      return { deletedAddresses: addressCount, deletedDeliveries };
    });
  }

  /** Multi-row `INSERT OR IGNORE`; returns the number of rows actually inserted. */
  private async insertAddressRows(
    tx: Tx,
    webhookId: string,
    planId: string,
    addresses: Array<{ network: Network; address: string }>,
  ): Promise<number> {
    if (addresses.length === 0) return 0;
    const placeholders = addresses.map(() => "(?, ?, ?, ?)").join(", ");
    const args = addresses.flatMap((entry) => [
      entry.network,
      entry.address,
      webhookId,
      planId,
    ]);
    return tx.run(
      `INSERT OR IGNORE INTO webhook_addresses (network, address, webhook_id, plan_id)
       VALUES ${placeholders}`,
      args,
    );
  }
}
