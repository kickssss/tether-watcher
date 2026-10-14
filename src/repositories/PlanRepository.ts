import type { InArgs } from "@libsql/client";
import type { Database, Queryable, Tx } from "../db/database";
import {
  incrementDroppedEvents,
  tryConsumeEventQuota,
} from "../db/statements";
import { NotFoundError } from "../errors";
import type { SubscriptionPlan } from "../types";
import { generateId } from "../utils/id";

export interface PlanRow {
  id: string;
  name: string;
  max_webhooks: number;
  max_addresses: number;
  max_deliverable_events: number;
  used_webhooks_count: number;
  used_addresses_count: number;
  delivered_events_count: number;
  dropped_events_count: number;
  is_active: number;
  created_at: number;
  updated_at: number;
}

export function toSubscriptionPlan(row: PlanRow): SubscriptionPlan {
  return {
    id: row.id,
    name: row.name,
    maxWebhooks: row.max_webhooks,
    maxAddresses: row.max_addresses,
    maxDeliverableEvents: row.max_deliverable_events,
    usedWebhooksCount: row.used_webhooks_count,
    usedAddressesCount: row.used_addresses_count,
    deliveredEventsCount: row.delivered_events_count,
    droppedEventsCount: row.dropped_events_count,
    isActive: row.is_active === 1,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export interface CreatePlanInput {
  id?: string;
  name: string;
  maxWebhooks: number;
  maxAddresses: number;
  maxDeliverableEvents: number;
}

export interface UpdatePlanInput {
  name?: string;
  maxWebhooks?: number;
  maxAddresses?: number;
  maxDeliverableEvents?: number;
  isActive?: boolean;
  /** Zeroes `delivered_events_count` and `dropped_events_count` (§10.1). */
  resetEventQuota?: boolean;
}

export interface CounterSnapshot {
  usedWebhooksCount: number;
  usedAddressesCount: number;
}

export interface ReconcileResult {
  stored: CounterSnapshot;
  recomputed: CounterSnapshot;
}

/**
 * `subscription_plans` access plus the atomic conditional quota updates of §7.2.
 * Methods taking a `Tx` compose inside a caller-owned transaction; the rest own theirs.
 */
export class PlanRepository {
  constructor(private readonly db: Database) {}

  async create(input: CreatePlanInput): Promise<SubscriptionPlan> {
    const id = input.id ?? generateId("plan");
    return this.db.write(async (tx) => {
      await tx.run(
        `INSERT INTO subscription_plans (id, name, max_webhooks, max_addresses, max_deliverable_events)
         VALUES (?, ?, ?, ?, ?)`,
        [id, input.name, input.maxWebhooks, input.maxAddresses, input.maxDeliverableEvents],
      );
      const plan = await this.findById(tx, id);
      if (!plan) throw new NotFoundError("plan", id);
      return plan;
    });
  }

  async getById(id: string): Promise<SubscriptionPlan> {
    const plan = await this.findById(this.db, id);
    if (!plan) throw new NotFoundError("plan", id);
    return plan;
  }

  async findById(
    reader: Queryable,
    id: string,
  ): Promise<SubscriptionPlan | null> {
    const rows = await reader.query<PlanRow>(
      "SELECT * FROM subscription_plans WHERE id = ?",
      [id],
    );
    return rows[0] ? toSubscriptionPlan(rows[0]) : null;
  }

  async update(id: string, patch: UpdatePlanInput): Promise<SubscriptionPlan> {
    return this.db.write(async (tx) => {
      const existing = await this.findById(tx, id);
      if (!existing) throw new NotFoundError("plan", id);

      const assignments: string[] = [];
      const args: InArgs = [];
      if (patch.name !== undefined) {
        assignments.push("name = ?");
        args.push(patch.name);
      }
      if (patch.maxWebhooks !== undefined) {
        assignments.push("max_webhooks = ?");
        args.push(patch.maxWebhooks);
      }
      if (patch.maxAddresses !== undefined) {
        assignments.push("max_addresses = ?");
        args.push(patch.maxAddresses);
      }
      if (patch.maxDeliverableEvents !== undefined) {
        assignments.push("max_deliverable_events = ?");
        args.push(patch.maxDeliverableEvents);
      }
      if (patch.isActive !== undefined) {
        assignments.push("is_active = ?");
        args.push(patch.isActive ? 1 : 0);
      }
      if (patch.resetEventQuota) {
        assignments.push("delivered_events_count = 0", "dropped_events_count = 0");
      }
      if (assignments.length === 0) return existing;

      assignments.push("updated_at = ?");
      args.push(Date.now(), id);
      await tx.run(
        `UPDATE subscription_plans SET ${assignments.join(", ")} WHERE id = ?`,
        args,
      );
      const updated = await this.findById(tx, id);
      if (!updated) throw new NotFoundError("plan", id);
      return updated;
    });
  }

  /** Recomputes the derived counters from the source tables (§7.2). */
  async reconcile(id: string): Promise<ReconcileResult> {
    return this.db.write(async (tx) => {
      const plan = await this.findById(tx, id);
      if (!plan) throw new NotFoundError("plan", id);

      const webhooks = await tx.query<{ n: number }>(
        "SELECT COUNT(*) AS n FROM webhooks WHERE plan_id = ?",
        [id],
      );
      const addresses = await tx.query<{ n: number }>(
        "SELECT COUNT(*) AS n FROM webhook_addresses WHERE plan_id = ?",
        [id],
      );
      const recomputed: CounterSnapshot = {
        usedWebhooksCount: Number(webhooks[0]?.n ?? 0),
        usedAddressesCount: Number(addresses[0]?.n ?? 0),
      };
      await tx.run(
        `UPDATE subscription_plans
         SET used_webhooks_count = ?, used_addresses_count = ?, updated_at = ?
         WHERE id = ?`,
        [
          recomputed.usedWebhooksCount,
          recomputed.usedAddressesCount,
          Date.now(),
          id,
        ],
      );
      return {
        stored: {
          usedWebhooksCount: plan.usedWebhooksCount,
          usedAddressesCount: plan.usedAddressesCount,
        },
        recomputed,
      };
    });
  }

  /** Plan counters for `GET /stats`. */
  async stats(): Promise<{ totalPlans: number; activePlans: number }> {
    const rows = await this.db.query<{ total: number; active: number }>(
      `SELECT COUNT(*) AS total, COALESCE(SUM(is_active), 0) AS active
       FROM subscription_plans`,
    );
    return {
      totalPlans: Number(rows[0]?.total ?? 0),
      activePlans: Number(rows[0]?.active ?? 0),
    };
  }

  /** Reserves one deliverable event; `false` means the quota is exhausted (§7.2). */
  async tryConsumeEventQuota(tx: Tx, planId: string): Promise<boolean> {
    const affected = await tx.run(tryConsumeEventQuota, [Date.now(), planId]);
    return affected === 1;
  }

  /** Counts an over-quota match as dropped without queueing it (§7.1, §8.2). */
  async incrementDroppedEvents(tx: Tx, planId: string): Promise<void> {
    await tx.run(incrementDroppedEvents, [Date.now(), planId]);
  }

  /** `used_webhooks_count + 1 <= max_webhooks`, active plan only (§7.2). */
  async tryConsumeWebhookQuota(tx: Tx, planId: string): Promise<boolean> {
    const affected = await tx.run(
      `UPDATE subscription_plans
       SET used_webhooks_count = used_webhooks_count + 1, updated_at = ?
       WHERE id = ? AND is_active = 1 AND (used_webhooks_count + 1) <= max_webhooks`,
      [Date.now(), planId],
    );
    return affected === 1;
  }

  /** `used_addresses_count + count <= max_addresses`, active plan only (§7.2). */
  async tryAddAddressQuota(
    tx: Tx,
    planId: string,
    count: number,
  ): Promise<boolean> {
    if (count === 0) return true;
    const affected = await tx.run(
      `UPDATE subscription_plans
       SET used_addresses_count = used_addresses_count + ?, updated_at = ?
       WHERE id = ? AND is_active = 1 AND (used_addresses_count + ?) <= max_addresses`,
      [count, Date.now(), planId, count],
    );
    return affected === 1;
  }

  async releaseWebhookQuota(tx: Tx, planId: string, count = 1): Promise<void> {
    await tx.run(
      `UPDATE subscription_plans
       SET used_webhooks_count = MAX(0, used_webhooks_count - ?), updated_at = ?
       WHERE id = ?`,
      [count, Date.now(), planId],
    );
  }

  async releaseAddressQuota(
    tx: Tx,
    planId: string,
    count: number,
  ): Promise<void> {
    if (count === 0) return;
    await tx.run(
      `UPDATE subscription_plans
       SET used_addresses_count = MAX(0, used_addresses_count - ?), updated_at = ?
       WHERE id = ?`,
      [count, Date.now(), planId],
    );
  }
}
