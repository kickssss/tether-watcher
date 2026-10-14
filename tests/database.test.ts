import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { isConstraintError } from "../src/db/database";
import { insertDelivery, matchBlockAddresses } from "../src/db/statements";
import {
  AddressNetworkMismatchError,
  AddressQuotaExceededError,
  NetworkFilterConflictError,
  WebhookQuotaExceededError,
} from "../src/errors";
import { CheckpointRepository } from "../src/repositories/CheckpointRepository";
import { DeliveryRepository } from "../src/repositories/DeliveryRepository";
import { PlanRepository } from "../src/repositories/PlanRepository";
import { WebhookRepository } from "../src/repositories/WebhookRepository";
import {
  BSC_ADDRESS,
  TRON_ADDRESS,
  TRON_OTHER_ADDRESS,
  openTestDatabase,
  type TestDatabase,
} from "./helpers";

let context: TestDatabase;
let plans: PlanRepository;
let webhooks: WebhookRepository;
let deliveries: DeliveryRepository;
let checkpoints: CheckpointRepository;

beforeAll(async () => {
  context = await openTestDatabase();
  plans = new PlanRepository(context.db);
  webhooks = new WebhookRepository(context.db, plans);
  deliveries = new DeliveryRepository(context.db);
  checkpoints = new CheckpointRepository(context.db);
});

afterAll(async () => {
  await context.close();
});

const newPlan = (overrides: Record<string, number> = {}) =>
  plans.create({
    name: "test plan",
    maxWebhooks: 2,
    maxAddresses: 5,
    maxDeliverableEvents: 10,
    ...overrides,
  });

describe("schema and engine setup", () => {
  test("migrations create all tables with WAL and foreign keys enabled", async () => {
    const tables = await context.db.query<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
    );
    const names = tables.map((row) => row.name);
    expect(names).toEqual(
      expect.arrayContaining([
        "subscription_plans",
        "webhooks",
        "webhook_addresses",
        "block_checkpoints",
        "deliveries",
      ]),
    );

    const version = await context.db.queryOne<{ user_version: number }>(
      "PRAGMA user_version",
    );
    expect(Number(version?.user_version)).toBe(1);

    const journal = await context.db.queryOne<{ journal_mode: string }>(
      "PRAGMA journal_mode",
    );
    expect(journal?.journal_mode).toBe("wal");

    const foreignKeys = await context.db.queryOne<{ foreign_keys: number }>(
      "PRAGMA foreign_keys",
    );
    expect(Number(foreignKeys?.foreign_keys)).toBe(1);
  });

  test("block-level match query drives from the JSON list into the clustered primary key", async () => {
    const plan = await newPlan();
    const webhook = await webhooks.create({
      planId: plan.id,
      url: "https://example.com/hook",
      addresses: [{ network: "BEP20", address: BSC_ADDRESS }],
    });

    const rows = await context.db.write(async (tx) => {
      // Seed enough rows that a wrong plan would be visible in the output.
      for (let index = 0; index < 200; index++) {
        await tx.run(
          `INSERT OR IGNORE INTO webhook_addresses (network, address, webhook_id, plan_id)
           VALUES ('BEP20', ?, ?, ?)`,
          [`0x${index.toString(16).padStart(40, "0")}`, webhook.webhook.id, plan.id],
        );
      }
      return tx.query<{ detail: string }>(`EXPLAIN QUERY PLAN ${matchBlockAddresses}`, {
        addressesJson: JSON.stringify([BSC_ADDRESS]),
        network: "BEP20",
      });
    });

    const detail = rows.map((row) => String(row.detail)).join(" | ");
    expect(detail).toContain("SEARCH wa USING PRIMARY KEY");
    expect(detail).not.toContain("idx_webhook_addresses_plan_id");
  });
});

describe("write serialization and atomicity", () => {
  test("concurrent writes are queued instead of failing with SQLITE_BUSY", async () => {
    const results = await Promise.allSettled(
      Array.from({ length: 20 }, (_, index) =>
        plans.create({
          id: `concurrent_${index}`,
          name: `concurrent ${index}`,
          maxWebhooks: 1,
          maxAddresses: 1,
          maxDeliverableEvents: 1,
        }),
      ),
    );
    expect(results.every((result) => result.status === "fulfilled")).toBe(true);
  });

  test("a throwing write transaction rolls back every statement", async () => {
    await expect(
      context.db.write(async (tx) => {
        await tx.run(
          "INSERT INTO subscription_plans (id, name, max_webhooks, max_addresses, max_deliverable_events) VALUES (?, ?, 1, 1, 1)",
          ["rollback_plan", "rollback plan"],
        );
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");

    const row = await context.db.queryOne<{ id: string }>(
      "SELECT id FROM subscription_plans WHERE id = ?",
      ["rollback_plan"],
    );
    expect(row).toBeNull();
  });
});

describe("quota accounting", () => {
  test("deliverable event quota stops at max and counts the rest as dropped", async () => {
    const plan = await newPlan({ maxDeliverableEvents: 2 });

    const outcomes = await context.db.write(async (tx) => {
      const results: string[] = [];
      for (let index = 0; index < 5; index++) {
        if (await plans.tryConsumeEventQuota(tx, plan.id)) {
          results.push("delivered");
        } else {
          await plans.incrementDroppedEvents(tx, plan.id);
          results.push("dropped");
        }
      }
      return results;
    });

    expect(outcomes).toEqual([
      "delivered",
      "delivered",
      "dropped",
      "dropped",
      "dropped",
    ]);
    const updated = await plans.getById(plan.id);
    expect(updated.deliveredEventsCount).toBe(2);
    expect(updated.droppedEventsCount).toBe(3);
  });

  test("address quota rejects the batch without partial inserts", async () => {
    const plan = await newPlan({ maxAddresses: 1 });
    const webhook = await webhooks.create({
      planId: plan.id,
      url: "https://example.com/hook",
    });

    await webhooks.addAddresses(webhook.webhook.id, [
      { network: "TRC20", address: TRON_ADDRESS },
    ]);

    await expect(
      webhooks.addAddresses(webhook.webhook.id, [
        { network: "TRC20", address: TRON_OTHER_ADDRESS },
      ]),
    ).rejects.toBeInstanceOf(AddressQuotaExceededError);

    const planAfter = await plans.getById(plan.id);
    expect(planAfter.usedAddressesCount).toBe(1);
    const addresses = await webhooks.listAddresses({
      webhookId: webhook.webhook.id,
      limit: 10,
    });
    expect(addresses.addresses).toHaveLength(1);
  });

  test("webhook quota rejects creation beyond max_webhooks", async () => {
    const plan = await newPlan({ maxWebhooks: 1 });
    await webhooks.create({ planId: plan.id, url: "https://example.com/one" });

    await expect(
      webhooks.create({ planId: plan.id, url: "https://example.com/two" }),
    ).rejects.toBeInstanceOf(WebhookQuotaExceededError);
    expect((await plans.getById(plan.id)).usedWebhooksCount).toBe(1);
  });
});

describe("address assignment rules", () => {
  test("the same address on two webhooks counts twice, and is idempotent per webhook", async () => {
    const plan = await newPlan({ maxWebhooks: 2, maxAddresses: 2 });
    const first = await webhooks.create({
      planId: plan.id,
      url: "https://example.com/first",
    });
    const second = await webhooks.create({
      planId: plan.id,
      url: "https://example.com/second",
    });

    const address = { network: "TRC20" as const, address: TRON_ADDRESS };
    await webhooks.addAddresses(first.webhook.id, [address]);
    await webhooks.addAddresses(second.webhook.id, [address]);
    const duplicate = await webhooks.addAddresses(first.webhook.id, [address]);

    expect(duplicate.added).toBe(0);
    expect((await plans.getById(plan.id)).usedAddressesCount).toBe(2);

    const removal = await webhooks.removeAddresses(second.webhook.id, [address]);
    expect(removal.removed).toBe(1);
    expect((await plans.getById(plan.id)).usedAddressesCount).toBe(1);
  });

  test("an address contradicting the webhook network filter is rejected", async () => {
    const plan = await newPlan();
    const webhook = await webhooks.create({
      planId: plan.id,
      url: "https://example.com/trc20-only",
      networkFilter: "TRC20",
    });

    await expect(
      webhooks.addAddresses(webhook.webhook.id, [
        { network: "BEP20", address: BSC_ADDRESS },
      ]),
    ).rejects.toBeInstanceOf(AddressNetworkMismatchError);
  });

  test("narrowing a network filter with conflicting addresses returns 409", async () => {
    const plan = await newPlan();
    const webhook = await webhooks.create({
      planId: plan.id,
      url: "https://example.com/narrow",
      addresses: [{ network: "BEP20", address: BSC_ADDRESS }],
    });

    await expect(
      webhooks.update(webhook.webhook.id, { networkFilter: "TRC20" }),
    ).rejects.toBeInstanceOf(NetworkFilterConflictError);
    expect(
      (await webhooks.getById(webhook.webhook.id)).networkFilter,
    ).toBe("ALL");
  });
});

describe("checkpoints and leases", () => {
  test("the lease is exclusive, renewable, and released on stop", async () => {
    const acquired = await checkpoints.acquireLease({
      network: "TRC20",
      instanceId: "scanner-a",
      initialBlock: 100,
    });
    expect(acquired).toBe(true);

    const stolen = await checkpoints.acquireLease({
      network: "TRC20",
      instanceId: "scanner-b",
      initialBlock: 100,
    });
    expect(stolen).toBe(false);
    expect((await checkpoints.get("TRC20"))?.leaseOwner).toBe("scanner-a");

    expect(await checkpoints.renewLease("TRC20", "scanner-b")).toBe(false);
    expect(await checkpoints.renewLease("TRC20", "scanner-a")).toBe(true);

    await checkpoints.releaseLease("TRC20", "scanner-a");
    expect(
      await checkpoints.acquireLease({
        network: "TRC20",
        instanceId: "scanner-b",
        initialBlock: 100,
      }),
    ).toBe(true);
  });

  test("an expired lease can be taken over", async () => {
    await checkpoints.acquireLease({
      network: "BEP20",
      instanceId: "dead-scanner",
      initialBlock: 7,
      ttlMs: -1,
    });
    const takenOver = await checkpoints.acquireLease({
      network: "BEP20",
      instanceId: "live-scanner",
      initialBlock: 7,
    });
    expect(takenOver).toBe(true);
    expect((await checkpoints.get("BEP20"))?.leaseOwner).toBe("live-scanner");
  });
});

describe("deliveries and derived counters", () => {
  test("the UNIQUE key blocks duplicate outbox rows for one webhook event", async () => {
    const plan = await newPlan();
    const webhook = await webhooks.create({
      planId: plan.id,
      url: "https://example.com/dedupe",
      addresses: [{ network: "TRC20", address: TRON_ADDRESS }],
    });

    const delivery = {
      id: "del_duplicate",
      planId: plan.id,
      webhookId: webhook.webhook.id,
      network: "TRC20" as const,
      blockNumber: 42,
      txHash: "a".repeat(64),
      logIndex: 0,
      fromAddress: TRON_OTHER_ADDRESS,
      toAddress: TRON_ADDRESS,
      matchedAddress: TRON_ADDRESS,
      direction: "INCOMING" as const,
      amountRaw: "1005000",
      amountDecimal: "1.005",
      tokenContract: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
      blockTimestamp: 1_700_000_000_000,
      payloadJson: "{}",
      nextAttemptAt: Date.now(),
    };

    await context.db.write((tx) => deliveries.insert(tx, delivery));
    expect(
      await context.db.write((tx) =>
        deliveries.exists(tx, {
          webhookId: delivery.webhookId,
          network: delivery.network,
          txHash: delivery.txHash,
          logIndex: delivery.logIndex,
          matchedAddress: delivery.matchedAddress,
        }),
      ),
    ).toBe(true);

    const duplicate = await context.db
      .write((tx) =>
        tx.run(insertDelivery, [
          "del_duplicate_2",
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
        ]),
      )
      .catch((error: unknown) => error);

    expect(isConstraintError(duplicate)).toBe(true);
  });

  test("deleting a webhook removes its addresses and deliveries and frees quota", async () => {
    const plan = await newPlan();
    const webhook = await webhooks.create({
      planId: plan.id,
      url: "https://example.com/delete-me",
      addresses: [{ network: "TRC20", address: TRON_ADDRESS }],
    });

    const removed = await webhooks.delete(webhook.webhook.id);
    expect(removed.deletedAddresses).toBe(1);

    const remainingAddresses = await webhooks.listAddresses({
      webhookId: webhook.webhook.id,
      limit: 10,
    });
    expect(remainingAddresses.addresses).toHaveLength(0);

    const updated = await plans.getById(plan.id);
    expect(updated.usedWebhooksCount).toBe(0);
    expect(updated.usedAddressesCount).toBe(0);
  });

  test("reconcile recomputes drifted counters from the source tables", async () => {
    const plan = await newPlan();
    await webhooks.create({
      planId: plan.id,
      url: "https://example.com/reconcile",
      addresses: [{ network: "TRC20", address: TRON_ADDRESS }],
    });

    await context.db.write((tx) =>
      tx.run("UPDATE subscription_plans SET used_addresses_count = 99 WHERE id = ?", [
        plan.id,
      ]),
    );

    const result = await plans.reconcile(plan.id);
    expect(result.stored.usedAddressesCount).toBe(99);
    expect(result.recomputed.usedAddressesCount).toBe(1);
    expect((await plans.getById(plan.id)).usedAddressesCount).toBe(1);
  });
});
