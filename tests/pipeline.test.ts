import { describe, expect, test } from "vitest";
import { LeaseUnavailableError, ReorgDetectedError } from "../src/errors";
import type { BlockData, DeliveryStatus, Network } from "../src/types";
import { generateSignature } from "../src/utils/generateSignature";
import {
  BSC_ADDRESS,
  BSC_OTHER_ADDRESS,
  FakeBlockProvider,
  TRON_ADDRESS,
  TRON_OTHER_ADDRESS,
  USDT_TRC20,
  createPipeline,
  startReceiver,
  waitFor,
  type TestContext,
} from "./helpers";

function tronBlock(overrides: Partial<BlockData> = {}): BlockData {
  return {
    number: 6,
    hash: "aa".repeat(32),
    parentHash: "bb".repeat(32),
    timestamp: 1_700_000_000_000,
    transfers: [
      {
        txHash: "c".repeat(64),
        logIndex: 0,
        from: TRON_OTHER_ADDRESS,
        to: TRON_ADDRESS,
        amountRaw: "1005000",
      },
    ],
    ...overrides,
  };
}

async function seedDelivery(
  context: TestContext,
  params: { planId: string; webhookId: string; network: Network; key: string },
): Promise<string> {
  const id = `del_${params.key}`;
  await context.db.write((tx) =>
    context.deliveries.insert(tx, {
      id,
      planId: params.planId,
      webhookId: params.webhookId,
      network: params.network,
      blockNumber: 1,
      txHash: params.key.padEnd(64, "0").slice(0, 64),
      logIndex: 0,
      fromAddress: TRON_OTHER_ADDRESS,
      toAddress: TRON_ADDRESS,
      matchedAddress: TRON_ADDRESS,
      direction: "INCOMING",
      amountRaw: "1000000",
      amountDecimal: "1",
      tokenContract: USDT_TRC20,
      blockTimestamp: 1_700_000_000_000,
      payloadJson: "{}",
      nextAttemptAt: Date.now(),
    }),
  );
  return id;
}

describe("scanner to worker pipeline", () => {
  test("a matched transfer is queued, retried, and delivered with a valid signature", async () => {
    const context = await createPipeline();
    const receiver = await startReceiver((requestNumber) =>
      requestNumber === 1 ? 502 : 200,
    );

    const plan = await context.plans.create({
      name: "pipeline",
      maxWebhooks: 1,
      maxAddresses: 5,
      maxDeliverableEvents: 10,
    });
    const secret = "test-secret";
    const webhook = await context.webhooks.create({
      planId: plan.id,
      url: receiver.url,
      secret,
      networkFilter: "TRC20",
      addresses: [{ network: "TRC20", address: TRON_ADDRESS }],
    });

    const provider = new FakeBlockProvider("TRC20");
    provider.latestBlockNumber = 7;
    const block = tronBlock();
    provider.blocks.set(6, block);

    const scanner = context.makeScanner(provider, { startBlock: 6 });
    const worker = context.makeWorker();

    try {
      await scanner.start();
      await worker.start();

      const delivered = await waitFor(async () => {
        const [row] = await context.deliveries.list({ limit: 1, offset: 0 });
        return row?.status === "DELIVERED" ? row : null;
      });

      expect(delivered.direction).toBe("INCOMING");
      expect(delivered.matchedAddress).toBe(TRON_ADDRESS);
      expect(delivered.amountDecimal).toBe("1.005");
      expect(delivered.blockNumber).toBe(6);
      expect(delivered.status).toBe("DELIVERED");
      expect(delivered.attemptCount).toBe(2);
      expect(delivered.lastHttpStatus).toBe(200);
      expect(delivered.deliveredAt).not.toBeNull();

      expect(receiver.received).toHaveLength(2);
      expect(receiver.received[0].headers["x-delivery-id"]).toBe(delivered.id);
      expect(receiver.received[0].headers["x-delivery-attempt"]).toBe("1");
      expect(receiver.received[1].headers["x-delivery-attempt"]).toBe("2");
      expect(receiver.received[0].headers["x-webhook-id"]).toBe(
        webhook.webhook.id,
      );

      // Signed bytes must be exactly the posted bytes.
      for (const request of receiver.received) {
        expect(request.headers["x-webhook-signature"]).toBe(
          generateSignature(request.body, secret),
        );
      }

      const payload = JSON.parse(receiver.received[0].body);
      expect(payload).toMatchObject({
        eventId: delivered.id,
        eventType: "USDT_TRANSFER",
        network: "TRC20",
        networkMode: "mainnet",
        webhookId: webhook.webhook.id,
        planId: plan.id,
        matchedAddress: TRON_ADDRESS,
        direction: "INCOMING",
      });
      expect(payload.transaction).toMatchObject({
        txHash: "c".repeat(64),
        logIndex: 0,
        blockNumber: 6,
        blockTimestamp: 1_700_000_000_000,
        fromAddress: TRON_OTHER_ADDRESS,
        toAddress: TRON_ADDRESS,
        tokenContract: USDT_TRC20,
        tokenSymbol: "USDT",
        decimals: 6,
        amountRaw: "1005000",
        amountDecimal: "1.005",
      });

      const planAfter = await context.plans.getById(plan.id);
      expect(planAfter.deliveredEventsCount).toBe(1);
      expect(planAfter.droppedEventsCount).toBe(0);

      const checkpoint = await context.checkpoints.get("TRC20");
      expect(checkpoint?.lastProcessedBlock).toBe(6);
      expect(checkpoint?.lastBlockHash).toBe(block.hash);

      // A re-scan of the committed block must not double-count quota.
      const rescan = await scanner.commitBlock(block);
      expect(rescan).toMatchObject({ queued: 0, dropped: 0, duplicates: 1 });
      expect((await context.plans.getById(plan.id)).deliveredEventsCount).toBe(1);
      expect(await context.deliveries.list({ limit: 10, offset: 0 })).toHaveLength(1);
    } finally {
      await scanner.stop();
      await worker.stop();
      await receiver.close();
      await context.close();
    }
  }, 20_000);

  test("network_filter prevents cross-network deliveries", async () => {
    const context = await createPipeline();
    const plan = await context.plans.create({
      name: "filter",
      maxWebhooks: 2,
      maxAddresses: 5,
      maxDeliverableEvents: 10,
    });
    const allNetworks = await context.webhooks.create({
      planId: plan.id,
      url: "https://example.com/all",
      addresses: [{ network: "BEP20", address: BSC_ADDRESS }],
    });
    const tronOnly = await context.webhooks.create({
      planId: plan.id,
      url: "https://example.com/tron",
      addresses: [{ network: "BEP20", address: BSC_ADDRESS }],
    });
    // Simulate a legacy row that contradicts the filter (the API blocks this).
    await context.db.write((tx) =>
      tx.run("UPDATE webhooks SET network_filter = 'TRC20' WHERE id = ?", [
        tronOnly.webhook.id,
      ]),
    );

    const provider = new FakeBlockProvider("BEP20");
    provider.latestBlockNumber = 100;
    provider.blocks.set(
      6,
      tronBlock({
        number: 6,
        transfers: [
          {
            txHash: "d".repeat(64),
            logIndex: 3,
            from: BSC_OTHER_ADDRESS,
            to: BSC_ADDRESS,
            amountRaw: "2000000000000000000",
          },
        ],
      }),
    );

    try {
      const scanner = context.makeScanner(provider, { startBlock: 6 });
      const summary = await scanner.commitBlock(provider.blocks.get(6)!);
      expect(summary).toMatchObject({ queued: 1, dropped: 0 });

      const rows = await context.deliveries.list({ limit: 10, offset: 0 });
      expect(rows).toHaveLength(1);
      expect(rows[0].webhookId).toBe(allNetworks.webhook.id);
      expect(rows[0].amountDecimal).toBe("2");
      await scanner.stop();
    } finally {
      await context.close();
    }
  });

  test("quota exhaustion drops matches without queueing them", async () => {
    const context = await createPipeline();
    const plan = await context.plans.create({
      name: "quota",
      maxWebhooks: 1,
      maxAddresses: 5,
      maxDeliverableEvents: 1,
    });
    await context.webhooks.create({
      planId: plan.id,
      url: "https://example.com/quota",
      addresses: [{ network: "TRC20", address: TRON_ADDRESS }],
    });

    const provider = new FakeBlockProvider("TRC20");
    const block = tronBlock({
      transfers: [0, 1, 2].map((logIndex) => ({
        txHash: `e${logIndex}`.padEnd(64, "0"),
        logIndex,
        from: TRON_OTHER_ADDRESS,
        to: TRON_ADDRESS,
        amountRaw: "1000000",
      })),
    });

    try {
      const scanner = context.makeScanner(provider, { startBlock: 6 });
      const summary = await scanner.commitBlock(block);
      expect(summary).toMatchObject({ queued: 1, dropped: 2 });

      const planAfter = await context.plans.getById(plan.id);
      expect(planAfter.deliveredEventsCount).toBe(1);
      expect(planAfter.droppedEventsCount).toBe(2);
      expect(await context.deliveries.list({ limit: 10, offset: 0 })).toHaveLength(1);
      await scanner.stop();
    } finally {
      await context.close();
    }
  });

  test("a parent-hash mismatch stops the scanner and leaves the checkpoint untouched", async () => {
    const context = await createPipeline();
    const provider = new FakeBlockProvider("TRC20");
    provider.latestBlockNumber = 7;
    provider.blocks.set(6, tronBlock({ parentHash: "cc".repeat(32) }));

    const scanner = context.makeScanner(provider, { startBlock: 6 });
    // A previously committed block, whose hash is the expected parent of block 6.
    await context.db.write((tx) =>
      context.checkpoints.commitBlock(tx, {
        network: "TRC20",
        blockNumber: 5,
        blockHash: "dd".repeat(32),
        blockTimestamp: 1_699_999_000_000,
      }),
    );

    try {
      await scanner.start();
      const fatal = await waitFor(async () => scanner.fatalError);
      expect(fatal).toBeInstanceOf(ReorgDetectedError);
      expect(fatal.message).toContain("reorg detected");

      const checkpoint = await context.checkpoints.get("TRC20");
      expect(checkpoint?.lastProcessedBlock).toBe(5);
      expect(checkpoint?.lastBlockHash).toBe("dd".repeat(32));
      expect(await context.deliveries.list({ limit: 10, offset: 0 })).toHaveLength(0);
    } finally {
      await scanner.stop();
      await context.close();
    }
  });

  test("a second scanner for the same network cannot start", async () => {
    const context = await createPipeline();
    const provider = new FakeBlockProvider("TRC20");
    provider.latestBlockNumber = 0;

    const first = context.makeScanner(provider, {
      startBlock: 1,
      instanceId: "instance-a",
    });
    const second = context.makeScanner(provider, {
      startBlock: 1,
      instanceId: "instance-b",
    });

    try {
      await first.start();
      await expect(second.start()).rejects.toBeInstanceOf(LeaseUnavailableError);
      await first.stop();
      await second.start();
      await second.stop();
    } finally {
      await context.close();
    }
  });
});

describe("delivery worker state machine", () => {
  test("a 400 response dead-letters the delivery after one attempt", async () => {
    const context = await createPipeline();
    const receiver = await startReceiver(() => 400);
    const plan = await context.plans.create({
      name: "dead letter",
      maxWebhooks: 1,
      maxAddresses: 1,
      maxDeliverableEvents: 5,
    });
    const webhook = await context.webhooks.create({
      planId: plan.id,
      url: receiver.url,
    });
    const id = await seedDelivery(context, {
      planId: plan.id,
      webhookId: webhook.webhook.id,
      network: "TRC20",
      key: "bad-request",
    });

    const worker = context.makeWorker();
    try {
      await worker.start();
      const row = await waitFor(async () => {
        const [delivery] = await context.deliveries.list({ limit: 1, offset: 0 });
        return delivery?.status === "DEAD_LETTER" ? delivery : null;
      });
      expect(row.id).toBe(id);
      expect(row.attemptCount).toBe(1);
      expect(row.lastHttpStatus).toBe(400);
      expect(receiver.received).toHaveLength(1);
    } finally {
      await worker.stop();
      await receiver.close();
      await context.close();
    }
  }, 15_000);

  test("an unreachable endpoint schedules a retry with backoff", async () => {
    const context = await createPipeline();
    const plan = await context.plans.create({
      name: "offline",
      maxWebhooks: 1,
      maxAddresses: 1,
      maxDeliverableEvents: 5,
    });
    const webhook = await context.webhooks.create({
      planId: plan.id,
      url: "http://127.0.0.1:1/hook",
    });
    await seedDelivery(context, {
      planId: plan.id,
      webhookId: webhook.webhook.id,
      network: "TRC20",
      key: "offline",
    });

    const worker = context.makeWorker();
    try {
      await worker.start();
      const row = await waitFor(async () => {
        const [delivery] = await context.deliveries.list({ limit: 1, offset: 0 });
        return delivery?.status === "FAILED" ? delivery : null;
      });
      expect(row.attemptCount).toBe(1);
      expect(row.nextAttemptAt).toBeGreaterThan(Date.now());
      expect(row.lastError).toBeTruthy();
      expect(row.lastHttpStatus).toBeNull();
    } finally {
      await worker.stop();
      await context.close();
    }
  }, 15_000);

  test("stale PROCESSING rows are requeued and terminal rows are pruned", async () => {
    const context = await createPipeline();
    const plan = await context.plans.create({
      name: "maintenance",
      maxWebhooks: 1,
      maxAddresses: 1,
      maxDeliverableEvents: 5,
    });
    const webhook = await context.webhooks.create({
      planId: plan.id,
      url: "https://example.com/maintenance",
    });
    const stuckId = await seedDelivery(context, {
      planId: plan.id,
      webhookId: webhook.webhook.id,
      network: "TRC20",
      key: "stuck",
    });
    const processedId = await seedDelivery(context, {
      planId: plan.id,
      webhookId: webhook.webhook.id,
      network: "TRC20",
      key: "processed",
    });
    const pendingId = await seedDelivery(context, {
      planId: plan.id,
      webhookId: webhook.webhook.id,
      network: "TRC20",
      key: "still-pending",
    });

    try {
      const claimed = await context.deliveries.claimDue(10);
      expect(claimed.map((row) => row.id).sort()).toEqual(
        [stuckId, processedId, pendingId].sort(),
      );

      const recovered = await context.deliveries.recoverStaleProcessing(
        Date.now() + 60_000,
      );
      expect(recovered).toBe(3);
      const afterRecovery = await context.deliveries.countByStatus();
      expect(afterRecovery.find((row) => row.status === "PROCESSING")).toBeUndefined();

      await context.db.write(async (tx) => {
        await tx.run(
          "UPDATE deliveries SET status = 'DELIVERED', updated_at = ? WHERE id = ?",
          [Date.now() - 10 * 24 * 60 * 60 * 1000, processedId],
        );
        await tx.run("UPDATE deliveries SET updated_at = ? WHERE id = ?", [
          Date.now() - 10 * 24 * 60 * 60 * 1000,
          pendingId,
        ]);
      });

      const cutoff = Date.now() - 5 * 24 * 60 * 60 * 1000;
      const pruned = await context.deliveries.prune(cutoff, 10);
      expect(pruned).toBe(1);

      const remaining = await context.deliveries.list({ limit: 10, offset: 0 });
      const remainingIds = remaining.map((row) => row.id);
      expect(remainingIds).not.toContain(processedId);
      expect(remainingIds).toContain(pendingId);
    } finally {
      await context.close();
    }
  });

  test("terminal states survive a worker restart once DELIVERED", async () => {
    const context = await createPipeline();
    const receiver = await startReceiver(() => 200);
    const plan = await context.plans.create({
      name: "restart",
      maxWebhooks: 1,
      maxAddresses: 1,
      maxDeliverableEvents: 5,
    });
    const webhook = await context.webhooks.create({
      planId: plan.id,
      url: receiver.url,
    });
    await seedDelivery(context, {
      planId: plan.id,
      webhookId: webhook.webhook.id,
      network: "TRC20",
      key: "restart",
    });

    const worker = context.makeWorker();
    try {
      await worker.start();
      await waitFor(async () => {
        const [delivery] = await context.deliveries.list({ limit: 1, offset: 0 });
        return delivery?.status === "DELIVERED" ? delivery : null;
      });
      await worker.stop();

      // Deterministic proof that nothing is re-claimable, no wall-clock wait needed.
      const reclaimable = await context.deliveries.claimDue(10);
      expect(reclaimable).toHaveLength(0);

      const secondWorker = context.makeWorker();
      await secondWorker.start();
      await secondWorker.stop();

      expect(receiver.received).toHaveLength(1);
      const statuses = await context.deliveries.countByStatus();
      expect(statuses).toEqual([{ status: "DELIVERED" as DeliveryStatus, count: 1 }]);
    } finally {
      await receiver.close();
      await context.close();
    }
  }, 15_000);
});
