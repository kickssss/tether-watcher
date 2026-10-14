import { Router, type RequestHandler } from "express";
import { z } from "zod";
import type { Database } from "../db/database";
import { ValidationError } from "../errors";
import { createApiKeyAuth } from "../middleware/auth";
import type { CheckpointRepository } from "../repositories/CheckpointRepository";
import type { DeliveryRepository } from "../repositories/DeliveryRepository";
import type { PlanRepository } from "../repositories/PlanRepository";
import type { WebhookRepository } from "../repositories/WebhookRepository";
import type { Network } from "../types";

const networkSchema = z.enum(["TRC20", "BEP20"]);
const networkFilterSchema = z.enum(["TRC20", "BEP20", "ALL"]);
const statusSchema = z.enum([
  "PENDING",
  "PROCESSING",
  "DELIVERED",
  "FAILED",
  "DEAD_LETTER",
]);

const addressInputSchema = z.object({
  network: networkSchema,
  address: z.string().min(1),
});

const createPlanSchema = z.object({
  id: z.string().min(1).optional(),
  name: z.string().min(1),
  maxWebhooks: z.number().int().min(0),
  maxAddresses: z.number().int().min(0),
  maxDeliverableEvents: z.number().int().min(0),
});

const updatePlanSchema = z
  .object({
    name: z.string().min(1).optional(),
    maxWebhooks: z.number().int().min(0).optional(),
    maxAddresses: z.number().int().min(0).optional(),
    maxDeliverableEvents: z.number().int().min(0).optional(),
    isActive: z.boolean().optional(),
    resetEventQuota: z.boolean().optional(),
  })
  .refine(
    (patch) => Object.values(patch).some((value) => value !== undefined),
    { message: "at least one field must be provided" },
  );

const createWebhookSchema = z.object({
  planId: z.string().min(1),
  url: z.string().url(),
  secret: z.string().min(1).optional(),
  description: z.string().nullable().optional(),
  networkFilter: networkFilterSchema.optional(),
  addresses: z.array(addressInputSchema).max(1000).optional(),
});

const updateWebhookSchema = z
  .object({
    url: z.string().url().optional(),
    secret: z.string().min(1).optional(),
    description: z.string().nullable().optional(),
    networkFilter: networkFilterSchema.optional(),
    isActive: z.boolean().optional(),
    planId: z.string().min(1).optional(),
  })
  .refine(
    (patch) => Object.values(patch).some((value) => value !== undefined),
    { message: "at least one field must be provided" },
  );

const addressesBodySchema = z.object({
  addresses: z.array(addressInputSchema).min(1).max(1000),
});

const webhookDetailQuerySchema = z.object({
  includeAddresses: z.enum(["true", "false"]).optional(),
  limit: z.coerce.number().int().min(1).max(500).default(200),
  cursor: z.string().min(1).optional(),
});

const deliveriesQuerySchema = z.object({
  webhookId: z.string().min(1).optional(),
  planId: z.string().min(1).optional(),
  status: statusSchema.optional(),
  limit: z.coerce.number().int().min(1).max(200).default(50),
  offset: z.coerce.number().int().min(0).default(0),
});

/** Parses with zod and converts failures into `400 VALIDATION_ERROR`. */
function parse<T extends z.ZodTypeAny>(schema: T, input: unknown): z.output<T> {
  const result = schema.safeParse(input);
  if (!result.success) {
    const details = result.error.issues
      .map((issue) => `${issue.path.join(".") || "body"}: ${issue.message}`)
      .join("; ");
    throw new ValidationError(details);
  }
  return result.data;
}

/** Express 4 does not forward rejected promises; this adapts async handlers to `next`. */
function asyncHandler(
  handler: (req: Parameters<RequestHandler>[0], res: Parameters<RequestHandler>[1]) => Promise<void>,
): RequestHandler {
  return (req, res, next) => {
    handler(req, res).catch(next);
  };
}

export interface WatcherRouteDependencies {
  db: Database;
  plans: PlanRepository;
  webhooks: WebhookRepository;
  deliveries: DeliveryRepository;
  checkpoints: CheckpointRepository;
  apiKey: string;
  networks: Array<{ network: Network; enabled: boolean; confirmations: number }>;
  retentionDays: number;
}

/**
 * REST surface (§10). `GET /health` is public; every other route requires `X-API-Key`.
 * A single shared key means there is no per-tenant authorization by design.
 */
export function createWatcherRoutes(deps: WatcherRouteDependencies): Router {
  const router = Router();

  router.get(
    "/health",
    asyncHandler(async (_req, res) => {
      try {
        await deps.db.query("SELECT 1 AS ok");
      } catch {
        res.status(503).json({ status: "degraded", database: "unavailable" });
        return;
      }
      const scanners = [];
      for (const network of deps.networks) {
        const checkpoint = await deps.checkpoints.get(network.network);
        scanners.push({
          network: network.network,
          enabled: network.enabled,
          confirmations: network.confirmations,
          lastProcessedBlock: checkpoint?.lastProcessedBlock ?? null,
          lastBlockHash: checkpoint?.lastBlockHash ?? null,
          leaseOwner: checkpoint?.leaseOwner ?? null,
          leaseExpiresAt: checkpoint?.leaseExpiresAt ?? null,
          leaseActive: Boolean(
            checkpoint?.leaseOwner &&
              checkpoint.leaseExpiresAt &&
              checkpoint.leaseExpiresAt > Date.now(),
          ),
        });
      }
      res.json({ status: "ok", database: "ok", scanners });
    }),
  );

  router.use(createApiKeyAuth(deps.apiKey));

  router.post(
    "/plans",
    asyncHandler(async (req, res) => {
      const input = parse(createPlanSchema, req.body);
      const plan = await deps.plans.create(input);
      res.status(201).json(plan);
    }),
  );

  router.get(
    "/plans/:planId",
    asyncHandler(async (req, res) => {
      res.json(await deps.plans.getById(req.params.planId));
    }),
  );

  router.patch(
    "/plans/:planId",
    asyncHandler(async (req, res) => {
      const patch = parse(updatePlanSchema, req.body);
      res.json(await deps.plans.update(req.params.planId, patch));
    }),
  );

  router.post(
    "/plans/:planId/reconcile",
    asyncHandler(async (req, res) => {
      res.json(await deps.plans.reconcile(req.params.planId));
    }),
  );

  router.post(
    "/webhooks",
    asyncHandler(async (req, res) => {
      const input = parse(createWebhookSchema, req.body);
      const { webhook, createdAddresses } = await deps.webhooks.create(input);
      res.status(201).json({ webhook, createdAddresses });
    }),
  );

  router.get(
    "/webhooks/:webhookId",
    asyncHandler(async (req, res) => {
      const query = parse(webhookDetailQuerySchema, req.query);
      const webhook = await deps.webhooks.getById(req.params.webhookId);
      const addressCount = await deps.webhooks.countAddresses(
        deps.db,
        webhook.id,
      );
      if (query.includeAddresses !== "true") {
        res.json({ webhook, addressCount });
        return;
      }
      const page = await deps.webhooks.listAddresses({
        webhookId: webhook.id,
        limit: query.limit,
        cursor: query.cursor ?? null,
      });
      res.json({
        webhook,
        addressCount,
        addresses: page.addresses,
        nextCursor: page.nextCursor,
      });
    }),
  );

  router.patch(
    "/webhooks/:webhookId",
    asyncHandler(async (req, res) => {
      const patch = parse(updateWebhookSchema, req.body);
      res.json(await deps.webhooks.update(req.params.webhookId, patch));
    }),
  );

  router.delete(
    "/webhooks/:webhookId",
    asyncHandler(async (req, res) => {
      res.json(await deps.webhooks.delete(req.params.webhookId));
    }),
  );

  router.post(
    "/webhooks/:webhookId/addresses",
    asyncHandler(async (req, res) => {
      const body = parse(addressesBodySchema, req.body);
      res.json(await deps.webhooks.addAddresses(req.params.webhookId, body.addresses));
    }),
  );

  router.delete(
    "/webhooks/:webhookId/addresses",
    asyncHandler(async (req, res) => {
      const body = parse(addressesBodySchema, req.body);
      res.json(
        await deps.webhooks.removeAddresses(req.params.webhookId, body.addresses),
      );
    }),
  );

  router.get(
    "/deliveries",
    asyncHandler(async (req, res) => {
      const query = parse(deliveriesQuerySchema, req.query);
      const deliveries = await deps.deliveries.list(query);
      res.json({
        deliveries,
        limit: query.limit,
        offset: query.offset,
      });
    }),
  );

  router.get(
    "/stats",
    asyncHandler(async (_req, res) => {
      const scanners = [];
      for (const network of deps.networks) {
        const checkpoint = await deps.checkpoints.get(network.network);
        scanners.push({
          network: network.network,
          enabled: network.enabled,
          confirmations: network.confirmations,
          lastProcessedBlock: checkpoint?.lastProcessedBlock ?? null,
          lastBlockTimestamp: checkpoint?.lastBlockTimestamp ?? null,
          leaseOwner: checkpoint?.leaseOwner ?? null,
          leaseExpiresAt: checkpoint?.leaseExpiresAt ?? null,
        });
      }
      res.json({
        scanners,
        plans: await deps.plans.stats(),
        deliveries: { byStatus: await deps.deliveries.countByStatus() },
        retention: { days: deps.retentionDays },
      });
    }),
  );

  return router;
}
