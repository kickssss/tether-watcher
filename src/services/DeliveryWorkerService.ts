import type { Logger } from "winston";
import { errorMessage } from "../errors";
import type { ClaimedDelivery, DeliveryRepository } from "../repositories/DeliveryRepository";
import { dispatchWebhook, type DispatchResult } from "../utils/webhook";
import { sleep } from "../utils/async";

/** Rows stranded in PROCESSING longer than this are requeued (§9.2 step 4). */
const STALE_PROCESSING_MS = 60_000;
const MAINTENANCE_INTERVAL_MS = 60_000;
/** Exponential backoff ceiling: base · 2^attempt + jitter, capped at one hour (§9.2). */
const MAX_BACKOFF_MS = 3_600_000;

export interface WorkerOptions {
  pollIntervalMs: number;
  batchSize: number;
  concurrency: number;
  httpTimeoutMs: number;
  /** First retry delay; doubles per attempt up to the ceiling (§9.2). */
  backoffBaseMs: number;
  retentionDays: number;
  pruneIntervalMs: number;
}

/** Retryable: request timeouts, rate limits, and server-side failures (§9.2 step 3). */
function isRetryable(result: DispatchResult): boolean {
  if (result.status === null) return true; // network/timeout error
  return result.status === 408 || result.status === 429 || result.status >= 500;
}

function backoffDelayMs(baseMs: number, attemptCount: number): number {
  return (
    Math.min(MAX_BACKOFF_MS, baseMs * 2 ** attemptCount) +
    Math.floor(Math.random() * 1_000)
  );
}

/**
 * Subsystem B (§3): claims due deliveries atomically, signs and POSTs them, and
 * advances each row through the retry state machine. Delivery is at-least-once:
 * a crash between a 2xx response and the `DELIVERED` update re-sends the event (§9.2).
 */
export class DeliveryWorkerService {
  private running = false;
  private loopPromise: Promise<void> | null = null;
  private maintenance: NodeJS.Timeout | null = null;
  private lastPruneAt = 0;

  constructor(
    private readonly options: WorkerOptions,
    private readonly deliveries: DeliveryRepository,
    private readonly logger: Logger,
  ) {}

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    await this.deliveries.recoverStaleProcessing(Date.now() - STALE_PROCESSING_MS);
    this.maintenance = setInterval(() => {
      void this.runMaintenance();
    }, MAINTENANCE_INTERVAL_MS);
    this.loopPromise = this.runLoop().finally(() => {
      this.loopPromise = null;
    });
    this.logger.info("delivery worker started", {
      pollIntervalMs: this.options.pollIntervalMs,
      batchSize: this.options.batchSize,
      concurrency: this.options.concurrency,
    });
  }

  async stop(): Promise<void> {
    if (this.maintenance) {
      clearInterval(this.maintenance);
      this.maintenance = null;
    }
    this.running = false;
    await this.loopPromise;
    this.logger.info("delivery worker stopped");
  }

  private async runLoop(): Promise<void> {
    while (this.running) {
      try {
        const claimed = await this.deliveries.claimDue(this.options.batchSize);
        if (claimed.length === 0) {
          await sleep(this.options.pollIntervalMs);
          continue;
        }
        await this.dispatchBatch(claimed);
      } catch (error) {
        this.logger.error("delivery worker iteration failed", {
          error: errorMessage(error),
        });
        await sleep(this.options.pollIntervalMs);
      }
    }
  }

  /** Stale-PROCESSING recovery every minute; retention pruning on its own cadence. */
  private async runMaintenance(): Promise<void> {
    try {
      const recovered = await this.deliveries.recoverStaleProcessing(
        Date.now() - STALE_PROCESSING_MS,
      );
      if (recovered > 0) {
        this.logger.warn("requeued stale PROCESSING deliveries", { recovered });
      }

      if (Date.now() - this.lastPruneAt >= this.options.pruneIntervalMs) {
        this.lastPruneAt = Date.now();
        const cutoff =
          Date.now() - this.options.retentionDays * 24 * 60 * 60 * 1000;
        const pruned = await this.deliveries.prune(cutoff);
        if (pruned > 0) {
          this.logger.info("pruned terminal deliveries", {
            pruned,
            retentionDays: this.options.retentionDays,
          });
        }
        // Keep planner statistics fresh after bulk deletes (§4.2).
        await this.deliveries.optimizePlannerStats();
      }
    } catch (error) {
      this.logger.error("delivery worker maintenance failed", {
        error: errorMessage(error),
      });
    }
  }

  private async dispatchBatch(batch: ClaimedDelivery[]): Promise<void> {
    for (let index = 0; index < batch.length; index += this.options.concurrency) {
      const chunk = batch.slice(index, index + this.options.concurrency);
      await Promise.all(chunk.map((delivery) => this.dispatch(delivery)));
    }
  }

  private async dispatch(delivery: ClaimedDelivery): Promise<void> {
    if (!delivery.webhookUrl || !delivery.webhookSecret) {
      await this.deliveries.markDeadLetter(delivery.id, {
        httpStatus: null,
        error: "webhook no longer exists",
      });
      return;
    }

    const result = await dispatchWebhook({
      url: delivery.webhookUrl,
      secret: delivery.webhookSecret,
      payloadJson: delivery.payloadJson,
      deliveryId: delivery.id,
      webhookId: delivery.webhookId,
      attempt: delivery.attemptCount,
      timeoutMs: this.options.httpTimeoutMs,
    });

    if (result.success) {
      await this.deliveries.markDelivered(delivery.id, result.status ?? 200);
      return;
    }

    if (isRetryable(result) && delivery.attemptCount < delivery.maxAttempts) {
      await this.deliveries.markRetry(delivery.id, {
        nextAttemptAt: Date.now() + backoffDelayMs(this.options.backoffBaseMs, delivery.attemptCount),
        httpStatus: result.status,
        error: result.error,
      });
      return;
    }

    await this.deliveries.markDeadLetter(delivery.id, {
      httpStatus: result.status,
      error: result.error,
    });
    this.logger.warn("delivery dead-lettered", {
      deliveryId: delivery.id,
      webhookId: delivery.webhookId,
      attempts: delivery.attemptCount,
      httpStatus: result.status,
      error: result.error,
    });
  }
}
