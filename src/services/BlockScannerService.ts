import type { Logger } from "winston";
import type { Database } from "../db/database";
import { LeaseUnavailableError, ReorgDetectedError, errorMessage } from "../errors";
import {
  LEASE_RENEW_INTERVAL_MS,
  type CheckpointRepository,
} from "../repositories/CheckpointRepository";
import type { DeliveryRepository, NewDelivery } from "../repositories/DeliveryRepository";
import { findBlockMatches, type AddressMatch } from "../repositories/MatchRepository";
import type { PlanRepository } from "../repositories/PlanRepository";
import type {
  BlockData,
  Direction,
  EventPayload,
  Network,
  TransferEvent,
} from "../types";
import { formatUnits } from "../utils/amount";
import { sleep } from "../utils/async";
import { generateId } from "../utils/id";
import type { BlockProvider } from "./providers/BlockProvider";

export interface ScannerOptions {
  network: Network;
  confirmations: number;
  tokenContract: string;
  decimals: number;
  startBlock: number | null;
  idleIntervalMs: number;
  instanceId: string;
  networkMode: "mainnet" | "testnet";
}

export interface ScannerDependencies {
  db: Database;
  checkpoints: CheckpointRepository;
  deliveries: DeliveryRepository;
  plans: PlanRepository;
  logger: Logger;
  /** Invoked when the scanner stops itself (lease lost or deep reorg). */
  onFatal?: (error: Error) => void;
}

export interface CommitSummary {
  blockNumber: number;
  queued: number;
  dropped: number;
  duplicates: number;
}

interface TransferSide {
  address: string;
  direction: Direction;
}

/**
 * A self-transfer (`from === to`) yields one INCOMING match: emitting both directions
 * would collide on the delivery UNIQUE key while consuming quota twice.
 */
function transferSides(transfer: TransferEvent): TransferSide[] {
  if (transfer.from === transfer.to) {
    return [{ address: transfer.to, direction: "INCOMING" }];
  }
  return [
    { address: transfer.to, direction: "INCOMING" },
    { address: transfer.from, direction: "OUTGOING" },
  ];
}

function buildDelivery(params: {
  block: BlockData;
  transfer: TransferEvent;
  direction: Direction;
  match: AddressMatch;
  options: ScannerOptions;
}): NewDelivery {
  const { block, transfer, direction, match, options } = params;
  const id = generateId("del");
  const amountDecimal = formatUnits(transfer.amountRaw, options.decimals);
  const payload: EventPayload = {
    eventId: id,
    eventType: "USDT_TRANSFER",
    network: options.network,
    networkMode: options.networkMode,
    webhookId: match.webhookId,
    planId: match.planId,
    matchedAddress: match.address,
    direction,
    transaction: {
      txHash: transfer.txHash,
      logIndex: transfer.logIndex,
      blockNumber: block.number,
      blockTimestamp: block.timestamp,
      fromAddress: transfer.from,
      toAddress: transfer.to,
      tokenContract: options.tokenContract,
      tokenSymbol: "USDT",
      decimals: options.decimals,
      amountRaw: transfer.amountRaw,
      amountDecimal,
    },
    createdAt: new Date().toISOString(),
  };

  return {
    id,
    planId: match.planId,
    webhookId: match.webhookId,
    network: options.network,
    blockNumber: block.number,
    txHash: transfer.txHash,
    logIndex: transfer.logIndex,
    fromAddress: transfer.from,
    toAddress: transfer.to,
    matchedAddress: match.address,
    direction,
    amountRaw: transfer.amountRaw,
    amountDecimal,
    tokenContract: options.tokenContract,
    blockTimestamp: block.timestamp,
    payloadJson: JSON.stringify(payload),
    nextAttemptAt: Date.now(),
  };
}

/**
 * Subsystem A (§3): one sequential scan loop per network. Holds the network's scanner
 * lease, walks block by block from the committed checkpoint, and commits quota
 * accounting, outbox rows, and the new checkpoint in a single transaction (§8.2).
 */
export class BlockScannerService {
  private running = false;
  private loopPromise: Promise<void> | null = null;
  private heartbeat: NodeJS.Timeout | null = null;
  private fatal: Error | null = null;

  constructor(
    private readonly options: ScannerOptions,
    private readonly provider: BlockProvider,
    private readonly deps: ScannerDependencies,
  ) {}

  get fatalError(): Error | null {
    return this.fatal;
  }

  /** Acquires the lease and starts the background loop. Throws if another scanner is live. */
  async start(): Promise<void> {
    const { network, instanceId } = this.options;
    if (this.running) throw new Error(`${network}: scanner is already running`);

    const initialBlock = await this.resolveInitialBlock();
    const acquired = await this.deps.checkpoints.acquireLease({
      network,
      instanceId,
      initialBlock,
    });
    if (!acquired) {
      const checkpoint = await this.deps.checkpoints.get(network);
      throw new LeaseUnavailableError(network, checkpoint?.leaseOwner ?? null);
    }

    this.running = true;
    this.heartbeat = setInterval(() => {
      void this.renewLease();
    }, LEASE_RENEW_INTERVAL_MS);
    this.loopPromise = this.runLoop().finally(() => {
      this.loopPromise = null;
    });

    this.deps.logger.info("scanner started", {
      network,
      instanceId,
      initialBlock,
      scanningFrom: initialBlock + 1,
      confirmations: this.options.confirmations,
    });
  }

  /** Stops the loop and releases the lease. Safe to call when not running. */
  async stop(): Promise<void> {
    if (this.heartbeat) {
      clearInterval(this.heartbeat);
      this.heartbeat = null;
    }
    this.running = false;
    await this.loopPromise;
    await this.deps.checkpoints.releaseLease(
      this.options.network,
      this.options.instanceId,
    );
    this.deps.logger.info("scanner stopped", { network: this.options.network });
  }

  /** First boot: `START_BLOCK - 1`, else the confirmation-safe head (§8.1). */
  private async resolveInitialBlock(): Promise<number> {
    const existing = await this.deps.checkpoints.get(this.options.network);
    if (existing) return existing.lastProcessedBlock;
    if (this.options.startBlock !== null) {
      return Math.max(0, this.options.startBlock - 1);
    }
    const latest = await this.provider.getLatestBlockNumber();
    return Math.max(0, latest - this.options.confirmations);
  }

  private async runLoop(): Promise<void> {
    while (this.running) {
      try {
        await this.scanOnce();
      } catch (error) {
        if (error instanceof ReorgDetectedError) {
          this.fail(error);
          return;
        }
        this.deps.logger.error(
          "scan iteration failed; the same block will be retried",
          { network: this.options.network, error: errorMessage(error) },
        );
        await sleep(this.options.idleIntervalMs);
      }
    }
  }

  private async scanOnce(): Promise<CommitSummary | null> {
    const { network, confirmations, idleIntervalMs } = this.options;
    const checkpoint = await this.deps.checkpoints.get(network);
    if (!checkpoint) {
      throw new Error(`${network}: checkpoint row missing after lease acquisition`);
    }

    const nextBlock = checkpoint.lastProcessedBlock + 1;
    const safeTarget = (await this.provider.getLatestBlockNumber()) - confirmations;
    if (nextBlock > safeTarget) {
      await sleep(idleIntervalMs);
      return null;
    }

    const block = await this.provider.getBlock(nextBlock);
    if (checkpoint.lastBlockHash && block.parentHash !== checkpoint.lastBlockHash) {
      throw new ReorgDetectedError(
        network,
        block.number,
        checkpoint.lastBlockHash,
        block.parentHash,
      );
    }

    const summary = await this.commitBlock(block);
    this.deps.logger.info("block committed", {
      network,
      ...summary,
    });
    return summary;
  }

  /**
   * §8.2: address matching, quota accounting, outbox inserts, and the checkpoint
   * advance all commit or roll back together.
   */
  async commitBlock(block: BlockData): Promise<CommitSummary> {
    const { network } = this.options;
    const blockAddresses = [
      ...new Set(block.transfers.flatMap((transfer) => [transfer.from, transfer.to])),
    ];

    return this.deps.db.write(async (tx) => {
      const matches = await findBlockMatches(tx, network, blockAddresses);
      const matchesByAddress = new Map<string, AddressMatch[]>();
      for (const match of matches) {
        const existing = matchesByAddress.get(match.address);
        if (existing) existing.push(match);
        else matchesByAddress.set(match.address, [match]);
      }

      let queued = 0;
      let dropped = 0;
      let duplicates = 0;

      for (const transfer of block.transfers) {
        for (const side of transferSides(transfer)) {
          for (const match of matchesByAddress.get(side.address) ?? []) {
            const alreadyQueued = await this.deps.deliveries.exists(tx, {
              webhookId: match.webhookId,
              network,
              txHash: transfer.txHash,
              logIndex: transfer.logIndex,
              matchedAddress: match.address,
            });
            if (alreadyQueued) {
              duplicates++;
              continue;
            }

            const consumed = await this.deps.plans.tryConsumeEventQuota(
              tx,
              match.planId,
            );
            if (!consumed) {
              await this.deps.plans.incrementDroppedEvents(tx, match.planId);
              dropped++;
              continue;
            }

            await this.deps.deliveries.insert(
              tx,
              buildDelivery({
                block,
                transfer,
                direction: side.direction,
                match,
                options: this.options,
              }),
            );
            queued++;
          }
        }
      }

      await this.deps.checkpoints.commitBlock(tx, {
        network,
        blockNumber: block.number,
        blockHash: block.hash,
        blockTimestamp: block.timestamp,
      });

      return { blockNumber: block.number, queued, dropped, duplicates };
    });
  }

  private async renewLease(): Promise<void> {
    if (!this.running) return;
    try {
      const renewed = await this.deps.checkpoints.renewLease(
        this.options.network,
        this.options.instanceId,
      );
      if (!renewed) {
        this.fail(new LeaseUnavailableError(this.options.network, null));
      }
    } catch (error) {
      this.deps.logger.error("scanner lease renewal failed", {
        network: this.options.network,
        error: errorMessage(error),
      });
    }
  }

  private fail(error: Error): void {
    this.fatal = error;
    this.running = false;
    this.deps.logger.error("CRITICAL: scanner stopping", {
      network: this.options.network,
      error: error.message,
    });
    this.deps.onFatal?.(error);
  }
}
