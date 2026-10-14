/** Core domain types shared across the persistence, scanning, and delivery layers. */

export type Network = "TRC20" | "BEP20";

export const NETWORKS: readonly Network[] = ["TRC20", "BEP20"];

export type NetworkFilter = Network | "ALL";

export type Direction = "INCOMING" | "OUTGOING";

export type DeliveryStatus =
  | "PENDING"
  | "PROCESSING"
  | "DELIVERED"
  | "FAILED"
  | "DEAD_LETTER";

/** A normalized token transfer extracted from a block. Addresses are already normalized (§6.2). */
export interface TransferEvent {
  txHash: string;
  logIndex: number;
  from: string;
  to: string;
  /** Raw integer amount in the token's smallest unit, as a decimal string. */
  amountRaw: string;
}

/** A fully fetched block: identity for the §8.3 continuity check plus its token transfers. */
export interface BlockData {
  number: number;
  hash: string;
  parentHash: string;
  /** Block time in milliseconds since epoch. */
  timestamp: number;
  transfers: TransferEvent[];
}

export interface SubscriptionPlan {
  id: string;
  name: string;
  maxWebhooks: number;
  maxAddresses: number;
  maxDeliverableEvents: number;
  usedWebhooksCount: number;
  usedAddressesCount: number;
  deliveredEventsCount: number;
  droppedEventsCount: number;
  isActive: boolean;
  createdAt: number;
  updatedAt: number;
}

export interface Webhook {
  id: string;
  planId: string;
  url: string;
  secret: string;
  description: string | null;
  networkFilter: NetworkFilter;
  isActive: boolean;
  createdAt: number;
  updatedAt: number;
}

export interface WatchedAddress {
  network: Network;
  address: string;
  webhookId: string;
  planId: string;
  createdAt: number;
}

export interface BlockCheckpoint {
  network: Network;
  lastProcessedBlock: number;
  lastBlockHash: string | null;
  lastBlockTimestamp: number | null;
  leaseOwner: string | null;
  leaseExpiresAt: number | null;
  updatedAt: number;
}

export interface DeliveryRecord {
  id: string;
  planId: string;
  webhookId: string;
  network: Network;
  blockNumber: number;
  txHash: string;
  logIndex: number;
  fromAddress: string;
  toAddress: string;
  matchedAddress: string;
  direction: Direction;
  amountRaw: string;
  amountDecimal: string;
  tokenContract: string;
  blockTimestamp: number;
  payloadJson: string;
  status: DeliveryStatus;
  attemptCount: number;
  maxAttempts: number;
  nextAttemptAt: number;
  lastHttpStatus: number | null;
  lastError: string | null;
  deliveredAt: number | null;
  createdAt: number;
  updatedAt: number;
}

/** Standardized `payload_json` body POSTed to webhooks (§9.1). */
export interface EventPayload {
  eventId: string;
  eventType: "USDT_TRANSFER";
  network: Network;
  networkMode: "mainnet" | "testnet";
  webhookId: string;
  planId: string;
  matchedAddress: string;
  direction: Direction;
  transaction: {
    txHash: string;
    logIndex: number;
    blockNumber: number;
    blockTimestamp: number;
    fromAddress: string;
    toAddress: string;
    tokenContract: string;
    tokenSymbol: "USDT";
    decimals: number;
    amountRaw: string;
    amountDecimal: string;
  };
  createdAt: string;
}
