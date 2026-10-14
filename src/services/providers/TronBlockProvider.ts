import axios, { type AxiosInstance } from "axios";
import { ProviderError } from "../../errors";
import type { BlockData, Network, TransferEvent } from "../../types";
import { normalizeAddress } from "../../utils/addressNormalizer";
import { normalizeRawAmount } from "../../utils/amount";
import { type BlockProvider, withProviderErrors } from "./BlockProvider";

const PAGE_LIMIT = 200;

interface TronGridEvent {
  block_number: number;
  transaction_id: string;
  event_index?: number;
  result: { from?: string; to?: string; value?: string };
}

interface TronGridEventsResponse {
  data?: TronGridEvent[];
  meta?: { fingerprint?: string };
}

interface TronBlockResponse {
  blockID?: string;
  block_header?: {
    raw_data?: { number?: number; parentHash?: string; timestamp?: number };
  };
}

/**
 * TRC20 + USDT via TronGrid (§11 provider contract):
 * one height per request (`min_block_number == max_block_number`), `only_confirmed: false`
 * because finality is enforced by the scanner's confirmation depth, `fingerprint`
 * paging until exhausted, and a hard assertion that every returned event belongs
 * to the requested height.
 */
export class TronBlockProvider implements BlockProvider {
  readonly network: Network = "TRC20";
  private readonly http: AxiosInstance;

  constructor(private readonly tokenContract: string, url: string, apiKey: string | null) {
    this.http = axios.create({
      baseURL: url,
      timeout: 15_000,
      headers: apiKey ? { "TRON-PRO-API-KEY": apiKey } : {},
    });
  }

  async getLatestBlockNumber(): Promise<number> {
    return withProviderErrors("TRC20 getnowblock", async () => {
      const response = await this.http.post<TronBlockResponse>("/wallet/getnowblock");
      const number = response.data?.block_header?.raw_data?.number;
      if (typeof number !== "number") {
        throw new ProviderError("TRC20 getnowblock returned no block number");
      }
      return number;
    });
  }

  async getBlock(blockNumber: number): Promise<BlockData> {
    const [identity, transfers] = await Promise.all([
      this.fetchBlockIdentity(blockNumber),
      this.fetchTransfers(blockNumber),
    ]);
    return {
      number: blockNumber,
      hash: identity.hash,
      parentHash: identity.parentHash,
      timestamp: identity.timestamp,
      transfers,
    };
  }

  private async fetchBlockIdentity(
    blockNumber: number,
  ): Promise<{ hash: string; parentHash: string; timestamp: number }> {
    return withProviderErrors(`TRC20 getblockbynum ${blockNumber}`, async () => {
      const response = await this.http.post<TronBlockResponse>(
        "/wallet/getblockbynum",
        { num: blockNumber },
      );
      const raw = response.data?.block_header?.raw_data;
      if (!raw || raw.number !== blockNumber || !response.data?.blockID) {
        throw new ProviderError(`TRC20 block ${blockNumber} identity is incomplete`);
      }
      return {
        hash: response.data.blockID.replace(/^0x/, "").toLowerCase(),
        parentHash: (raw.parentHash ?? "").replace(/^0x/, "").toLowerCase(),
        timestamp: raw.timestamp ?? 0,
      };
    });
  }

  private async fetchTransfers(blockNumber: number): Promise<TransferEvent[]> {
    return withProviderErrors(`TRC20 transfer events ${blockNumber}`, async () => {
      const transfers: TransferEvent[] = [];
      let fingerprint: string | undefined;
      let pageIndex = 0;

      do {
        const response = await this.http.get<TronGridEventsResponse>(
          `/v1/contracts/${this.tokenContract}/events`,
          {
            params: {
              event_name: "Transfer",
              min_block_number: blockNumber,
              max_block_number: blockNumber,
              only_confirmed: "false",
              limit: PAGE_LIMIT,
              ...(fingerprint ? { fingerprint } : {}),
            },
          },
        );

        for (const event of response.data?.data ?? []) {
          if (event.block_number !== blockNumber) {
            throw new ProviderError(
              `TRC20 event for block ${event.block_number} returned while fetching ${blockNumber}`,
            );
          }
          const from = event.result?.from;
          const to = event.result?.to;
          const value = event.result?.value;
          if (!from || !to || value === undefined) {
            throw new ProviderError(
              `TRC20 Transfer ${event.transaction_id} is missing from/to/value`,
            );
          }
          transfers.push({
            txHash: event.transaction_id.toLowerCase(),
            logIndex: typeof event.event_index === "number" ? event.event_index : pageIndex,
            from: normalizeAddress("TRC20", from),
            to: normalizeAddress("TRC20", to),
            amountRaw: normalizeRawAmount(value),
          });
          pageIndex++;
        }

        fingerprint = response.data?.meta?.fingerprint;
      } while (fingerprint);

      return transfers;
    });
  }
}
