import { JsonRpcProvider, type Log } from "ethers";
import { ProviderError } from "../../errors";
import type { BlockData, Network, TransferEvent } from "../../types";
import { normalizeAddress } from "../../utils/addressNormalizer";
import { normalizeRawAmount } from "../../utils/amount";
import { type BlockProvider, withProviderErrors } from "./BlockProvider";

/** keccak256("Transfer(address,address,uint256)") */
const TRANSFER_TOPIC =
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/** A 32-byte topic holds a 20-byte address in its last 40 hex characters. */
const ADDRESS_TOPIC_TRAILING_HEX = 40;

function topicToAddress(topic: string | undefined, label: string): string {
  const value = topic?.replace(/^0x/, "") ?? "";
  if (value.length !== 64) {
    throw new ProviderError(`BEP20 ${label} topic is malformed: ${topic ?? "missing"}`);
  }
  return normalizeAddress("BEP20", `0x${value.slice(-ADDRESS_TOPIC_TRAILING_HEX)}`);
}

/**
 * BEP20 + USDT via a JSON-RPC endpoint (§11 provider contract): `getBlock(N)` for
 * identity and `eth_getLogs` restricted to the single height `N`, which avoids the
 * block-range and result caps public endpoints impose. A failed fetch throws so the
 * scanner retries the same height instead of advancing the checkpoint.
 */
export class BscBlockProvider implements BlockProvider {
  readonly network: Network = "BEP20";
  private readonly provider: JsonRpcProvider;

  constructor(private readonly tokenContract: string, rpcUrl: string) {
    this.provider = new JsonRpcProvider(rpcUrl);
  }

  async getLatestBlockNumber(): Promise<number> {
    return withProviderErrors("BEP20 eth_blockNumber", () =>
      this.provider.getBlockNumber(),
    );
  }

  async getBlock(blockNumber: number): Promise<BlockData> {
    return withProviderErrors(`BEP20 block ${blockNumber}`, async () => {
      const block = await this.provider.getBlock(blockNumber);
      if (!block || !block.hash) {
        throw new ProviderError(`BEP20 block ${blockNumber} is unavailable`);
      }

      const logs = await this.provider.getLogs({
        address: this.tokenContract,
        topics: [TRANSFER_TOPIC],
        fromBlock: blockNumber,
        toBlock: blockNumber,
      });

      const transfers: TransferEvent[] = logs.map((log: Log) => ({
        txHash: log.transactionHash.toLowerCase(),
        logIndex: log.index,
        from: topicToAddress(log.topics[1], "from"),
        to: topicToAddress(log.topics[2], "to"),
        amountRaw: normalizeRawAmount(BigInt(log.data).toString()),
      }));

      return {
        number: block.number,
        hash: block.hash.toLowerCase(),
        parentHash: block.parentHash.toLowerCase(),
        timestamp: block.timestamp * 1000,
        transfers,
      };
    });
  }
}
