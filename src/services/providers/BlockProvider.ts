import { ProviderError } from "../../errors";
import type { BlockData, Network } from "../../types";

/** A chain-specific source of blocks and their USDT transfers. */
export interface BlockProvider {
  readonly network: Network;
  /** Height of the provider's chain head (used to compute the confirmation-safe target). */
  getLatestBlockNumber(): Promise<number>;
  /**
   * Fetches exactly one block: identity (`hash`, `parentHash`, `timestamp`) for the
   * §8.3 continuity check plus every USDT `Transfer` in that block. Must never
   * return a partial result — throw and let the scanner retry the same height.
   */
  getBlock(blockNumber: number): Promise<BlockData>;
}

/** Wraps provider calls so upstream failures surface as `ProviderError` with context. */
export async function withProviderErrors<T>(
  label: string,
  call: () => Promise<T>,
): Promise<T> {
  try {
    return await call();
  } catch (error) {
    if (error instanceof ProviderError) throw error;
    const message = error instanceof Error ? error.message : String(error);
    throw new ProviderError(`${label}: ${message}`, error);
  }
}
