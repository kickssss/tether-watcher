import { TronWeb } from "tronweb";
import { ValidationError } from "../errors";
import type { Network } from "../types";

const TRON_BASE58 = /^T[1-9A-HJ-NP-Za-km-z]{33}$/;
const EVM_HEX = /^0x[0-9a-f]{40}$/;
const TRON_HEX = /^(41)?[0-9a-fA-F]{40}$/;

/**
 * Normalizes an address to its canonical indexed form (§6.2):
 * - BEP20: lowercase 42-char hex (no EIP-55 checksum casing).
 * - TRC20: 34-char Base58Check (`T...`), converting `41`-prefixed topic hex.
 */
export function normalizeAddress(network: Network, raw: string): string {
  const value = raw.trim();

  if (network === "BEP20") {
    const hex = value.toLowerCase();
    if (!EVM_HEX.test(hex)) {
      throw new ValidationError(`invalid BEP20 address: ${raw}`);
    }
    return hex;
  }

  if (TRON_BASE58.test(value)) return value;
  if (TRON_HEX.test(value)) {
    try {
      return TronWeb.address.fromHex(value.length === 40 ? `41${value}` : value);
    } catch {
      throw new ValidationError(`invalid TRC20 address: ${raw}`);
    }
  }
  throw new ValidationError(`invalid TRC20 address: ${raw}`);
}
