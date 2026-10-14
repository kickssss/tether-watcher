/**
 * Exact token-amount helpers built on BigInt (no precision loss, no dependency).
 */

/** Canonicalizes a raw integer amount: `"0001005"` -> `"1005"`. Throws on non-integers. */
export function normalizeRawAmount(raw: string | number | bigint): string {
  const value = typeof raw === "string" ? raw.trim() : String(raw);
  if (!/^\d+$/.test(value)) {
    throw new Error(`invalid raw token amount: ${raw}`);
  }
  return BigInt(value).toString();
}

/** Formats a raw integer amount as a decimal string: `("100500000", 6)` -> `"100.5"`. */
export function formatUnits(raw: string, decimals: number): string {
  const normalized = normalizeRawAmount(raw);
  if (decimals === 0) return normalized;
  const padded = normalized.padStart(decimals + 1, "0");
  const whole = padded.slice(0, padded.length - decimals);
  const fraction = padded.slice(padded.length - decimals).replace(/0+$/, "");
  return fraction ? `${whole}.${fraction}` : whole;
}
