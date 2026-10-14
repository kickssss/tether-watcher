import { createHmac } from "crypto";

/** HMAC-SHA256 hex signature over the exact JSON bytes that are POSTed (§9.2). */
export function generateSignature(payloadJson: string, secret: string): string {
  return createHmac("sha256", secret).update(payloadJson, "utf8").digest("hex");
}
