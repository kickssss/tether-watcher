import axios from "axios";
import { generateSignature } from "./generateSignature";

export interface DispatchResult {
  success: boolean;
  status: number | null;
  error: string | null;
}

export interface DispatchParams {
  url: string;
  secret: string;
  payloadJson: string;
  deliveryId: string;
  webhookId: string;
  attempt: number;
  timeoutMs: number;
}

/**
 * POSTs a stored delivery payload and reports the outcome to the worker state machine.
 * The signature covers the exact request body; HTTP 4xx/5xx responses are returned as
 * results (not thrown) so the worker can apply retry policy deterministically.
 */
export async function dispatchWebhook(
  params: DispatchParams,
): Promise<DispatchResult> {
  const { url, secret, payloadJson, deliveryId, webhookId, attempt, timeoutMs } =
    params;

  try {
    const response = await axios.post(url, payloadJson, {
      timeout: timeoutMs,
      headers: {
        "Content-Type": "application/json",
        "X-Webhook-Signature": generateSignature(payloadJson, secret),
        "X-Webhook-Id": webhookId,
        "X-Delivery-Id": deliveryId,
        "X-Delivery-Attempt": String(attempt),
      },
      // Sign and send the identical bytes: no axios JSON re-serialization.
      transformRequest: [(data: string) => data],
      validateStatus: () => true,
    });

    const success = response.status >= 200 && response.status < 300;
    return {
      success,
      status: response.status,
      error: success ? null : `HTTP ${response.status}`,
    };
  } catch (error) {
    if (axios.isAxiosError(error)) {
      return {
        success: false,
        status: error.response?.status ?? null,
        error: error.code ?? error.message,
      };
    }
    return {
      success: false,
      status: null,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}
