/**
 * Typed application errors. Every error carries a stable machine-readable `code`
 * and the HTTP status the API must return; anything else becomes a 500.
 */

export class AppError extends Error {
  constructor(
    message: string,
    readonly code: string,
    readonly httpStatus: number,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

export class NotFoundError extends AppError {
  constructor(what: string, id: string) {
    super(`${what} not found: ${id}`, "NOT_FOUND", 404);
  }
}

export class ValidationError extends AppError {
  constructor(message: string) {
    super(message, "VALIDATION_ERROR", 400);
  }
}

export class PlanInactiveError extends AppError {
  constructor(planId: string) {
    super(`subscription plan is not active: ${planId}`, "PLAN_INACTIVE", 403);
  }
}

export class WebhookQuotaExceededError extends AppError {
  constructor(planId: string) {
    super(
      `plan ${planId} has reached max_webhooks`,
      "WEBHOOK_QUOTA_EXCEEDED",
      403,
    );
  }
}

export class AddressQuotaExceededError extends AppError {
  constructor(planId: string) {
    super(
      `plan ${planId} would exceed max_addresses`,
      "ADDRESS_QUOTA_EXCEEDED",
      403,
    );
  }
}

export class AddressNetworkMismatchError extends AppError {
  constructor(address: string, network: string, filter: string) {
    super(
      `address ${address} (${network}) is incompatible with webhook network_filter ${filter}`,
      "ADDRESS_NETWORK_MISMATCH",
      400,
    );
  }
}

export class NetworkFilterConflictError extends AppError {
  constructor(readonly addresses: string[]) {
    super(
      `cannot narrow network_filter: ${addresses.length} assigned address(es) belong to the excluded network`,
      "NETWORK_FILTER_CONFLICT",
      409,
    );
  }
}

/** The network already has a live scanner instance (lease is held elsewhere). */
export class LeaseUnavailableError extends AppError {
  constructor(network: string, owner: string | null) {
    super(
      `scanner lease for ${network} is held by ${owner ?? "another instance"}`,
      "SCANNER_LEASE_UNAVAILABLE",
      409,
    );
  }
}

/** Chain reorganized deeper than the last processed block (§8.3). */
export class ReorgDetectedError extends AppError {
  constructor(
    readonly network: string,
    readonly blockNumber: number,
    readonly expectedParentHash: string,
    readonly actualParentHash: string,
  ) {
    super(
      `reorg detected on ${network} at block ${blockNumber}: expected parent ${expectedParentHash}, got ${actualParentHash}`,
      "REORG_DETECTED",
      500,
    );
  }
}

/** Human-readable message for values caught from `unknown` (3+ call sites). */
export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** The upstream chain provider failed after exhausting retries. */
export class ProviderError extends AppError {
  constructor(message: string, readonly cause?: unknown) {
    super(message, "PROVIDER_ERROR", 503);
  }
}
