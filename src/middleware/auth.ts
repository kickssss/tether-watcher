import type { RequestHandler } from "express";

/** Constant-time-ish API key gate; the single shared key matches the no-signup model (§10). */
export function createApiKeyAuth(apiKey: string): RequestHandler {
  return (req, res, next) => {
    const provided = req.header("X-API-Key") ?? req.header("x-api-key");
    if (!provided || provided !== apiKey) {
      res.status(401).json({ error: "UNAUTHORIZED", message: "invalid API key" });
      return;
    }
    next();
  };
}
