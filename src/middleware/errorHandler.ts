import type { ErrorRequestHandler } from "express";
import type { Logger } from "winston";
import { isConstraintError } from "../db/database";
import { AppError, errorMessage } from "../errors";

/** Maps `AppError` to its HTTP status/code, constraints to 409, everything else to 500. */
export function createErrorHandler(logger: Logger): ErrorRequestHandler {
  return (error, _req, res, _next) => {
    if (error instanceof AppError) {
      const context = { code: error.code };
      if (error.httpStatus >= 500) logger.error(error.message, context);
      else logger.warn(error.message, context);
      res.status(error.httpStatus).json({
        error: error.code,
        message: error.message,
      });
      return;
    }

    if (isConstraintError(error)) {
      res.status(409).json({
        error: "CONFLICT",
        message: "resource already exists or violates a database constraint",
      });
      return;
    }

    logger.error(errorMessage(error));
    res.status(500).json({
      error: "INTERNAL_ERROR",
      message: "An unexpected error occurred",
    });
  };
}
