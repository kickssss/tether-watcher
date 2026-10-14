import type { Express } from "express";
import expressWinston from "express-winston";
import winston, { format, transports, type Logger } from "winston";

export interface LoggerOptions {
  level: string;
  /** File transports are for long-running services; tests keep the console silent. */
  toFiles: boolean;
  silent?: boolean;
}

export function createAppLogger(options: LoggerOptions): Logger {
  const consoleTransport = new transports.Console();
  return winston.createLogger({
    level: options.level,
    silent: options.silent ?? false,
    format: format.combine(format.timestamp(), format.json()),
    transports: options.toFiles
      ? [
          consoleTransport,
          new transports.File({ filename: "error.log", level: "error" }),
          new transports.File({ filename: "combined.log" }),
        ]
      : [consoleTransport],
  });
}

export function setupLogging(app: Express, logger: Logger): void {
  app.use(
    expressWinston.logger({
      winstonInstance: logger,
      meta: true,
      msg: "HTTP {{req.method}} {{req.url}}",
      expressFormat: true,
      colorize: false,
    }),
  );
}
