import { randomUUID } from "crypto";

/** Prefixed UUIDv4 identifiers, e.g. `wh_1d94c0a7-...`. */
export const generateId = (prefix: "plan" | "wh" | "del"): string =>
  `${prefix}_${randomUUID()}`;
