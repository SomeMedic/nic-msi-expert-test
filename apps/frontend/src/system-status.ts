import { systemStatusStatusValues } from "./api/generated";
import type { components } from "./api/generated";

export const statusLabels = {
  ready: "Доступен",
  degraded: "Доступен частично",
  unavailable: "Недоступен",
} satisfies Record<components["schemas"]["SystemStatus"]["status"], string>;

export function isSystemState(
  value: unknown,
): value is components["schemas"]["SystemStatus"]["status"] {
  return typeof value === "string" && systemStatusStatusValues.some((state) => state === value);
}
