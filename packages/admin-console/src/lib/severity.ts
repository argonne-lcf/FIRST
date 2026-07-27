import { type DeploymentState } from "@/components/DeploymentRow";

export type Severity = "ok" | "warning" | "critical";

/** State → severity. Only "healthy" is green; hard failures are red. */
export const STATE_SEVERITY: Record<DeploymentState, Severity> = {
  healthy: "ok",
  unhealthy: "critical",
  failed: "critical",
  degraded: "warning",
  starting: "warning",
  stopping: "warning",
  awaiting_capacity: "warning",
  offline: "warning",
  unknown: "warning",
};
