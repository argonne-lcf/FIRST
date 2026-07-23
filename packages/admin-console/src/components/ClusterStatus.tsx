import {
  type HealthCheckResult,
  type PilotDeploymentSummary,
  type StaticDeploymentSummary,
} from "@/lib/client";
import { cn } from "@/lib/utils";

const HEALTH_DOT: Record<HealthCheckResult, string> = {
  healthy: "bg-success",
  unhealthy: "bg-destructive",
  unknown: "bg-muted-foreground",
};

export function HealthIndicator({ health }: { health: HealthCheckResult }) {
  return (
    <span className="flex items-center gap-1.5 text-xs text-muted-foreground capitalize">
      <span className={cn("size-2 rounded-full", HEALTH_DOT[health])} />
      {health}
    </span>
  );
}

/** Count occurrences of each key, preserving the given display order. */
function tally<T extends string>(items: T[], order: readonly T[]) {
  return order
    .map((key) => ({ key, count: items.filter((i) => i === key).length }))
    .filter((entry) => entry.count > 0);
}

const HEALTH_ORDER: readonly HealthCheckResult[] = [
  "healthy",
  "unhealthy",
  "unknown",
];
const PILOT_STATE_ORDER = [
  "healthy",
  "degraded",
  "starting",
  "stopping",
  "awaiting_capacity",
  "failed",
  "offline",
] as const;

export function DeploymentCounts({
  staticDeployments,
  pilotDeployments,
}: {
  staticDeployments: StaticDeploymentSummary[];
  pilotDeployments: PilotDeploymentSummary[];
}) {
  const staticByHealth = tally(
    staticDeployments.map((d) => d.health),
    HEALTH_ORDER,
  );
  const pilotByState = tally(
    pilotDeployments.map((d) => d.state),
    PILOT_STATE_ORDER,
  );

  return (
    <div className="space-y-1 text-xs text-muted-foreground">
      <p>
        {staticDeployments.length} StaticDeployment
        {staticDeployments.length === 1 ? "" : "s"}
        {staticByHealth.length > 0 && (
          <>
            {" ("}
            {staticByHealth.map((e) => `${e.count} ${e.key}`).join(", ")}
            {")"}
          </>
        )}
      </p>
      <p>
        {pilotDeployments.length} PilotDeployment
        {pilotDeployments.length === 1 ? "" : "s"}
        {pilotByState.length > 0 && (
          <>
            {" ("}
            {pilotByState
              .map((e) => `${e.count} ${e.key.replace(/_/g, " ")}`)
              .join(", ")}
            {")"}
          </>
        )}
      </p>
    </div>
  );
}
