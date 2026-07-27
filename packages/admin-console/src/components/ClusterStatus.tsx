import {
  type HealthCheckResult,
  type PilotDeploymentState,
  type PilotDeploymentSummary,
  type StaticDeploymentSummary,
} from "@/lib/client";
import { STATE_SEVERITY, type Severity } from "@/lib/severity";
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

const SEVERITY_DOT: Record<Severity, string> = {
  ok: "bg-success",
  warning: "bg-warning",
  critical: "bg-destructive",
};

/** Dot + label for a pilot deployment's aggregated state, colored by severity. */
export function PilotStateIndicator({
  state,
}: {
  state: PilotDeploymentState;
}) {
  return (
    <span className="flex items-center gap-1.5 text-xs text-muted-foreground capitalize">
      <span
        className={cn(
          "size-2 rounded-full",
          SEVERITY_DOT[STATE_SEVERITY[state]],
        )}
      />
      {state.replace(/_/g, " ")}
    </span>
  );
}

const HEALTH_DESCRIPTION: Record<HealthCheckResult, string> = {
  healthy: "The configured health check is succeeding.",
  unknown: "The health check result is not yet known.",
  unhealthy:
    "The configured health check is failing; see Health Observer logs for details.",
};

/** Muted one-line explanation of a health check result. */
export function HealthDescription({ health }: { health: HealthCheckResult }) {
  return (
    <p className="text-xs text-muted-foreground">
      {HEALTH_DESCRIPTION[health]}
    </p>
  );
}

/** Muted one-line explanation of a pilot deployment state (N = desired replicas). */
export function PilotStateDescription({
  state,
  desiredReplicas,
}: {
  state: PilotDeploymentState;
  desiredReplicas: number;
}) {
  const n = desiredReplicas;
  const replicas = `${n} desired replica${n === 1 ? "" : "s"}`;
  const text: Record<PilotDeploymentState, string> = {
    healthy: `Routeable capacity exists; all ${replicas} are ready.`,
    degraded: `Routeable capacity exists; fewer than ${replicas} are ready.`,
    starting: "Nothing is serving yet, but capacity is on the way.",
    stopping: "Teardown is underway.",
    failed: "All replicas are unhealthy or launches are failing.",
    awaiting_capacity: "No replicas created yet.",
    offline: "Desired replicas = 0; no live replicas.",
  };
  return <p className="text-xs text-muted-foreground">{text[state]}</p>;
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
