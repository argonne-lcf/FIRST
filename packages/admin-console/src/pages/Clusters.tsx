import { useQuery } from "@tanstack/react-query";
import { TriangleAlert } from "lucide-react";
import {
  type HealthCheckResult,
  type PilotDeploymentState,
} from "@/lib/client";
import { clusterQueries, type ClusterOverview } from "@/queries/cluster";
import {
  Card,
  CardAction,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";

const HEALTH_DOT: Record<HealthCheckResult, string> = {
  healthy: "bg-success",
  unhealthy: "bg-destructive",
  unknown: "bg-muted-foreground",
};

function HealthIndicator({ health }: { health: HealthCheckResult }) {
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
const PILOT_STATE_ORDER: readonly PilotDeploymentState[] = [
  "healthy",
  "degraded",
  "starting",
  "stopping",
  "awaiting_capacity",
  "failed",
  "offline",
];

function DeploymentCounts({ overview }: { overview: ClusterOverview }) {
  const { staticDeployments, pilotDeployments } = overview;
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

function ClusterCard({ overview }: { overview: ClusterOverview }) {
  const { cluster } = overview;
  return (
    <Card>
      <CardHeader>
        <CardTitle>{cluster.name}</CardTitle>
        <CardAction>
          <HealthIndicator health={cluster.health} />
        </CardAction>
      </CardHeader>
      <CardContent className="space-y-3">
        {cluster.maintenance_notice && (
          <div className="flex items-start gap-2 rounded-md bg-warning/10 p-2 text-xs text-warning">
            <TriangleAlert className="mt-0.5 size-4 shrink-0" />
            <span>{cluster.maintenance_notice}</span>
          </div>
        )}
        <DeploymentCounts overview={overview} />
      </CardContent>
    </Card>
  );
}

export function Clusters() {
  const { data, isPending, isError, error } = useQuery(
    clusterQueries.overview(),
  );

  if (isPending) {
    return (
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {Array.from({ length: 6 }).map((_, i) => (
          <Skeleton key={i} className="h-32" />
        ))}
      </div>
    );
  }

  if (isError) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Clusters</CardTitle>
          <CardContent className="px-0 text-sm text-destructive">
            Failed to load clusters: {error.message}
          </CardContent>
        </CardHeader>
      </Card>
    );
  }

  if (data.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Clusters</CardTitle>
          <CardContent className="px-0 text-sm text-muted-foreground">
            No clusters configured.
          </CardContent>
        </CardHeader>
      </Card>
    );
  }

  return (
    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {data.map((overview) => (
        <ClusterCard key={overview.cluster.uid} overview={overview} />
      ))}
    </div>
  );
}
