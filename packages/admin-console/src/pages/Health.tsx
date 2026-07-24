import { useQuery } from "@tanstack/react-query";
import {
  CircleCheck,
  CircleX,
  Info,
  TriangleAlert,
  type LucideIcon,
} from "lucide-react";
import { type ResourceHealth } from "@/lib/client";
import { healthQueries } from "@/queries/health";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";

type Severity = "ok" | "warning" | "critical" | "info";

// Status → severity per resource type. Unknown statuses fall back to "info".
const HEALTH: Record<string, Severity> = {
  healthy: "ok",
  unhealthy: "critical",
  unknown: "info",
};
const PILOT_STATE: Record<string, Severity> = {
  healthy: "ok",
  failed: "critical",
  degraded: "warning",
  starting: "info",
  stopping: "info",
  awaiting_capacity: "info",
  offline: "info",
};
const REPLICA_STATE: Record<string, Severity> = {
  ready: "ok",
  error: "critical",
  start_timeout: "critical",
  unhealthy: "warning",
  pending: "info",
  placed: "info",
  launching: "info",
  terminating: "info",
  terminated: "info",
};

type GroupKey =
  | "clusters"
  | "static_deployments"
  | "pilot_deployments"
  | "pilot_jobs"
  | "pilot_replicas";

const GROUPS: {
  key: GroupKey;
  label: string;
  severityOf: Record<string, Severity>;
}[] = [
  { key: "clusters", label: "Clusters", severityOf: HEALTH },
  {
    key: "static_deployments",
    label: "Static Deployments",
    severityOf: HEALTH,
  },
  {
    key: "pilot_deployments",
    label: "Pilot Deployments",
    severityOf: PILOT_STATE,
  },
  { key: "pilot_jobs", label: "Pilot Jobs", severityOf: HEALTH },
  { key: "pilot_replicas", label: "Pilot Replicas", severityOf: REPLICA_STATE },
];

const SEVERITY_RANK: Record<Severity, number> = {
  critical: 3,
  warning: 2,
  info: 1,
  ok: 0,
};

const SEVERITY_ICON: Record<Severity, LucideIcon> = {
  ok: CircleCheck,
  warning: TriangleAlert,
  critical: CircleX,
  info: Info,
};

const SEVERITY_COLOR: Record<Severity, string> = {
  ok: "text-success",
  warning: "text-warning",
  critical: "text-destructive",
  info: "text-muted-foreground",
};

const SEVERITY_BADGE: Record<
  Severity,
  "success" | "warning" | "destructive" | "outline"
> = {
  ok: "success",
  warning: "warning",
  critical: "destructive",
  info: "outline",
};

/** reconcile failures always dominate; otherwise fall back to the status map. */
function resourceSeverity(
  r: ResourceHealth,
  severityOf: Record<string, Severity>,
): Severity {
  if ((r.reconcile_failures ?? 0) > 0) return "critical";
  return severityOf[r.status] ?? "info";
}

/** The worst severity across a set (empty → "ok"). */
function worst(severities: Severity[]): Severity {
  return severities.reduce<Severity>(
    (acc, s) => (SEVERITY_RANK[s] > SEVERITY_RANK[acc] ? s : acc),
    "ok",
  );
}

function humanize(status: string): string {
  return status.replace(/_/g, " ").replace(/^\w/, (c) => c.toUpperCase());
}

/** Grouped status counts, sorted worst-severity first. */
function tally(
  resources: ResourceHealth[],
  severityOf: Record<string, Severity>,
) {
  const counts = new Map<string, number>();
  for (const r of resources)
    counts.set(r.status, (counts.get(r.status) ?? 0) + 1);
  return [...counts.entries()]
    .map(([status, count]) => ({
      status,
      count,
      severity: severityOf[status] ?? "info",
    }))
    .sort(
      (a, b) =>
        SEVERITY_RANK[b.severity] - SEVERITY_RANK[a.severity] ||
        a.status.localeCompare(b.status),
    );
}

function GroupCard({
  label,
  resources,
  severityOf,
}: {
  label: string;
  resources: ResourceHealth[];
  severityOf: Record<string, Severity>;
}) {
  const overall = worst(resources.map((r) => resourceSeverity(r, severityOf)));
  const Icon = SEVERITY_ICON[overall];
  const groups = tally(resources, severityOf);

  return (
    <Card>
      <CardHeader>
        <CardTitle>{label}</CardTitle>
        <CardDescription>
          {resources.length === 0
            ? "None configured"
            : overall === "ok"
              ? "All OK"
              : `${resources.length} total`}
        </CardDescription>
        <CardAction>
          <Icon className={cn("size-6", SEVERITY_COLOR[overall])} />
        </CardAction>
      </CardHeader>
      {groups.length > 0 && (
        <CardContent className="flex flex-wrap gap-1.5">
          {groups.map((g) => (
            <Badge key={g.status} variant={SEVERITY_BADGE[g.severity]}>
              {g.count} {humanize(g.status)}
            </Badge>
          ))}
        </CardContent>
      )}
    </Card>
  );
}

function OverallBanner({ overall }: { overall: Severity }) {
  if (overall === "ok" || overall === "info") {
    return (
      <Card className="border-success/30 bg-success/5">
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-success">
            <CircleCheck className="size-5" />
            All systems operational
          </CardTitle>
          <CardDescription>
            No degraded or failed resources detected.
          </CardDescription>
        </CardHeader>
      </Card>
    );
  }
  const isCritical = overall === "critical";
  return (
    <Card
      className={cn(
        isCritical
          ? "border-destructive/30 bg-destructive/5"
          : "border-warning/30 bg-warning/5",
      )}
    >
      <CardHeader>
        <CardTitle
          className={cn(
            "flex items-center gap-2",
            isCritical ? "text-destructive" : "text-warning",
          )}
        >
          {isCritical ? (
            <CircleX className="size-5" />
          ) : (
            <TriangleAlert className="size-5" />
          )}
          {isCritical ? "Issues need attention" : "Degraded — monitoring"}
        </CardTitle>
        <CardDescription>
          One or more resources are {isCritical ? "failing" : "degraded"}. See
          details below.
        </CardDescription>
      </CardHeader>
    </Card>
  );
}

function ReconcileErrors({ errors }: { errors: ResourceHealth[] }) {
  if (errors.length === 0) return null;
  return (
    <Card className="border-destructive/30">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-destructive">
          <CircleX className="size-5" />
          Control plane reconcile errors
        </CardTitle>
        <CardDescription>
          {errors.length} resource{errors.length === 1 ? "" : "s"} failing to
          reconcile.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        {errors.map((r) => (
          <div key={`${r.name}-${r.uid}`} className="space-y-0.5 text-sm">
            <div className="flex items-center gap-2">
              <span className="font-medium">{r.name}</span>
              <Badge variant="destructive">
                {r.reconcile_failures} failure
                {r.reconcile_failures === 1 ? "" : "s"}
              </Badge>
            </div>
            {r.reconcile_last_error && (
              <p className="font-mono text-xs break-words text-muted-foreground">
                {r.reconcile_last_error}
              </p>
            )}
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

export function Health() {
  const { data, isPending, isError, error } = useQuery(healthQueries.system());

  if (isPending) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-24" />
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-32" />
          ))}
        </div>
      </div>
    );
  }

  if (isError) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Health</CardTitle>
          <CardContent className="px-0 text-sm text-destructive">
            Failed to load system health: {error.message}
          </CardContent>
        </CardHeader>
      </Card>
    );
  }

  const overall = worst(
    GROUPS.flatMap((g) =>
      data[g.key].map((r) => resourceSeverity(r, g.severityOf)),
    ),
  );
  const reconcileErrors = GROUPS.flatMap((g) => data[g.key]).filter(
    (r) => (r.reconcile_failures ?? 0) > 0,
  );

  return (
    <div className="space-y-6">
      <OverallBanner overall={overall} />
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {GROUPS.map((g) => (
          <GroupCard
            key={g.key}
            label={g.label}
            resources={data[g.key]}
            severityOf={g.severityOf}
          />
        ))}
      </div>
      <ReconcileErrors errors={reconcileErrors} />
    </div>
  );
}
