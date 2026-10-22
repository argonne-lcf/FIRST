import { useQuery } from "@tanstack/react-query";
import { Link, type LinkProps } from "@tanstack/react-router";
import {
  ArrowRight,
  CircleCheck,
  CircleX,
  Info,
  TriangleAlert,
  type LucideIcon,
} from "lucide-react";
import {
  type ClusterHealth,
  type PilotJobFailure,
  type ResourceHealth,
} from "@/lib/client";
import { healthQueries } from "@/queries/health";
import { RetryCountdown, TracebackBlock } from "@/components/ReconcileBanner";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Skeleton } from "@/components/ui/skeleton";
import { cn, formatAgo, humanize, slugify } from "@/lib/utils";

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

/**
 * Build the detail-view link for a resource in a given group. `ResourceHealth`
 * carries only name/uid/status, so parent slugs are recovered from the name:
 * - replicas are named `{deployment}/replica/{token}` → deployment is the head.
 * - pilot jobs are named `{cluster}-pilot-{token}` (cluster '/' mangled to '-'),
 *   so we strip the suffix; correct when cluster names contain no '/'.
 */
type LinkFor = (r: ResourceHealth) => LinkProps;

const clusterLink: LinkFor = (r) => ({
  to: "/clusters/$clusterSlug",
  params: { clusterSlug: slugify(r.name) },
});
const staticDeploymentLink: LinkFor = (r) => ({
  to: "/deployments/$kind/$deploymentSlug",
  params: { kind: "static", deploymentSlug: slugify(r.name) },
});
const pilotDeploymentLink: LinkFor = (r) => ({
  to: "/deployments/$kind/$deploymentSlug",
  params: { kind: "pilot", deploymentSlug: slugify(r.name) },
});
const pilotJobLink: LinkFor = (r) => ({
  to: "/clusters/$clusterSlug/jobs/$jobSlug",
  params: {
    clusterSlug: slugify(r.name.replace(/-pilot-[^-]+$/, "")),
    jobSlug: slugify(r.name),
  },
});
const pilotReplicaLink: LinkFor = (r) => ({
  to: "/deployments/pilot/$deploymentSlug/replicas/$replicaSlug",
  params: {
    deploymentSlug: slugify(r.name.split("/replica/")[0]),
    replicaSlug: slugify(r.name),
  },
});

const GROUPS: {
  key: GroupKey;
  label: string;
  kind: string;
  severityOf: Record<string, Severity>;
  linkFor: LinkFor;
}[] = [
  {
    key: "clusters",
    label: "Clusters",
    kind: "Cluster",
    severityOf: HEALTH,
    linkFor: clusterLink,
  },
  {
    key: "static_deployments",
    label: "Static Deployments",
    kind: "StaticDeployment",
    severityOf: HEALTH,
    linkFor: staticDeploymentLink,
  },
  {
    key: "pilot_deployments",
    label: "Pilot Deployments",
    kind: "PilotDeployment",
    severityOf: PILOT_STATE,
    linkFor: pilotDeploymentLink,
  },
  {
    key: "pilot_jobs",
    label: "Pilot Jobs",
    kind: "PilotJob",
    severityOf: HEALTH,
    linkFor: pilotJobLink,
  },
  {
    key: "pilot_replicas",
    label: "Pilot Replicas",
    kind: "PilotReplica",
    severityOf: REPLICA_STATE,
    linkFor: pilotReplicaLink,
  },
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

/** Resources grouped by status, sorted worst-severity first. */
function tally(
  resources: ResourceHealth[],
  severityOf: Record<string, Severity>,
) {
  const byStatus = new Map<string, ResourceHealth[]>();
  for (const r of resources) {
    const bucket = byStatus.get(r.status);
    if (bucket) bucket.push(r);
    else byStatus.set(r.status, [r]);
  }
  return [...byStatus.entries()]
    .map(([status, items]) => ({
      status,
      items,
      severity: severityOf[status] ?? "info",
    }))
    .sort(
      (a, b) =>
        SEVERITY_RANK[b.severity] - SEVERITY_RANK[a.severity] ||
        a.status.localeCompare(b.status),
    );
}

/** Dialog listing the resources in one status; each item links to its detail. */
function StatusDialog({
  label,
  status,
  severity,
  items,
  linkFor,
}: {
  label: string;
  status: string;
  severity: Severity;
  items: ResourceHealth[];
  linkFor: LinkFor;
}) {
  return (
    <Dialog>
      <DialogTrigger asChild>
        <Badge variant={SEVERITY_BADGE[severity]} className="cursor-pointer">
          {items.length} {humanize(status)}
        </Badge>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>
            {label} — {humanize(status)}
          </DialogTitle>
          <DialogDescription>
            {items.length} resource{items.length === 1 ? "" : "s"}
          </DialogDescription>
        </DialogHeader>
        <ul className="-mx-1 max-h-96 overflow-auto">
          {items.map((r) => (
            <li key={r.uid}>
              <DialogClose asChild>
                <Link
                  {...linkFor(r)}
                  className="flex items-center gap-2 rounded-md px-1 py-1.5 text-sm hover:bg-muted"
                >
                  <span className="min-w-0 flex-1">
                    <span className="block truncate">{r.name}</span>
                    {r.health_detail && (
                      <span className="block truncate font-mono text-xs text-muted-foreground">
                        {r.health_detail}
                      </span>
                    )}
                  </span>
                  <span className="font-mono text-xs text-muted-foreground">
                    #{r.uid}
                  </span>
                  <ArrowRight className="size-4 shrink-0 text-muted-foreground" />
                </Link>
              </DialogClose>
            </li>
          ))}
        </ul>
      </DialogContent>
    </Dialog>
  );
}

function GroupCard({
  label,
  resources,
  severityOf,
  linkFor,
}: {
  label: string;
  resources: ResourceHealth[];
  severityOf: Record<string, Severity>;
  linkFor: LinkFor;
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
            <StatusDialog
              key={g.status}
              label={label}
              status={g.status}
              severity={g.severity}
              items={g.items}
              linkFor={linkFor}
            />
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

/** A resource in reconcile backoff, tagged with its kind and detail link. */
type Backoff = { kind: string; resource: ResourceHealth; link: LinkProps };

/**
 * Resources in reconcile backoff, one compact row each (kind, name, failure
 * count, countdown to the next attempt, one-line error); expanding a row shows
 * the full error and traceback.
 */
function ReconcileBackoff({ backoffs }: { backoffs: Backoff[] }) {
  if (backoffs.length === 0) return null;
  return (
    <Card className="border-destructive/30">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-destructive">
          <CircleX className="size-5" />
          Reconcile backoff
        </CardTitle>
        <CardDescription>
          {backoffs.length} resource{backoffs.length === 1 ? "" : "s"} failing
          to reconcile, retried with exponential backoff. Expand for the
          traceback.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Accordion type="multiple">
          {backoffs.map(({ kind, resource: r, link }) => (
            <AccordionItem key={`${kind}-${r.uid}`} value={`${kind}-${r.uid}`}>
              <AccordionTrigger className="gap-4 hover:no-underline">
                <div className="min-w-0 flex-1 space-y-1">
                  <div className="flex items-center gap-2">
                    <Badge variant="outline">{kind}</Badge>
                    <span className="truncate">{r.name}</span>
                  </div>
                  {r.reconcile_last_error && (
                    <p className="truncate font-mono text-xs font-normal text-muted-foreground group-aria-expanded/accordion-trigger:break-words group-aria-expanded/accordion-trigger:whitespace-normal">
                      {r.reconcile_last_error}
                    </p>
                  )}
                </div>
                <div className="flex shrink-0 flex-col items-end gap-1 text-xs font-normal text-muted-foreground">
                  <Badge variant="destructive">
                    {r.reconcile_failures} failure
                    {r.reconcile_failures === 1 ? "" : "s"}
                  </Badge>
                  {r.reconcile_retry_at && (
                    <span>
                      retry <RetryCountdown retryAt={r.reconcile_retry_at} />
                    </span>
                  )}
                </div>
              </AccordionTrigger>
              <AccordionContent className="space-y-2">
                {r.reconcile_last_traceback && (
                  <TracebackBlock traceback={r.reconcile_last_traceback} />
                )}
                <Link
                  {...link}
                  className="inline-flex items-center gap-1 text-xs text-muted-foreground"
                >
                  Open {kind} <ArrowRight className="size-3" />
                </Link>
              </AccordionContent>
            </AccordionItem>
          ))}
        </Accordion>
      </CardContent>
    </Card>
  );
}

/** A resource failing its health check, tagged with its kind and detail link. */
type HealthFailure = {
  kind: string;
  resource: ResourceHealth;
  link: LinkProps;
};

/**
 * Clusters and static deployments failing their health check, each with the
 * reason from the latest probe. Resources in reconcile backoff are left to
 * that card.
 */
function HealthCheckFailures({ failures }: { failures: HealthFailure[] }) {
  if (failures.length === 0) return null;
  return (
    <Card className="border-destructive/30">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-destructive">
          <CircleX className="size-5" />
          Failing health checks
        </CardTitle>
        <CardDescription>
          {failures.length} resource{failures.length === 1 ? "" : "s"} failing
          health checks, with the reason from the latest probe.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ul className="divide-y divide-border">
          {failures.map(({ kind, resource: r, link }) => (
            <li key={`${kind}-${r.uid}`} className="space-y-1 py-2">
              <div className="flex items-center gap-2">
                <Badge variant="outline">{kind}</Badge>
                <Link
                  {...link}
                  className="min-w-0 truncate text-sm hover:underline"
                >
                  {r.name}
                </Link>
              </div>
              <p className="font-mono text-xs break-words whitespace-pre-wrap text-muted-foreground">
                {r.health_detail}
              </p>
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}

/** Failed pilot jobs listed per cluster before the rest are elided. */
const MAX_FAILED_JOBS_SHOWN = 5;

/**
 * Severity of a cluster's pilot start failures: critical once submissions
 * halt, warning while a streak is building, info with only past failures.
 */
function pilotStartSeverity(c: ClusterHealth, failedJobs: number): Severity {
  const n = c.pilot_start_failures ?? 0;
  const max = c.max_consecutive_pilot_start_failures;
  if (max != null && n >= max) return "critical";
  if (n > 0) return "warning";
  return failedJobs > 0 ? "info" : "ok";
}

function PilotStartBadge({ cluster: c }: { cluster: ClusterHealth }) {
  const n = c.pilot_start_failures ?? 0;
  const max = c.max_consecutive_pilot_start_failures;
  const streak = max != null ? `${n}/${max}` : `${n}`;
  if (max != null && n >= max) {
    return (
      <Badge variant="destructive">Submissions halted · {streak} failed</Badge>
    );
  }
  if (n > 0) {
    return <Badge variant="warning">{streak} consecutive start failures</Badge>;
  }
  return <Badge variant="outline">Streak reset</Badge>;
}

/**
 * Pilot jobs that died before their manager came up, grouped by cluster with
 * each cluster's consecutive start failure count against its halt threshold.
 */
function PilotStartFailures({
  clusters,
  failedJobs,
}: {
  clusters: ClusterHealth[];
  failedJobs: PilotJobFailure[];
}) {
  const jobsByCluster = new Map<string, PilotJobFailure[]>();
  for (const j of failedJobs) {
    const bucket = jobsByCluster.get(j.cluster_name);
    if (bucket) bucket.push(j);
    else jobsByCluster.set(j.cluster_name, [j]);
  }
  const rows = clusters
    .map((c) => {
      const jobs = jobsByCluster.get(c.name) ?? [];
      return { cluster: c, jobs, severity: pilotStartSeverity(c, jobs.length) };
    })
    .filter((row) => row.severity !== "ok")
    .sort(
      (a, b) =>
        SEVERITY_RANK[b.severity] - SEVERITY_RANK[a.severity] ||
        a.cluster.name.localeCompare(b.cluster.name),
    );
  if (rows.length === 0) return null;

  const overall = worst(rows.map((row) => row.severity));
  const Icon = SEVERITY_ICON[overall];
  return (
    <Card>
      <CardHeader>
        <CardTitle>Pilot start failures</CardTitle>
        <CardDescription>
          Pilot jobs that died before their manager came up. Consecutive
          failures halt new submissions on the cluster until a reconcile-reset.
        </CardDescription>
        <CardAction>
          <Icon className={cn("size-6", SEVERITY_COLOR[overall])} />
        </CardAction>
      </CardHeader>
      <CardContent className="space-y-4">
        {rows.map(({ cluster, jobs }) => (
          <div key={cluster.uid} className="space-y-1">
            <div className="flex flex-wrap items-center gap-2">
              <Link
                {...clusterLink(cluster)}
                className="text-sm font-medium hover:underline"
              >
                {cluster.name}
              </Link>
              <PilotStartBadge cluster={cluster} />
            </div>
            {jobs.length > 0 && (
              <ul className="space-y-1">
                {jobs.slice(0, MAX_FAILED_JOBS_SHOWN).map((j) => (
                  <li key={j.uid} className="text-xs">
                    <div className="flex items-center gap-2">
                      <Link
                        to="/clusters/$clusterSlug/jobs/$jobSlug"
                        params={{
                          clusterSlug: slugify(j.cluster_name),
                          jobSlug: slugify(j.name),
                        }}
                        className="min-w-0 truncate font-mono hover:underline"
                      >
                        {j.name}
                      </Link>
                      <span className="shrink-0 text-muted-foreground">
                        {formatAgo(j.deleted_at)}
                      </span>
                    </div>
                    <p className="text-muted-foreground">{j.failure_reason}</p>
                  </li>
                ))}
                {jobs.length > MAX_FAILED_JOBS_SHOWN && (
                  <li className="text-xs text-muted-foreground">
                    +{jobs.length - MAX_FAILED_JOBS_SHOWN} more
                  </li>
                )}
              </ul>
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

  const failedJobs = data.failed_pilot_jobs ?? [];
  const overall = worst([
    ...GROUPS.flatMap((g) =>
      data[g.key].map((r) => resourceSeverity(r, g.severityOf)),
    ),
    ...data.clusters.map((c) =>
      pilotStartSeverity(
        c,
        failedJobs.filter((j) => j.cluster_name === c.name).length,
      ),
    ),
  ]);
  const backoffs: Backoff[] = GROUPS.flatMap((g) =>
    data[g.key]
      .filter((r) => (r.reconcile_failures ?? 0) > 0)
      .map((r) => ({ kind: g.kind, resource: r, link: g.linkFor(r) })),
  ).sort(
    (a, b) =>
      (b.resource.reconcile_failures ?? 0) -
        (a.resource.reconcile_failures ?? 0) ||
      a.resource.name.localeCompare(b.resource.name),
  );

  const healthFailures: HealthFailure[] = GROUPS.flatMap((g) =>
    data[g.key]
      .filter(
        (r) =>
          r.status === "unhealthy" &&
          r.health_detail &&
          (r.reconcile_failures ?? 0) === 0,
      )
      .map((r) => ({ kind: g.kind, resource: r, link: g.linkFor(r) })),
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
            linkFor={g.linkFor}
          />
        ))}
      </div>
      <HealthCheckFailures failures={healthFailures} />
      <ReconcileBackoff backoffs={backoffs} />
      <PilotStartFailures clusters={data.clusters} failedJobs={failedJobs} />
    </div>
  );
}
