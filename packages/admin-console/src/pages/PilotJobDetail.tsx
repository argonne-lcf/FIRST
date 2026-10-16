import { useQuery } from "@tanstack/react-query";
import { useParams } from "@tanstack/react-router";
import {
  type HostGpus,
  type PilotJob,
  type PilotResources,
} from "@/lib/client";
import { clusterQueries } from "@/queries/cluster";
import { Breadcrumb } from "@/components/Breadcrumb";
import { FieldValueTable } from "@/components/FieldValueTable";
import { GpuCell } from "@/components/GpuCell";
import { ReconcileBanner } from "@/components/ReconcileBanner";
import { ReplicaCards } from "@/components/ReplicaCards";
import { ResourceHeader } from "@/components/ResourceHeader";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { formatAgo, formatTimestamp, gib, unslug } from "@/lib/utils";

function DetailShell({
  clusterSlug,
  clusterName,
  jobName,
  children,
}: {
  clusterSlug: string;
  clusterName: string;
  jobName: string;
  children: React.ReactNode;
}) {
  return (
    <>
      <Breadcrumb
        items={[
          { label: "Clusters", link: { to: "/clusters" } },
          {
            label: clusterName,
            link: { to: "/clusters/$clusterSlug", params: { clusterSlug } },
          },
          { label: jobName },
        ]}
      />
      <main className="flex-1 space-y-6 p-6">{children}</main>
    </>
  );
}

/** One host node: a rounded panel of its GPUs. */
function NodeCard({
  host,
  observedAt,
}: {
  host: HostGpus;
  observedAt: string | null | undefined;
}) {
  return (
    <div className="space-y-3 rounded-xl border border-border bg-card p-4">
      <div className="flex items-center justify-between gap-2">
        <span
          className="truncate font-mono text-sm font-medium"
          title={host.hostname}
        >
          {host.hostname}
        </span>
        <Badge variant="outline">{host.gpus.length} GPU</Badge>
      </div>
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
        {host.gpus.map((gpu) => (
          <GpuCell
            key={gpu.index}
            gpu={gpu}
            hostname={host.hostname}
            observedAt={observedAt}
          />
        ))}
      </div>
    </div>
  );
}

/** One labelled figure in the summary strip, with an optional muted hint. */
function StatCard({
  label,
  value,
  hint,
}: {
  label: string;
  value: React.ReactNode;
  hint?: React.ReactNode;
}) {
  return (
    <Card size="sm">
      <CardContent className="space-y-1">
        <p className="text-xs text-muted-foreground">{label}</p>
        <p className="text-xl font-semibold tabular-nums">{value}</p>
        {hint != null && (
          <p className="text-xs text-muted-foreground tabular-nums">{hint}</p>
        )}
      </CardContent>
    </Card>
  );
}

/** Aggregate figures across all hosts; utilization averages GPUs reporting it. */
function summarize(hosts: HostGpus[]) {
  const gpus = hosts.flatMap((h) => h.gpus);
  const usedMib = gpus.reduce((sum, g) => sum + g.memory_used_mib, 0);
  const totalMib = gpus.reduce((sum, g) => sum + g.memory_total_mib, 0);
  const utils = gpus.flatMap((g) =>
    g.utilization_gpu_pct != null ? [g.utilization_gpu_pct] : [],
  );
  return {
    hosts: hosts.length,
    gpus: gpus.length,
    usedMib,
    totalMib,
    memPct: totalMib > 0 ? Math.round((usedMib / totalMib) * 100) : null,
    utilPct:
      utils.length > 0
        ? Math.round(utils.reduce((a, b) => a + b, 0) / utils.length)
        : null,
  };
}

const DASH = <span className="text-muted-foreground">—</span>;

/** Row of aggregate stat cards above the host grid. */
function SummaryStrip({ hosts }: { hosts: HostGpus[] }) {
  const s = summarize(hosts);
  return (
    <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
      <StatCard label="Hosts" value={s.hosts} />
      <StatCard label="GPUs" value={s.gpus} />
      <StatCard
        label="Memory"
        value={
          s.totalMib > 0 ? `${gib(s.usedMib)} / ${gib(s.totalMib)} GiB` : DASH
        }
        hint={s.memPct != null ? `${s.memPct}% used` : undefined}
      />
      <StatCard
        label="GPU utilization"
        value={s.utilPct != null ? `${s.utilPct}%` : DASH}
        hint="mean across GPUs"
      />
    </div>
  );
}

// The pilot re-queries its GPUs every 60s and keeps reporting its last good
// query while queries fail; three missed refreshes means the figures are stale.
const INVENTORY_STALE_SEC = 180;

function secondsSince(iso: string): number {
  return (Date.now() - new Date(iso).getTime()) / 1000;
}

/** When the pilot last queried its GPUs, flagged once it is stale. */
function ObservedAt({ resources }: { resources: PilotResources }) {
  const at = resources.observed_at;
  if (!at) return null;
  return (
    <span
      className="flex items-center gap-2 text-xs text-muted-foreground"
      title={formatTimestamp(at)}
    >
      Observed {formatAgo(at)}
      {secondsSince(at) > INVENTORY_STALE_SEC && (
        <Badge variant="warning">Stale</Badge>
      )}
    </span>
  );
}

/** Live HPC node/GPU resource grid, from the job's runtime resources. */
function ResourcesGrid({ job }: { job: PilotJob }) {
  const resources = job.runtime?.resources ?? job.resources;
  const hosts = resources.hosts ?? [];

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="text-lg font-medium">Node Resources</h2>
        <ObservedAt resources={resources} />
      </div>
      {hosts.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          No node resources reported.
        </p>
      ) : (
        <>
          <SummaryStrip hosts={hosts} />
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            {hosts.map((host) => (
              <NodeCard
                key={host.hostname}
                host={host}
                observedAt={resources.observed_at}
              />
            ))}
          </div>
        </>
      )}
    </div>
  );
}

export function PilotJobDetail() {
  const { clusterSlug, jobSlug } = useParams({
    from: "/shell/clusters/$clusterSlug/jobs/$jobSlug",
  });
  const clusterName = unslug(clusterSlug);
  const jobName = unslug(jobSlug);

  const query = useQuery(clusterQueries.detail(clusterName));

  if (query.isPending) {
    return (
      <DetailShell
        clusterSlug={clusterSlug}
        clusterName={clusterName}
        jobName={jobName}
      >
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-48" />
      </DetailShell>
    );
  }

  if (query.isError) {
    return (
      <DetailShell
        clusterSlug={clusterSlug}
        clusterName={clusterName}
        jobName={jobName}
      >
        <Card>
          <CardHeader>
            <CardTitle>Pilot Job</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-destructive">
            Failed to load cluster:{" "}
            {query.error instanceof Error
              ? query.error.message
              : "unknown error"}
          </CardContent>
        </Card>
      </DetailShell>
    );
  }

  const job = query.data.pilot_jobs.find((j) => j.slug === jobSlug);
  if (!job) {
    return (
      <DetailShell
        clusterSlug={clusterSlug}
        clusterName={query.data.name}
        jobName={jobName}
      >
        <Card>
          <CardHeader>
            <CardTitle>Pilot Job</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-destructive">
            Pilot job not found.
          </CardContent>
        </Card>
      </DetailShell>
    );
  }

  return (
    <DetailShell
      clusterSlug={clusterSlug}
      clusterName={query.data.name}
      jobName={job.name}
    >
      <ResourceHeader kind={job.kind} name={job.name} uid={job.uid}>
        <Badge variant="outline">
          {job.scheduler_state.replace(/_/g, " ")}
        </Badge>
      </ResourceHeader>

      <ReconcileBanner resource={job} />

      <ResourcesGrid job={job} />

      <div className="space-y-3">
        <h2 className="text-lg font-medium">Replicas</h2>
        <ReplicaCards replicas={job.assigned_replicas} />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Details</CardTitle>
        </CardHeader>
        <CardContent>
          <FieldValueTable
            obj={job}
            omit={[
              "kind",
              "name",
              "uid",
              "assigned_replicas",
              "resources",
              "runtime",
            ]}
          />
        </CardContent>
      </Card>
    </DetailShell>
  );
}
