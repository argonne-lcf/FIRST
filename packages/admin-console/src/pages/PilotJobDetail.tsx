import { useQuery } from "@tanstack/react-query";
import { useParams } from "@tanstack/react-router";
import { type GpuInfo, type HostGpus, type PilotJob } from "@/lib/client";
import { clusterQueries } from "@/queries/cluster";
import { Breadcrumb } from "@/components/Breadcrumb";
import { FieldValueTable } from "@/components/FieldValueTable";
import { ReconcileBanner } from "@/components/ReconcileBanner";
import { ReplicaCards } from "@/components/ReplicaCards";
import { ResourceHeader } from "@/components/ResourceHeader";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { cn, unslug } from "@/lib/utils";

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

/** MiB → GiB with one decimal, e.g. 81920 → "80.0". */
function gib(mib: number): string {
  return (mib / 1024).toFixed(1);
}

/** Diagonal hatch fill (token-colored via currentColor) for unknown metrics. */
const HATCH =
  "repeating-linear-gradient(45deg, currentColor 0 3px, transparent 3px 6px)";

/** One GPU: index, device name, and a memory occupancy bar. */
function GpuCell({ gpu }: { gpu: GpuInfo }) {
  const total = gpu.memory_total_mib;
  const used = gpu.memory_used_mib;
  const hasMem = total != null && used != null && total > 0;
  const pct = hasMem ? Math.min(100, Math.round((used / total) * 100)) : 0;

  return (
    <div className="space-y-1 rounded-md border border-border bg-background p-2">
      <div className="flex items-center justify-between gap-2">
        <span className="font-mono text-xs text-muted-foreground">
          GPU {gpu.index}
        </span>
        {hasMem && <span className="text-xs tabular-nums">{pct}%</span>}
      </div>
      <div className="truncate text-xs" title={gpu.name}>
        {gpu.name}
      </div>
      <div className="h-1.5 overflow-hidden rounded-full bg-muted">
        {hasMem ? (
          <div
            className={cn(
              "h-full rounded-full transition-[width]",
              pct >= 90
                ? "bg-destructive"
                : pct >= 70
                  ? "bg-warning"
                  : "bg-success",
            )}
            style={{ width: `${pct}%` }}
          />
        ) : (
          <div
            className="h-full w-full text-muted-foreground/40"
            style={{ backgroundImage: HATCH }}
          />
        )}
      </div>
      <div className="text-xs tabular-nums text-muted-foreground">
        {hasMem ? (
          `${gib(used)} / ${gib(total)} GiB`
        ) : (
          <span title="Memory metrics unavailable">— GiB</span>
        )}
      </div>
    </div>
  );
}

/** One host node: a rounded panel of its GPUs. */
function NodeCard({ host }: { host: HostGpus }) {
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
          <GpuCell key={gpu.index} gpu={gpu} />
        ))}
      </div>
    </div>
  );
}

/** One labelled figure in the summary strip. */
function StatCard({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <Card size="sm">
      <CardContent className="space-y-1">
        <p className="text-xs text-muted-foreground">{label}</p>
        <p className="text-xl font-semibold tabular-nums">{value}</p>
      </CardContent>
    </Card>
  );
}

/** Aggregate figures across all hosts; memory sums only GPUs that report it. */
function summarize(hosts: HostGpus[]) {
  const gpus = hosts.flatMap((h) => h.gpus);
  let usedMib = 0;
  let totalMib = 0;
  for (const g of gpus) {
    if (g.memory_used_mib != null && g.memory_total_mib != null) {
      usedMib += g.memory_used_mib;
      totalMib += g.memory_total_mib;
    }
  }
  return {
    hosts: hosts.length,
    gpus: gpus.length,
    usedMib,
    totalMib,
    pct: totalMib > 0 ? Math.round((usedMib / totalMib) * 100) : null,
  };
}

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
          s.totalMib > 0 ? (
            `${gib(s.usedMib)} / ${gib(s.totalMib)} GiB`
          ) : (
            <span className="text-muted-foreground">—</span>
          )
        }
      />
      <StatCard
        label="Utilization"
        value={
          s.pct != null ? (
            `${s.pct}%`
          ) : (
            <span className="text-muted-foreground">—</span>
          )
        }
      />
    </div>
  );
}

/** Live HPC node/GPU resource grid, from the job's runtime resources. */
function ResourcesGrid({ job }: { job: PilotJob }) {
  const hosts = job.runtime?.resources?.hosts ?? job.resources.hosts ?? [];

  return (
    <div className="space-y-4">
      <h2 className="text-lg font-medium">Node Resources</h2>
      {hosts.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          No node resources reported.
        </p>
      ) : (
        <>
          <SummaryStrip hosts={hosts} />
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            {hosts.map((host) => (
              <NodeCard key={host.hostname} host={host} />
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
