import { useQuery } from "@tanstack/react-query";
import { useNavigate, useParams } from "@tanstack/react-router";
import { TriangleAlert } from "lucide-react";
import {
  type ClusterDetail as ClusterDetailModel,
  type PilotJob,
  type ReplicaState,
} from "@/lib/client";
import { clusterQueries } from "@/queries/cluster";
import { Breadcrumb } from "@/components/Breadcrumb";
import {
  DeploymentCounts,
  HealthDescription,
  HealthIndicator,
} from "@/components/ClusterStatus";
import { JsonBlock } from "@/components/JsonBlock";
import { ResourceHeader } from "@/components/ResourceHeader";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { formatTimestamp, unslug } from "@/lib/utils";

function DetailShell({
  clusterName,
  children,
}: {
  clusterName: string;
  children: React.ReactNode;
}) {
  return (
    <>
      <Breadcrumb
        items={[
          { label: "Clusters", link: { to: "/clusters" } },
          { label: clusterName },
        ]}
      />
      <main className="flex-1 space-y-6 p-6">{children}</main>
    </>
  );
}

function ConfigTable({ cluster }: { cluster: ClusterDetailModel }) {
  const rows: { field: string; value: unknown }[] = [
    { field: "health_check", value: cluster.health_check },
    { field: "maintenance_notice", value: cluster.maintenance_notice },
    { field: "pilot_system", value: cluster.pilot_system },
  ];
  return (
    <Card>
      <CardHeader>
        <CardTitle>Configuration</CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-48">Field</TableHead>
              <TableHead>Content</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <TableRow key={row.field}>
                <TableCell className="align-top font-mono text-xs text-muted-foreground">
                  {row.field}
                </TableCell>
                <TableCell className="w-full">
                  <JsonBlock value={row.value} />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

/** Display order for replica states in the coalesced Status column. */
const REPLICA_STATE_ORDER: readonly ReplicaState[] = [
  "ready",
  "launching",
  "placed",
  "pending",
  "unhealthy",
  "start_timeout",
  "error",
  "terminating",
  "terminated",
];

/** Long-form elapsed time since an ISO timestamp, e.g. "2 hours" or "14 minutes". */
function formatIdleDuration(iso: string): string {
  const seconds = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  const hours = Math.floor(seconds / 3600);
  if (hours > 0) return `${hours} hour${hours === 1 ? "" : "s"}`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes} minute${minutes === 1 ? "" : "s"}`;
}

/**
 * Coalesced status for a pilot job: a per-state replica tally (ignoring
 * soft-deleted replicas), else an idle notice, else "—".
 */
function JobStatus({ job }: { job: PilotJob }) {
  const liveReplicas = job.assigned_replicas.filter((r) => !r.deleted_at);
  if (liveReplicas.length > 0) {
    const summary = REPLICA_STATE_ORDER.map((state) => ({
      state,
      count: liveReplicas.filter((r) => r.state === state).length,
    }))
      .filter((entry) => entry.count > 0)
      .map((entry) => `${entry.count} ${entry.state.replace(/_/g, " ")}`)
      .join(" | ");
    return <span className="tabular-nums">{summary}</span>;
  }
  if (job.idle_since) {
    return (
      <span className="text-muted-foreground">
        Idle for {formatIdleDuration(job.idle_since)}
      </span>
    );
  }
  return <span className="text-muted-foreground">—</span>;
}

function PilotJobsTableBody({
  jobs,
  clusterSlug,
}: {
  jobs: PilotJob[];
  clusterSlug: string;
}) {
  const navigate = useNavigate();
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Name</TableHead>
          <TableHead>Scheduler Job ID</TableHead>
          <TableHead>Scheduler State</TableHead>
          <TableHead>Manager Health</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Created</TableHead>
          <TableHead>Nodes / GPUs</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {jobs.length === 0 ? (
          <TableRow>
            <TableCell
              colSpan={7}
              className="text-center text-muted-foreground"
            >
              No pilot jobs.
            </TableCell>
          </TableRow>
        ) : (
          jobs.map((job) => (
            <TableRow
              key={job.uid}
              className="cursor-pointer"
              onClick={() => {
                void navigate({
                  to: "/clusters/$clusterSlug/jobs/$jobSlug",
                  params: { clusterSlug, jobSlug: job.slug },
                });
              }}
            >
              <TableCell className="font-medium">{job.name}</TableCell>
              <TableCell className="font-mono text-xs">
                {job.scheduler_job_id}
              </TableCell>
              <TableCell>
                <Badge variant="outline">
                  {job.scheduler_state.replace(/_/g, " ")}
                </Badge>
              </TableCell>
              <TableCell>
                <HealthIndicator health={job.manager_health} />
                {job.manager_url && (
                  <div className="max-w-48 truncate font-mono text-xs text-muted-foreground">
                    {job.manager_url}
                  </div>
                )}
              </TableCell>
              <TableCell>
                <JobStatus job={job} />
              </TableCell>
              <TableCell className="text-xs text-muted-foreground">
                {formatTimestamp(job.created_at)}
                {job.time_started && (
                  <div className="text-muted-foreground/70">
                    Started {formatTimestamp(job.time_started)}
                  </div>
                )}
              </TableCell>
              <TableCell className="tabular-nums">
                {job.num_nodes} / {job.gpus_per_node}
              </TableCell>
            </TableRow>
          ))
        )}
      </TableBody>
    </Table>
  );
}

function PilotJobsTable({
  cluster,
  clusterSlug,
}: {
  cluster: ClusterDetailModel;
  clusterSlug: string;
}) {
  const byNewest = (a: PilotJob, b: PilotJob) =>
    new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
  const active = cluster.pilot_jobs.filter((j) => !j.deleted_at).sort(byNewest);
  const deleted = cluster.pilot_jobs.filter((j) => j.deleted_at).sort(byNewest);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Pilot Jobs</CardTitle>
      </CardHeader>
      <CardContent>
        <Tabs defaultValue="active">
          <TabsList>
            <TabsTrigger value="active">Active ({active.length})</TabsTrigger>
            <TabsTrigger value="deleted">
              Deleted ({deleted.length})
            </TabsTrigger>
          </TabsList>
          <TabsContent value="active">
            <PilotJobsTableBody jobs={active} clusterSlug={clusterSlug} />
          </TabsContent>
          <TabsContent value="deleted">
            <PilotJobsTableBody jobs={deleted} clusterSlug={clusterSlug} />
          </TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  );
}

export function ClusterDetail() {
  const { clusterSlug } = useParams({ from: "/shell/clusters/$clusterSlug" });
  const name = unslug(clusterSlug);

  const clusterQuery = useQuery(clusterQueries.detail(name));
  const deploymentsQuery = useQuery(clusterQueries.deploymentsFor(name));

  if (clusterQuery.isPending) {
    return (
      <DetailShell clusterName={name}>
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-48" />
        <Skeleton className="h-48" />
      </DetailShell>
    );
  }

  if (clusterQuery.isError) {
    return (
      <DetailShell clusterName={name}>
        <Card>
          <CardHeader>
            <CardTitle>Cluster</CardTitle>
            <CardContent className="px-0 text-sm text-destructive">
              Failed to load cluster:{" "}
              {clusterQuery.error instanceof Error
                ? clusterQuery.error.message
                : "unknown error"}
            </CardContent>
          </CardHeader>
        </Card>
      </DetailShell>
    );
  }

  const cluster = clusterQuery.data;

  return (
    <DetailShell clusterName={cluster.name}>
      <div className="flex items-start justify-between">
        <ResourceHeader
          kind={cluster.kind}
          name={cluster.name}
          uid={cluster.uid}
        >
          <HealthIndicator health={cluster.health} />
        </ResourceHeader>
        {deploymentsQuery.data && (
          <DeploymentCounts
            staticDeployments={deploymentsQuery.data.staticDeployments}
            pilotDeployments={deploymentsQuery.data.pilotDeployments}
          />
        )}
      </div>

      <HealthDescription health={cluster.health} />

      {cluster.pilot_system != null && (
        <PilotJobsTable cluster={cluster} clusterSlug={clusterSlug} />
      )}

      {cluster.maintenance_notice && (
        <div className="flex items-start gap-2 rounded-md bg-warning/10 p-3 text-sm text-warning">
          <TriangleAlert className="mt-0.5 size-4 shrink-0" />
          <span>{cluster.maintenance_notice}</span>
        </div>
      )}

      <ConfigTable cluster={cluster} />
    </DetailShell>
  );
}
