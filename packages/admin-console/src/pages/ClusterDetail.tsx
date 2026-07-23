import { useQuery } from "@tanstack/react-query";
import { useNavigate, useParams } from "@tanstack/react-router";
import { TriangleAlert } from "lucide-react";
import { type ClusterDetail as ClusterDetailModel } from "@/lib/client";
import { clusterQueries } from "@/queries/cluster";
import { Breadcrumb } from "@/components/Breadcrumb";
import { DeploymentCounts, HealthIndicator } from "@/components/ClusterStatus";
import { JsonBlock } from "@/components/JsonBlock";
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
import { formatSince, formatTimestamp, unslug } from "@/lib/utils";

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

function PilotJobsTable({
  cluster,
  clusterSlug,
}: {
  cluster: ClusterDetailModel;
  clusterSlug: string;
}) {
  const navigate = useNavigate();
  const jobs = cluster.pilot_jobs;
  return (
    <Card>
      <CardHeader>
        <CardTitle>Pilot Jobs</CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Scheduler Job ID</TableHead>
              <TableHead>Scheduler State</TableHead>
              <TableHead>Manager URL</TableHead>
              <TableHead>Manager Health</TableHead>
              <TableHead className="text-right">Replicas</TableHead>
              <TableHead>Started</TableHead>
              <TableHead>Idle</TableHead>
              <TableHead>Nodes / GPUs</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {jobs.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={9}
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
                  <TableCell className="max-w-48 truncate font-mono text-xs text-muted-foreground">
                    {job.manager_url ?? "—"}
                  </TableCell>
                  <TableCell>
                    <HealthIndicator health={job.manager_health} />
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {job.assigned_replicas.length}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {formatTimestamp(job.time_started)}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {formatSince(job.idle_since)}
                  </TableCell>
                  <TableCell className="tabular-nums">
                    {job.num_nodes} / {job.gpus_per_node}
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
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
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-semibold">{cluster.name}</h1>
          <HealthIndicator health={cluster.health} />
        </div>
        {deploymentsQuery.data && (
          <DeploymentCounts
            staticDeployments={deploymentsQuery.data.staticDeployments}
            pilotDeployments={deploymentsQuery.data.pilotDeployments}
          />
        )}
      </div>

      {cluster.maintenance_notice && (
        <div className="flex items-start gap-2 rounded-md bg-warning/10 p-3 text-sm text-warning">
          <TriangleAlert className="mt-0.5 size-4 shrink-0" />
          <span>{cluster.maintenance_notice}</span>
        </div>
      )}

      <ConfigTable cluster={cluster} />

      {cluster.pilot_system != null && (
        <PilotJobsTable cluster={cluster} clusterSlug={clusterSlug} />
      )}
    </DetailShell>
  );
}
