import { useQuery } from "@tanstack/react-query";
import { useParams } from "@tanstack/react-router";
import { RefreshCw } from "lucide-react";
import { deploymentQueries } from "@/queries/deployment";
import { Breadcrumb } from "@/components/Breadcrumb";
import { FieldValueTable } from "@/components/FieldValueTable";
import { ReconcileBanner } from "@/components/ReconcileBanner";
import { ReplicaRuntime, ReplicaStateBadge } from "@/components/ReplicaStatus";
import { ResourceHeader } from "@/components/ResourceHeader";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { cn, unslug } from "@/lib/utils";

function DetailShell({
  deploymentSlug,
  deploymentName,
  replicaName,
  children,
}: {
  deploymentSlug: string;
  deploymentName: string;
  replicaName: string;
  children: React.ReactNode;
}) {
  return (
    <>
      <Breadcrumb
        items={[
          { label: "Deployments", link: { to: "/deployments" } },
          {
            label: deploymentName,
            link: {
              to: "/deployments/$kind/$deploymentSlug",
              params: { kind: "pilot", deploymentSlug },
            },
          },
          { label: replicaName },
        ]}
      />
      <main className="flex-1 space-y-6 p-6">{children}</main>
    </>
  );
}

/** TTY-styled, scrollable viewer that fetches the log tail on demand. */
function LogViewer({
  replicaName,
  replicaSlug,
}: {
  replicaName: string;
  replicaSlug: string;
}) {
  const query = useQuery(deploymentQueries.replicaLogs(replicaSlug));

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle>Logs</CardTitle>
        <Button
          variant="outline"
          size="sm"
          disabled={query.isFetching}
          onClick={() => void query.refetch()}
        >
          <RefreshCw className={cn(query.isFetching && "animate-spin")} />
          {query.isFetching ? "Fetching…" : "Fetch tail"}
        </Button>
      </CardHeader>
      <CardContent>
        <div className="overflow-hidden rounded-lg border border-border bg-zinc-950">
          <div className="flex items-center gap-1.5 border-b border-white/10 px-3 py-2">
            <span className="size-3 rounded-full bg-red-500" />
            <span className="size-3 rounded-full bg-yellow-500" />
            <span className="size-3 rounded-full bg-green-500" />
            <span className="ml-2 font-mono text-xs text-zinc-400">
              {replicaName} — last 300 lines
            </span>
          </div>
          <pre className="max-h-96 overflow-auto p-4 font-mono text-xs leading-relaxed text-zinc-100">
            {query.isError
              ? `Failed to fetch logs: ${
                  query.error instanceof Error
                    ? query.error.message
                    : "unknown error"
                }`
              : query.data
                ? query.data
                : query.isFetching
                  ? "Fetching logs…"
                  : "Press “Fetch tail” to load the latest logs."}
          </pre>
        </div>
      </CardContent>
    </Card>
  );
}

export function ReplicaDetail() {
  const { deploymentSlug, replicaSlug } = useParams({
    from: "/shell/deployments/pilot/$deploymentSlug/replicas/$replicaSlug",
  });
  const deploymentName = unslug(deploymentSlug);
  const replicaName = unslug(replicaSlug);

  const query = useQuery(deploymentQueries.pilot(deploymentName));

  if (query.isPending) {
    return (
      <DetailShell
        deploymentSlug={deploymentSlug}
        deploymentName={deploymentName}
        replicaName={replicaName}
      >
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-64" />
      </DetailShell>
    );
  }

  if (query.isError) {
    return (
      <DetailShell
        deploymentSlug={deploymentSlug}
        deploymentName={deploymentName}
        replicaName={replicaName}
      >
        <Card>
          <CardHeader>
            <CardTitle>Replica</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-destructive">
            Failed to load deployment:{" "}
            {query.error instanceof Error
              ? query.error.message
              : "unknown error"}
          </CardContent>
        </Card>
      </DetailShell>
    );
  }

  const replica = query.data.replicas.find((r) => r.slug === replicaSlug);
  if (!replica) {
    return (
      <DetailShell
        deploymentSlug={deploymentSlug}
        deploymentName={deploymentName}
        replicaName={replicaName}
      >
        <Card>
          <CardHeader>
            <CardTitle>Replica</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-destructive">
            Replica not found.
          </CardContent>
        </Card>
      </DetailShell>
    );
  }

  return (
    <DetailShell
      deploymentSlug={deploymentSlug}
      deploymentName={query.data.name}
      replicaName={replica.name}
    >
      <ResourceHeader kind={replica.kind} name={replica.name} uid={replica.uid}>
        <ReplicaStateBadge state={replica.state} />
      </ResourceHeader>

      <ReconcileBanner resource={replica} />

      <Card>
        <CardHeader>
          <CardTitle>Runtime</CardTitle>
        </CardHeader>
        <CardContent className="text-sm">
          <ReplicaRuntime runtime={replica.runtime} />
        </CardContent>
      </Card>

      <LogViewer replicaName={replica.name} replicaSlug={replica.slug} />

      <Card>
        <CardHeader>
          <CardTitle>Details</CardTitle>
        </CardHeader>
        <CardContent>
          <FieldValueTable
            obj={replica}
            omit={["kind", "name", "uid", "runtime"]}
          />
        </CardContent>
      </Card>
    </DetailShell>
  );
}
