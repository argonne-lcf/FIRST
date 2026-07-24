import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";
import {
  type PilotDeploymentDetail as PilotDeploymentDetailModel,
  type PilotReplica,
} from "@/lib/client";
import { deploymentQueries } from "@/queries/deployment";
import { Breadcrumb } from "@/components/Breadcrumb";
import { FieldValueTable } from "@/components/FieldValueTable";
import { ReplicaRuntime, ReplicaStateBadge } from "@/components/ReplicaStatus";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { formatAgo, unslug } from "@/lib/utils";

function DetailShell({
  name,
  children,
}: {
  name: string;
  children: React.ReactNode;
}) {
  return (
    <>
      <Breadcrumb
        items={[
          { label: "Deployments", link: { to: "/deployments" } },
          { label: name },
        ]}
      />
      <main className="flex-1 space-y-6 p-6">{children}</main>
    </>
  );
}

function LoadState({ name }: { name: string }) {
  return (
    <DetailShell name={name}>
      <Skeleton className="h-8 w-64" />
      <Skeleton className="h-64" />
    </DetailShell>
  );
}

function ErrorState({ name, error }: { name: string; error: unknown }) {
  return (
    <DetailShell name={name}>
      <Card>
        <CardHeader>
          <CardTitle>Deployment</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-destructive">
          Failed to load deployment:{" "}
          {error instanceof Error ? error.message : "unknown error"}
        </CardContent>
      </Card>
    </DetailShell>
  );
}

function DetailsCard({
  title,
  obj,
  omit,
}: {
  title: string;
  obj: Record<string, unknown>;
  omit?: readonly string[];
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <FieldValueTable obj={obj} omit={omit} />
      </CardContent>
    </Card>
  );
}

/** The replica's most recent status change, labelled with its verb. */
function statusChange(replica: PilotReplica): { verb: string; at: string } {
  if (replica.stopped_at) return { verb: "Stopped", at: replica.stopped_at };
  if (replica.started_at) return { verb: "Started", at: replica.started_at };
  return { verb: "Created", at: replica.created_at };
}

function ReplicaCard({
  replica,
  deploymentSlug,
}: {
  replica: PilotReplica;
  deploymentSlug: string;
}) {
  const status = statusChange(replica);
  return (
    <Card size="sm" className="relative">
      <CardHeader className="gap-2">
        <div className="flex items-center justify-between gap-2">
          <CardTitle className="truncate">{replica.name}</CardTitle>
          <ReplicaStateBadge state={replica.state} />
        </div>
        <div className="font-mono text-xs text-muted-foreground">
          #{replica.uid}
        </div>
      </CardHeader>
      <CardContent className="space-y-2 text-sm">
        <p className="text-muted-foreground">{replica.state_message}</p>
        <p className="text-xs text-muted-foreground">
          {status.verb} {formatAgo(status.at)}
        </p>
        <div className="text-xs">
          <ReplicaRuntime runtime={replica.runtime} />
        </div>
      </CardContent>
      <Button
        asChild
        variant="ghost"
        size="icon-sm"
        className="absolute right-2 bottom-2"
        aria-label={`View ${replica.name}`}
      >
        <Link
          to="/deployments/pilot/$deploymentSlug/replicas/$replicaSlug"
          params={{ deploymentSlug, replicaSlug: replica.slug }}
        >
          <ArrowRight />
        </Link>
      </Button>
    </Card>
  );
}

function ReplicaCards({
  replicas,
  deploymentSlug,
}: {
  replicas: PilotReplica[];
  deploymentSlug: string;
}) {
  if (replicas.length === 0) {
    return <p className="text-sm text-muted-foreground">No replicas.</p>;
  }
  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {replicas.map((replica) => (
        <ReplicaCard
          key={replica.uid}
          replica={replica}
          deploymentSlug={deploymentSlug}
        />
      ))}
    </div>
  );
}

function ReplicaGrid({
  deployment,
}: {
  deployment: PilotDeploymentDetailModel;
}) {
  const active = deployment.replicas.filter((r) => !r.deleted_at);
  const deleted = deployment.replicas.filter((r) => r.deleted_at);

  return (
    <div className="space-y-3">
      <h2 className="text-lg font-medium">Replicas</h2>
      <Tabs defaultValue="active">
        <TabsList>
          <TabsTrigger value="active">Active ({active.length})</TabsTrigger>
          <TabsTrigger value="deleted">Deleted ({deleted.length})</TabsTrigger>
        </TabsList>
        <TabsContent value="active">
          <ReplicaCards replicas={active} deploymentSlug={deployment.slug} />
        </TabsContent>
        <TabsContent value="deleted">
          <ReplicaCards replicas={deleted} deploymentSlug={deployment.slug} />
        </TabsContent>
      </Tabs>
    </div>
  );
}

function PilotDeploymentDetail({ name }: { name: string }) {
  const query = useQuery(deploymentQueries.pilot(name));

  if (query.isPending) return <LoadState name={name} />;
  if (query.isError) return <ErrorState name={name} error={query.error} />;

  const deployment = query.data;
  return (
    <DetailShell name={deployment.name}>
      <h1 className="text-2xl font-semibold">{deployment.name}</h1>
      <DetailsCard title="Details" obj={deployment} omit={["replicas"]} />
      <ReplicaGrid deployment={deployment} />
    </DetailShell>
  );
}

function StaticDeploymentDetail({ name }: { name: string }) {
  const query = useQuery(deploymentQueries.statics());

  if (query.isPending) return <LoadState name={name} />;
  if (query.isError) return <ErrorState name={name} error={query.error} />;

  const deployment = query.data.find((d) => d.name === name);
  if (!deployment) {
    return <ErrorState name={name} error={new Error("Deployment not found")} />;
  }
  return (
    <DetailShell name={deployment.name}>
      <h1 className="text-2xl font-semibold">{deployment.name}</h1>
      <DetailsCard title="Details" obj={deployment} />
    </DetailShell>
  );
}

export function DeploymentDetail() {
  const { kind, deploymentSlug } = useParams({
    from: "/shell/deployments/$kind/$deploymentSlug",
  });
  const name = unslug(deploymentSlug);

  return kind === "pilot" ? (
    <PilotDeploymentDetail name={name} />
  ) : (
    <StaticDeploymentDetail name={name} />
  );
}
