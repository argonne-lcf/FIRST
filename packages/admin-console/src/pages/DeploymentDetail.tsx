import { useQuery } from "@tanstack/react-query";
import { useParams } from "@tanstack/react-router";
import { type PilotDeploymentDetail as PilotDeploymentDetailModel } from "@/lib/client";
import { deploymentQueries } from "@/queries/deployment";
import { Breadcrumb } from "@/components/Breadcrumb";
import { FieldValueTable } from "@/components/FieldValueTable";
import { ReplicaCards } from "@/components/ReplicaCards";
import { ResourceHeader } from "@/components/ResourceHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { unslug } from "@/lib/utils";

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
  order,
}: {
  title: string;
  obj: Record<string, unknown>;
  omit?: readonly string[];
  order?: readonly string[];
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <FieldValueTable obj={obj} omit={omit} order={order} />
      </CardContent>
    </Card>
  );
}

/** Important fields floated to the top of a pilot deployment's Details card. */
const PILOT_FIELD_ORDER = [
  "cluster_name",
  "model_name",
  "state",
  "desired_replicas",
  "min_replicas",
  "max_replicas",
  "consecutive_launch_failures",
  "reconcile_failures",
  "reconcile_last_error",
  "reconcile_retry_at",
] as const;

/** Important fields floated to the top of a static deployment's Details card. */
const STATIC_FIELD_ORDER = [
  "cluster_name",
  "model_name",
  "health",
  "runtime",
  "api_url",
  "upstream_model_name",
] as const;

function ReplicaGrid({
  deployment,
}: {
  deployment: PilotDeploymentDetailModel;
}) {
  return (
    <div className="space-y-3">
      <h2 className="text-lg font-medium">Replicas</h2>
      <ReplicaCards replicas={deployment.replicas} />
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
      <ResourceHeader
        kind={deployment.kind}
        name={deployment.name}
        uid={deployment.uid}
      />
      <ReplicaGrid deployment={deployment} />
      <DetailsCard
        title="Details"
        obj={deployment}
        omit={["kind", "name", "uid", "replicas"]}
        order={PILOT_FIELD_ORDER}
      />
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
      <ResourceHeader
        kind={deployment.kind}
        name={deployment.name}
        uid={deployment.uid}
      />
      <DetailsCard
        title="Details"
        obj={deployment}
        omit={["kind", "name", "uid"]}
        order={STATIC_FIELD_ORDER}
      />
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
