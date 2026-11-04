import { useQuery } from "@tanstack/react-query";
import { type LinkProps, useParams } from "@tanstack/react-router";
import { type PilotDeploymentDetail as PilotDeploymentDetailModel } from "@/lib/client";
import { deploymentQueries } from "@/queries/deployment";
import { launchTemplateQueries } from "@/queries/launchTemplate";
import { Breadcrumb } from "@/components/Breadcrumb";
import {
  HealthDescription,
  HealthIndicator,
  PilotStateDescription,
  PilotStateIndicator,
} from "@/components/ClusterStatus";
import { FieldValueTable } from "@/components/FieldValueTable";
import { ReconcileBanner } from "@/components/ReconcileBanner";
import { ReplicaCards } from "@/components/ReplicaCards";
import { ResourceHeader } from "@/components/ResourceHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { resourceLinks } from "@/lib/links";
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
  links,
}: {
  title: string;
  obj: Record<string, unknown>;
  omit?: readonly string[];
  order?: readonly string[];
  links?: Partial<Record<string, LinkProps>>;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <FieldValueTable obj={obj} omit={omit} order={order} links={links} />
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
      <ReplicaCards
        replicas={deployment.replicas}
        clusterName={deployment.cluster_name}
      />
    </div>
  );
}

/** The LaunchTemplate this pilot deployment renders its replicas from. */
function LaunchTemplateCard({ name }: { name: string }) {
  const query = useQuery(launchTemplateQueries.byName(name));
  return (
    <Card id="launch-template" className="scroll-mt-6">
      <CardHeader>
        <CardTitle>
          Launch Template{" "}
          <span className="font-mono font-normal text-muted-foreground">
            {name}
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {query.isPending ? (
          <Skeleton className="h-48" />
        ) : query.isError ? (
          <p className="text-sm text-destructive">
            Failed to load launch template:{" "}
            {query.error instanceof Error
              ? query.error.message
              : "unknown error"}
          </p>
        ) : query.data === null ? (
          <p className="text-sm text-muted-foreground">
            Launch template not found.
          </p>
        ) : (
          <FieldValueTable
            obj={query.data}
            omit={["kind", "name"]}
            order={[
              "serve_script_template",
              "parameters",
              "env",
              "health_check",
            ]}
          />
        )}
      </CardContent>
    </Card>
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
        related={[
          {
            label: "Cluster",
            name: deployment.cluster_name,
            link: resourceLinks.cluster(deployment.cluster_name),
          },
          {
            label: "Model",
            name: deployment.model_name,
            link: resourceLinks.model(deployment.model_name),
          },
          {
            label: "Launch Template",
            name: deployment.launch_template_name,
            link: resourceLinks.launchTemplate(deployment.name),
          },
        ]}
      >
        <PilotStateIndicator state={deployment.state} />
      </ResourceHeader>
      <PilotStateDescription
        state={deployment.state}
        desiredReplicas={deployment.desired_replicas}
      />
      <ReconcileBanner resource={deployment} />
      <ReplicaGrid deployment={deployment} />
      <DetailsCard
        title="Details"
        obj={deployment}
        omit={["kind", "name", "uid", "replicas", "reconcile_last_traceback"]}
        order={PILOT_FIELD_ORDER}
        links={{
          cluster_name: resourceLinks.cluster(deployment.cluster_name),
          model_name: resourceLinks.model(deployment.model_name),
          launch_template_name: resourceLinks.launchTemplate(deployment.name),
        }}
      />
      <LaunchTemplateCard name={deployment.launch_template_name} />
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
        related={[
          {
            label: "Cluster",
            name: deployment.cluster_name,
            link: resourceLinks.cluster(deployment.cluster_name),
          },
          {
            label: "Model",
            name: deployment.model_name,
            link: resourceLinks.model(deployment.model_name),
          },
        ]}
      >
        <HealthIndicator health={deployment.health} />
      </ResourceHeader>
      <HealthDescription
        health={deployment.health}
        detail={deployment.health_detail}
      />
      <DetailsCard
        title="Details"
        obj={deployment}
        omit={[
          "kind",
          "name",
          "uid",
          "reconcile_last_traceback",
          "health_detail",
        ]}
        order={STATIC_FIELD_ORDER}
        links={{
          cluster_name: resourceLinks.cluster(deployment.cluster_name),
          model_name: resourceLinks.model(deployment.model_name),
        }}
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
