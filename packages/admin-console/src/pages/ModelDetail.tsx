import { useQuery } from "@tanstack/react-query";
import { useParams } from "@tanstack/react-router";
import { modelQueries } from "@/queries/models";
import { Breadcrumb } from "@/components/Breadcrumb";
import { DeploymentRow, type Deployment } from "@/components/DeploymentRow";
import { JsonBlock } from "@/components/JsonBlock";
import { ResourceHeader } from "@/components/ResourceHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { unslug } from "@/lib/utils";

function DetailShell({
  modelName,
  children,
}: {
  modelName: string;
  children: React.ReactNode;
}) {
  return (
    <>
      <Breadcrumb
        items={[
          { label: "Models", link: { to: "/models" } },
          { label: modelName },
        ]}
      />
      <main className="flex-1 space-y-6 p-6">{children}</main>
    </>
  );
}

export function ModelDetail() {
  const { modelSlug } = useParams({ from: "/shell/models/$modelSlug" });
  const name = unslug(modelSlug);

  const modelsQuery = useQuery(modelQueries.all());
  const routerConfigQuery = useQuery(modelQueries.routerConfig());

  if (modelsQuery.isPending) {
    return (
      <DetailShell modelName={name}>
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-48" />
        <Skeleton className="h-48" />
      </DetailShell>
    );
  }

  if (modelsQuery.isError) {
    return (
      <DetailShell modelName={name}>
        <Card>
          <CardHeader>
            <CardTitle>Model</CardTitle>
            <CardContent className="px-0 text-sm text-destructive">
              Failed to load model:{" "}
              {modelsQuery.error instanceof Error
                ? modelsQuery.error.message
                : "unknown error"}
            </CardContent>
          </CardHeader>
        </Card>
      </DetailShell>
    );
  }

  const model = modelsQuery.data.find((m) => m.slug === modelSlug);

  if (!model) {
    return (
      <DetailShell modelName={name}>
        <Card>
          <CardHeader>
            <CardTitle>Model</CardTitle>
            <CardContent className="px-0 text-sm text-muted-foreground">
              No model found named {name}.
            </CardContent>
          </CardHeader>
        </Card>
      </DetailShell>
    );
  }

  const modelConfig = routerConfigQuery.data?.models?.find(
    (m) => m.name === model.name,
  );

  const deployments: Deployment[] = [
    ...model.static_deployments.map((summary): Deployment => ({
      kind: "static",
      state: summary.health,
      summary,
    })),
    ...model.pilot_deployments.map((summary): Deployment => ({
      kind: "pilot",
      state: summary.state,
      summary,
    })),
  ];

  return (
    <DetailShell modelName={model.name}>
      <ResourceHeader kind={model.kind} name={model.name} uid={model.uid} />

      <Card>
        <CardHeader>
          <CardTitle>Deployments</CardTitle>
        </CardHeader>
        <CardContent>
          {deployments.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No deployments for this model.
            </p>
          ) : (
            deployments.map((d) => (
              <DeploymentRow
                key={`${d.kind}-${d.summary.uid}`}
                deployment={d}
              />
            ))
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Router Config</CardTitle>
        </CardHeader>
        <CardContent>
          <JsonBlock value={modelConfig} />
        </CardContent>
      </Card>
    </DetailShell>
  );
}
