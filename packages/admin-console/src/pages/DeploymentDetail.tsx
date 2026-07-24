import { useParams } from "@tanstack/react-router";
import { Breadcrumb } from "@/components/Breadcrumb";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { unslug } from "@/lib/utils";

function PilotDeploymentDetail({ name }: { name: string }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{name}</CardTitle>
      </CardHeader>
      <CardContent className="text-sm text-muted-foreground">
        PilotDeployment detail coming soon.
      </CardContent>
    </Card>
  );
}

function StaticDeploymentDetail({ name }: { name: string }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{name}</CardTitle>
      </CardHeader>
      <CardContent className="text-sm text-muted-foreground">
        StaticDeployment detail coming soon.
      </CardContent>
    </Card>
  );
}

export function DeploymentDetail() {
  const { kind, deploymentSlug } = useParams({
    from: "/shell/deployments/$kind/$deploymentSlug",
  });
  const name = unslug(deploymentSlug);

  return (
    <>
      <Breadcrumb
        items={[
          { label: "Deployments", link: { to: "/deployments" } },
          { label: name },
        ]}
      />
      <main className="flex-1 space-y-6 p-6">
        {kind === "pilot" ? (
          <PilotDeploymentDetail name={name} />
        ) : (
          <StaticDeploymentDetail name={name} />
        )}
      </main>
    </>
  );
}
