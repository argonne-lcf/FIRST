import { useParams } from "@tanstack/react-router";
import { Breadcrumb } from "@/components/Breadcrumb";
import { unslug } from "@/lib/utils";

export function PilotJobDetail() {
  const { clusterSlug, jobSlug } = useParams({
    from: "/shell/clusters/$clusterSlug/jobs/$jobSlug",
  });
  const clusterName = unslug(clusterSlug);
  const jobName = unslug(jobSlug);

  return (
    <>
      <Breadcrumb
        items={[
          { label: "Clusters", link: { to: "/clusters" } },
          {
            label: clusterName,
            link: {
              to: "/clusters/$clusterSlug",
              params: { clusterSlug },
            },
          },
          { label: jobName },
        ]}
      />
      <main className="flex-1 space-y-6 p-6">
        <h1 className="text-2xl font-semibold">{jobName}</h1>
        <p className="text-sm text-muted-foreground">Details coming soon.</p>
      </main>
    </>
  );
}
