import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { ArrowRight, TriangleAlert } from "lucide-react";
import { clusterQueries, type ClusterOverview } from "@/queries/cluster";
import { DeploymentCounts, HealthIndicator } from "@/components/ClusterStatus";
import {
  Card,
  CardAction,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";

function ClusterCard({ overview }: { overview: ClusterOverview }) {
  const { cluster, staticDeployments, pilotDeployments } = overview;
  return (
    <Card>
      <CardHeader>
        <CardTitle>{cluster.name}</CardTitle>
        <CardAction>
          <HealthIndicator health={cluster.health} />
        </CardAction>
      </CardHeader>
      <CardContent className="space-y-3">
        {cluster.maintenance_notice && (
          <div className="flex items-start gap-2 rounded-md bg-warning/10 p-2 text-xs text-warning">
            <TriangleAlert className="mt-0.5 size-4 shrink-0" />
            <span>{cluster.maintenance_notice}</span>
          </div>
        )}
        <DeploymentCounts
          staticDeployments={staticDeployments}
          pilotDeployments={pilotDeployments}
        />
        <div className="flex justify-end">
          <Button asChild variant="ghost" size="sm">
            <Link
              to="/clusters/$clusterSlug"
              params={{ clusterSlug: cluster.slug }}
            >
              View
              <ArrowRight data-icon="inline-end" />
            </Link>
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

export function Clusters() {
  const { data, isPending, isError, error } = useQuery(
    clusterQueries.overview(),
  );

  if (isPending) {
    return (
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {Array.from({ length: 6 }).map((_, i) => (
          <Skeleton key={i} className="h-32" />
        ))}
      </div>
    );
  }

  if (isError) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Clusters</CardTitle>
          <CardContent className="px-0 text-sm text-destructive">
            Failed to load clusters: {error.message}
          </CardContent>
        </CardHeader>
      </Card>
    );
  }

  if (data.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Clusters</CardTitle>
          <CardContent className="px-0 text-sm text-muted-foreground">
            No clusters configured.
          </CardContent>
        </CardHeader>
      </Card>
    );
  }

  return (
    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {data.map((overview) => (
        <ClusterCard key={overview.cluster.uid} overview={overview} />
      ))}
    </div>
  );
}
