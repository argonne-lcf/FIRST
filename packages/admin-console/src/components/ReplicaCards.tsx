import { Link } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";
import { type PilotReplica } from "@/lib/client";
import { ReplicaRuntime, ReplicaStateBadge } from "@/components/ReplicaStatus";
import { ResourceLink } from "@/components/ResourceLink";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { resourceLinks } from "@/lib/links";
import { formatAgo } from "@/lib/utils";

/** The replica's most recent status change, labelled with its verb. */
function statusChange(replica: PilotReplica): { verb: string; at: string } {
  if (replica.stopped_at) return { verb: "Stopped", at: replica.stopped_at };
  if (replica.started_at) return { verb: "Started", at: replica.started_at };
  return { verb: "Created", at: replica.created_at };
}

function ReplicaCard({
  replica,
  clusterName,
}: {
  replica: PilotReplica;
  clusterName: string;
}) {
  const status = statusChange(replica);
  const link = resourceLinks.replica(
    replica.pilot_deployment_name,
    replica.name,
  );
  return (
    <Card size="sm" className="relative">
      <CardHeader className="gap-2">
        <div className="flex items-center justify-between gap-2">
          <CardTitle className="truncate">
            <ResourceLink link={link}>{replica.name}</ResourceLink>
          </CardTitle>
          <ReplicaStateBadge state={replica.state} />
        </div>
        <div className="font-mono text-xs text-muted-foreground">
          #{replica.uid}
        </div>
      </CardHeader>
      <CardContent className="space-y-2 text-sm">
        <dl className="grid grid-cols-[auto_1fr] gap-x-2 text-xs">
          <dt className="text-muted-foreground">Deployment</dt>
          <dd className="truncate">
            <ResourceLink
              link={resourceLinks.deployment(
                "pilot",
                replica.pilot_deployment_name,
              )}
            >
              {replica.pilot_deployment_name}
            </ResourceLink>
          </dd>
          <dt className="text-muted-foreground">Pilot Job</dt>
          <dd className="truncate">
            {replica.pilot_job_name ? (
              <ResourceLink
                link={resourceLinks.pilotJob(
                  clusterName,
                  replica.pilot_job_name,
                )}
              >
                {replica.pilot_job_name}
              </ResourceLink>
            ) : (
              <span className="text-muted-foreground">—</span>
            )}
          </dd>
        </dl>
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
        <Link {...link}>
          <ArrowRight />
        </Link>
      </Button>
    </Card>
  );
}

function ReplicaCardGrid({
  replicas,
  clusterName,
}: {
  replicas: PilotReplica[];
  clusterName: string;
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
          clusterName={clusterName}
        />
      ))}
    </div>
  );
}

/**
 * Tabbed Active/Deleted card grid over a set of replicas. Each card links to
 * its replica's detail view, deriving the deployment slug per replica so a
 * pilot job hosting replicas from several deployments links correctly.
 * `clusterName` is the cluster all `replicas` run on, used to link each
 * replica's pilot job.
 */
export function ReplicaCards({
  replicas,
  clusterName,
}: {
  replicas: PilotReplica[];
  clusterName: string;
}) {
  const active = replicas.filter((r) => !r.deleted_at);
  const deleted = replicas.filter((r) => r.deleted_at);

  return (
    <Tabs defaultValue="active">
      <TabsList>
        <TabsTrigger value="active">Active ({active.length})</TabsTrigger>
        <TabsTrigger value="deleted">Deleted ({deleted.length})</TabsTrigger>
      </TabsList>
      <TabsContent value="active">
        <ReplicaCardGrid replicas={active} clusterName={clusterName} />
      </TabsContent>
      <TabsContent value="deleted">
        <ReplicaCardGrid replicas={deleted} clusterName={clusterName} />
      </TabsContent>
    </Tabs>
  );
}
