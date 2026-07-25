import { Link } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";
import { type PilotReplica } from "@/lib/client";
import { ReplicaRuntime, ReplicaStateBadge } from "@/components/ReplicaStatus";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { formatAgo, slugify } from "@/lib/utils";

/** The replica's most recent status change, labelled with its verb. */
function statusChange(replica: PilotReplica): { verb: string; at: string } {
  if (replica.stopped_at) return { verb: "Stopped", at: replica.stopped_at };
  if (replica.started_at) return { verb: "Started", at: replica.started_at };
  return { verb: "Created", at: replica.created_at };
}

function ReplicaCard({ replica }: { replica: PilotReplica }) {
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
          params={{
            deploymentSlug: slugify(replica.pilot_deployment_name),
            replicaSlug: replica.slug,
          }}
        >
          <ArrowRight />
        </Link>
      </Button>
    </Card>
  );
}

function ReplicaCardGrid({ replicas }: { replicas: PilotReplica[] }) {
  if (replicas.length === 0) {
    return <p className="text-sm text-muted-foreground">No replicas.</p>;
  }
  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {replicas.map((replica) => (
        <ReplicaCard key={replica.uid} replica={replica} />
      ))}
    </div>
  );
}

/**
 * Tabbed Active/Deleted card grid over a set of replicas. Each card links to
 * its replica's detail view, deriving the deployment slug per replica so a
 * pilot job hosting replicas from several deployments links correctly.
 */
export function ReplicaCards({ replicas }: { replicas: PilotReplica[] }) {
  const active = replicas.filter((r) => !r.deleted_at);
  const deleted = replicas.filter((r) => r.deleted_at);

  return (
    <Tabs defaultValue="active">
      <TabsList>
        <TabsTrigger value="active">Active ({active.length})</TabsTrigger>
        <TabsTrigger value="deleted">Deleted ({deleted.length})</TabsTrigger>
      </TabsList>
      <TabsContent value="active">
        <ReplicaCardGrid replicas={active} />
      </TabsContent>
      <TabsContent value="deleted">
        <ReplicaCardGrid replicas={deleted} />
      </TabsContent>
    </Tabs>
  );
}
