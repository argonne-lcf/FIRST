import { Link } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";
import {
  type HealthCheckResult,
  type PilotDeploymentState,
  type PilotDeploymentSummary,
  type StaticDeploymentSummary,
} from "@/lib/client";
import { ResourceLink } from "@/components/ResourceLink";
import { Button } from "@/components/ui/button";
import { resourceLinks } from "@/lib/links";

/** The union of static health results and pilot deployment states. */
export type DeploymentState = HealthCheckResult | PilotDeploymentState;

/** A static or pilot deployment flattened to what the list view needs. */
export type Deployment =
  | { kind: "static"; state: DeploymentState; summary: StaticDeploymentSummary }
  | { kind: "pilot"; state: DeploymentState; summary: PilotDeploymentSummary };

export function DeploymentRow({ deployment }: { deployment: Deployment }) {
  const { summary } = deployment;
  const detailLink = resourceLinks.deployment(deployment.kind, summary.name);
  return (
    <div className="flex items-center gap-4 border-t border-border py-2 text-sm first:border-t-0">
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <ResourceLink link={detailLink} className="truncate font-medium">
            {summary.name}
          </ResourceLink>
          <span className="font-mono text-xs text-muted-foreground">
            #{summary.uid}
          </span>
          <span className="text-xs text-muted-foreground">
            {deployment.kind === "pilot"
              ? "PilotDeployment"
              : "StaticDeployment"}
          </span>
        </div>
        <div className="truncate text-xs text-muted-foreground">
          <ResourceLink link={resourceLinks.cluster(summary.cluster_name)}>
            {summary.cluster_name}
          </ResourceLink>
          {" · "}
          <ResourceLink link={resourceLinks.model(summary.model_name)}>
            {summary.model_name}
          </ResourceLink>
        </div>
        {deployment.kind === "static" ? (
          <div className="truncate font-mono text-xs text-muted-foreground">
            {deployment.summary.api_url} · {deployment.summary.health}
          </div>
        ) : (
          <div className="text-xs text-muted-foreground">
            {deployment.summary.state}
            {` · `}
            {deployment.summary.desired_replicas} desired replica
            {deployment.summary.desired_replicas === 1 ? "" : "s"}
            {deployment.summary.consecutive_launch_failures > 0 && (
              <span className="ml-2 text-destructive/80">
                {deployment.summary.consecutive_launch_failures} consecutive
                launch failure
                {deployment.summary.consecutive_launch_failures === 1
                  ? ""
                  : "s"}
              </span>
            )}
          </div>
        )}
      </div>
      <Button asChild variant="ghost" size="sm">
        <Link {...detailLink}>
          View
          <ArrowRight data-icon="inline-end" />
        </Link>
      </Button>
    </div>
  );
}
