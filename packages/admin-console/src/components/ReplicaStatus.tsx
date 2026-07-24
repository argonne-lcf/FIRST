import { type BackendRuntime, type ReplicaState } from "@/lib/client";
import { Badge } from "@/components/ui/badge";
import { humanize } from "@/lib/utils";

const REPLICA_STATE_VARIANT: Record<
  ReplicaState,
  "success" | "warning" | "destructive" | "secondary"
> = {
  ready: "success",
  pending: "warning",
  placed: "warning",
  launching: "warning",
  unhealthy: "destructive",
  error: "destructive",
  start_timeout: "destructive",
  terminating: "secondary",
  terminated: "secondary",
};

/** Badge for a replica's lifecycle state, colored by severity. */
export function ReplicaStateBadge({ state }: { state: ReplicaState }) {
  return (
    <Badge variant={REPLICA_STATE_VARIANT[state]}>{humanize(state)}</Badge>
  );
}

/** Live runtime counters, e.g. "38 requests in-flight | 0 cooldown errors". */
export function ReplicaRuntime({ runtime }: { runtime?: BackendRuntime }) {
  const inflight = runtime?.inflight ?? 0;
  const cooldownErrors = runtime?.cooldown_errors ?? 0;
  return (
    <span className="tabular-nums text-muted-foreground">
      {inflight} request{inflight === 1 ? "" : "s"} in-flight
      {" | "}
      <span className={cooldownErrors > 0 ? "text-destructive" : undefined}>
        {cooldownErrors} cooldown error{cooldownErrors === 1 ? "" : "s"}
      </span>
    </span>
  );
}
