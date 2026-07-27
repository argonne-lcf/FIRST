import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { deploymentQueries } from "@/queries/deployment";
import {
  DeploymentRow,
  type Deployment,
  type DeploymentState,
} from "@/components/DeploymentRow";
import { STATE_SEVERITY, type Severity } from "@/lib/severity";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { cn, humanize } from "@/lib/utils";

const SEVERITY_RANK: Record<Severity, number> = {
  critical: 2,
  warning: 1,
  ok: 0,
};

/** Accordion header color-coding by severity. */
const SEVERITY_HEADER: Record<Severity, string> = {
  ok: "text-success",
  warning: "text-warning",
  critical: "text-destructive",
};

/** Display order of state groups: worst severity first, then alphabetical. */
const STATE_ORDER: readonly DeploymentState[] = (
  Object.keys(STATE_SEVERITY) as DeploymentState[]
).sort((a, b) => {
  const rank =
    SEVERITY_RANK[STATE_SEVERITY[b]] - SEVERITY_RANK[STATE_SEVERITY[a]];
  return rank !== 0 ? rank : a.localeCompare(b);
});

function StateGroup({
  state,
  deployments,
}: {
  state: DeploymentState;
  deployments: Deployment[];
}) {
  const severity = STATE_SEVERITY[state];
  return (
    <AccordionItem value={state}>
      <AccordionTrigger className={cn("px-1", SEVERITY_HEADER[severity])}>
        {deployments.length} {humanize(state)} Deployment
        {deployments.length === 1 ? "" : "s"}
      </AccordionTrigger>
      <AccordionContent className="px-1">
        {deployments.map((d) => (
          <DeploymentRow key={`${d.kind}-${d.summary.uid}`} deployment={d} />
        ))}
      </AccordionContent>
    </AccordionItem>
  );
}

export function Deployments() {
  const pilotsQuery = useQuery(deploymentQueries.pilots());
  const staticsQuery = useQuery(deploymentQueries.statics());
  const [filter, setFilter] = useState("");

  const isPending = pilotsQuery.isPending || staticsQuery.isPending;
  const isError = pilotsQuery.isError || staticsQuery.isError;
  const error = pilotsQuery.error ?? staticsQuery.error;

  const groups = useMemo(() => {
    const all: Deployment[] = [
      ...(staticsQuery.data ?? []).map((summary): Deployment => ({
        kind: "static",
        state: summary.health,
        summary,
      })),
      ...(pilotsQuery.data ?? []).map((summary): Deployment => ({
        kind: "pilot",
        state: summary.state,
        summary,
      })),
    ];

    const needle = filter.trim().toLowerCase();
    const filtered = needle
      ? all.filter((d) => d.summary.name.toLowerCase().includes(needle))
      : all;

    return STATE_ORDER.map((state) => ({
      state,
      deployments: filtered.filter((d) => d.state === state),
    })).filter((g) => g.deployments.length > 0);
  }, [staticsQuery.data, pilotsQuery.data, filter]);

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between gap-4">
        <CardTitle>Deployments</CardTitle>
        <Input
          className="max-w-xs"
          placeholder="Filter by name…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
      </CardHeader>
      <CardContent>
        {isPending ? (
          <div className="space-y-2">
            {Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="h-10" />
            ))}
          </div>
        ) : isError ? (
          <p className="text-sm text-destructive">
            Failed to load deployments:{" "}
            {error instanceof Error ? error.message : "unknown error"}
          </p>
        ) : groups.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            {filter.trim()
              ? "No deployments match your filter."
              : "No deployments configured."}
          </p>
        ) : (
          <Accordion type="multiple" defaultValue={groups.map((g) => g.state)}>
            {groups.map((g) => (
              <StateGroup
                key={g.state}
                state={g.state}
                deployments={g.deployments}
              />
            ))}
          </Accordion>
        )}
      </CardContent>
    </Card>
  );
}
