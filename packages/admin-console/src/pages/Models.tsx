import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";
import { type ModelConfig, type ModelSummary } from "@/lib/client";
import { modelQueries } from "@/queries/models";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { humanize } from "@/lib/utils";

/** Deployment states worst-first, matching the Deployments page grouping. */
const STATE_ORDER = [
  "unhealthy",
  "failed",
  "degraded",
  "starting",
  "stopping",
  "awaiting_capacity",
  "offline",
  "unknown",
  "healthy",
] as const;

/**
 * Combine a model's static and pilot deployment states into a compact tally
 * like "5 healthy | 3 offline", ordered worst-first.
 */
function deploymentStateTally(model: ModelSummary): string {
  const states = [
    ...model.static_deployments.map((d) => d.health as string),
    ...model.pilot_deployments.map((d) => d.state as string),
  ];
  if (states.length === 0) return "—";
  const seen = new Set(states);
  const ordered = [
    ...STATE_ORDER.filter((s) => seen.has(s)),
    ...[...seen].filter((s) => !STATE_ORDER.includes(s as never)).sort(),
  ];
  return ordered
    .map(
      (state) =>
        `${states.filter((s) => s === state).length} ${humanize(state)}`,
    )
    .join(" | ");
}

function RuntimeCell({ model }: { model: ModelSummary }) {
  const runtime = model.runtime;
  if (!runtime) return <span className="text-muted-foreground">—</span>;
  return (
    <span className="tabular-nums">
      {runtime.total_inflight ?? 0} in-flight |{" "}
      {runtime.capacity_rejects_total ?? 0} capacity rejects
    </span>
  );
}

function AccessGroupDialog({
  model,
  config,
  onClose,
}: {
  model: ModelSummary | null;
  config: ModelConfig | undefined;
  onClose: () => void;
}) {
  return (
    <Dialog open={model !== null} onOpenChange={(open) => !open && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{model?.access_group_name}</DialogTitle>
          <DialogDescription>Access group for {model?.name}</DialogDescription>
        </DialogHeader>
        <div className="space-y-4 text-sm">
          <AccessList label="Allowed Groups" items={config?.allowed_groups} />
          <AccessList label="Allowed Domains" items={config?.allowed_domains} />
        </div>
      </DialogContent>
    </Dialog>
  );
}

function AccessList({ label, items }: { label: string; items?: string[] }) {
  return (
    <div className="space-y-1">
      <p className="font-medium">{label}</p>
      {items && items.length > 0 ? (
        <ul className="flex flex-wrap gap-1">
          {items.map((item) => (
            <li key={item}>
              <Badge variant="outline">{item}</Badge>
            </li>
          ))}
        </ul>
      ) : (
        <p className="text-muted-foreground">None</p>
      )}
    </div>
  );
}

export function Models() {
  const modelsQuery = useQuery(modelQueries.all());
  const routerConfigQuery = useQuery(modelQueries.routerConfig());
  const [selected, setSelected] = useState<ModelSummary | null>(null);

  const selectedConfig = selected
    ? routerConfigQuery.data?.models?.find((m) => m.name === selected.name)
    : undefined;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Models</CardTitle>
      </CardHeader>
      <CardContent>
        {modelsQuery.isPending ? (
          <div className="space-y-2">
            {Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="h-10" />
            ))}
          </div>
        ) : modelsQuery.isError ? (
          <p className="text-sm text-destructive">
            Failed to load models:{" "}
            {modelsQuery.error instanceof Error
              ? modelsQuery.error.message
              : "unknown error"}
          </p>
        ) : modelsQuery.data.length === 0 ? (
          <p className="text-sm text-muted-foreground">No models configured.</p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>UID</TableHead>
                <TableHead>Name</TableHead>
                <TableHead>Access Group</TableHead>
                <TableHead>Supported Endpoints</TableHead>
                <TableHead>Deployments</TableHead>
                <TableHead>Runtime</TableHead>
                <TableHead />
              </TableRow>
            </TableHeader>
            <TableBody>
              {modelsQuery.data.map((model) => (
                <TableRow key={model.uid}>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    #{model.uid}
                  </TableCell>
                  <TableCell className="font-medium">{model.name}</TableCell>
                  <TableCell>
                    <Button
                      variant="link"
                      size="sm"
                      className="h-auto p-0"
                      onClick={() => setSelected(model)}
                    >
                      {model.access_group_name}
                    </Button>
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-1">
                      {model.supported_endpoints.map((ep) => (
                        <Badge key={ep} variant="outline">
                          {ep}
                        </Badge>
                      ))}
                    </div>
                  </TableCell>
                  <TableCell className="tabular-nums text-muted-foreground">
                    {deploymentStateTally(model)}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    <RuntimeCell model={model} />
                  </TableCell>
                  <TableCell className="text-right">
                    <Button asChild variant="ghost" size="sm">
                      <Link
                        to="/models/$modelSlug"
                        params={{ modelSlug: model.slug }}
                      >
                        View
                        <ArrowRight data-icon="inline-end" />
                      </Link>
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
      <AccessGroupDialog
        model={selected}
        config={selectedConfig}
        onClose={() => setSelected(null)}
      />
    </Card>
  );
}
