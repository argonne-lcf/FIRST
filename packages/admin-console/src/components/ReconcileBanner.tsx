import { TriangleAlert } from "lucide-react";

/** Reconcile bookkeeping shared by every reconciled resource. */
type Reconciled = {
  reconcile_failures?: number;
  reconcile_last_error?: string | null;
};

/** Destructive banner surfacing a resource's reconcile failures, if any. */
export function ReconcileBanner({ resource }: { resource: Reconciled }) {
  if (!resource.reconcile_failures) return null;
  return (
    <div className="flex items-start gap-2 rounded-md bg-destructive/10 p-3 text-sm text-destructive">
      <TriangleAlert className="mt-0.5 size-4 shrink-0" />
      <div className="space-y-1">
        <p className="font-medium">
          {resource.reconcile_failures} reconcile failure
          {resource.reconcile_failures === 1 ? "" : "s"}
        </p>
        {resource.reconcile_last_error && (
          <p className="font-mono text-xs">{resource.reconcile_last_error}</p>
        )}
      </div>
    </div>
  );
}
