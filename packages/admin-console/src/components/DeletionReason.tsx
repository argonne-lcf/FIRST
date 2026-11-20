import { Trash2 } from "lucide-react";
import { cn } from "@/lib/utils";

/** Soft-delete bookkeeping shared by PilotJob and PilotReplica. */
type SoftDeletable = {
  deletion_reason?: string | null;
  deleted_at?: string | null;
};

/** "Draining" while teardown is in progress, "Deleted" once it finished. */
function deletionVerb(resource: SoftDeletable, draining: string): string {
  return resource.deleted_at ? "Deleted" : draining;
}

/**
 * Banner explaining why a resource was scheduled for deletion. Warning-toned
 * while teardown is in progress, muted once the resource is deleted.
 */
export function DeletionBanner({
  resource,
  drainingLabel = "Draining",
}: {
  resource: SoftDeletable;
  drainingLabel?: string;
}) {
  if (!resource.deletion_reason) return null;
  return (
    <div
      className={cn(
        "flex items-start gap-2 rounded-md p-3 text-sm",
        resource.deleted_at
          ? "bg-muted text-muted-foreground"
          : "bg-warning/15 text-warning",
      )}
    >
      <Trash2 className="mt-0.5 size-4 shrink-0" />
      <p className="min-w-0 break-words">
        <span className="font-medium">
          {deletionVerb(resource, drainingLabel)}:
        </span>{" "}
        {resource.deletion_reason}
      </p>
    </div>
  );
}

/** Compact one-line deletion reason for cards and table cells. */
export function DeletionReasonLine({
  resource,
  drainingLabel = "Draining",
  className,
}: {
  resource: SoftDeletable;
  drainingLabel?: string;
  className?: string;
}) {
  if (!resource.deletion_reason) return null;
  return (
    <p
      title={resource.deletion_reason}
      className={cn(
        "text-xs break-words",
        resource.deleted_at ? "text-muted-foreground" : "text-warning",
        className,
      )}
    >
      <span className="font-medium">
        {deletionVerb(resource, drainingLabel)}:
      </span>{" "}
      {resource.deletion_reason}
    </p>
  );
}
