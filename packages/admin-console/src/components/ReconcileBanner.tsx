import { TriangleAlert } from "lucide-react";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { formatTimestamp, formatUntil } from "@/lib/utils";
import { useNow } from "@/lib/useNow";

/** Reconcile bookkeeping shared by every reconciled resource. */
type Reconciled = {
  reconcile_failures?: number;
  reconcile_last_error?: string | null;
  reconcile_last_traceback?: string | null;
  reconcile_retry_at?: string | null;
};

/** Live "in 4m 12s" countdown to a resource's next reconcile attempt. */
export function RetryCountdown({ retryAt }: { retryAt?: string | null }) {
  const now = useNow();
  if (!retryAt) return null;
  return (
    <span className="tabular-nums" title={formatTimestamp(retryAt)}>
      {formatUntil(retryAt, now)}
    </span>
  );
}

/** Scrollable, monospace traceback of the last reconcile failure. */
export function TracebackBlock({ traceback }: { traceback: string }) {
  return (
    <pre className="max-h-80 overflow-auto rounded-md bg-muted p-3 font-mono text-xs whitespace-pre text-foreground">
      {traceback}
    </pre>
  );
}

/** Destructive banner surfacing a resource's reconcile failures, if any. */
export function ReconcileBanner({ resource }: { resource: Reconciled }) {
  if (!resource.reconcile_failures) return null;
  return (
    <div className="flex items-start gap-2 rounded-md bg-destructive/10 p-3 text-sm text-destructive">
      <TriangleAlert className="mt-0.5 size-4 shrink-0" />
      <div className="min-w-0 flex-1 space-y-1">
        <p className="font-medium">
          {resource.reconcile_failures} reconcile failure
          {resource.reconcile_failures === 1 ? "" : "s"}
          {resource.reconcile_retry_at && (
            <span className="font-normal">
              {" "}
              · next retry{" "}
              <RetryCountdown retryAt={resource.reconcile_retry_at} />
            </span>
          )}
        </p>
        {resource.reconcile_last_error && (
          <p className="font-mono text-xs break-words">
            {resource.reconcile_last_error}
          </p>
        )}
        {resource.reconcile_last_traceback && (
          <Accordion type="single" collapsible>
            <AccordionItem value="traceback">
              <AccordionTrigger className="py-1 text-xs">
                Traceback
              </AccordionTrigger>
              <AccordionContent>
                <TracebackBlock traceback={resource.reconcile_last_traceback} />
              </AccordionContent>
            </AccordionItem>
          </Accordion>
        )}
      </div>
    </div>
  );
}
