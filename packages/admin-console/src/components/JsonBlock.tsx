import { Fragment } from "react";
import { cn } from "@/lib/utils";

/** Render a JSON value as syntax-highlighted, pretty-printed text. */
export function JsonBlock({
  value,
  className,
}: {
  value: unknown;
  className?: string;
}) {
  if (value === null || value === undefined) {
    return <span className="text-muted-foreground">—</span>;
  }

  const source = JSON.stringify(value, null, 2);
  const parts: React.ReactNode[] = [];
  let last = 0;

  // Matches, in order: object keys ("...":), string values ("..."),
  // booleans/null, and numbers.
  const token =
    /("(?:\\.|[^"\\])*")(\s*:)|("(?:\\.|[^"\\])*")|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;

  for (const match of source.matchAll(token)) {
    const [full, key, colon, str, keyword, num] = match;
    if (match.index > last) parts.push(source.slice(last, match.index));

    if (key !== undefined) {
      parts.push(
        <Fragment key={match.index}>
          <span className="text-primary">{key}</span>
          {colon}
        </Fragment>,
      );
    } else if (str !== undefined) {
      parts.push(
        <span key={match.index} className="text-success">
          {str}
        </span>,
      );
    } else if (keyword !== undefined) {
      parts.push(
        <span key={match.index} className="text-muted-foreground">
          {keyword}
        </span>,
      );
    } else if (num !== undefined) {
      parts.push(
        <span key={match.index} className="text-warning">
          {num}
        </span>,
      );
    }
    last = match.index + full.length;
  }
  if (last < source.length) parts.push(source.slice(last));

  return (
    <pre
      className={cn(
        "overflow-x-auto rounded-md bg-muted/50 p-3 font-mono text-xs leading-relaxed",
        className,
      )}
    >
      {parts}
    </pre>
  );
}
