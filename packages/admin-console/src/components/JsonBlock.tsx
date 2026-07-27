import { Fragment } from "react";
import { cn } from "@/lib/utils";

/** Longest one-line rendering of a subtree before it gets broken across lines. */
const WRAP_WIDTH = 60;

/** Long string leaves with embedded newlines expand once they exceed this. */
const STRING_WRAP_WIDTH = 64;

/**
 * Pretty-print like `JSON.stringify(v, null, 2)`, except any object or array
 * whose compact form fits on one line (<= WRAP_WIDTH chars) is kept inline
 * rather than exploded. Keeps small tuples like [1, 2] together while still
 * expanding large nested structures. Long multi-line string leaves (e.g. shell
 * scripts) are rendered across real lines instead of one `\n`-laden line.
 */
function prettyJson(value: unknown, indent = ""): string {
  if (
    typeof value === "string" &&
    value.length > STRING_WRAP_WIDTH &&
    value.includes("\n")
  ) {
    // Escape each line independently (no `\n` escapes survive), then rejoin on
    // real newlines so the <pre> shows the breaks. The quoted result still
    // matches the highlighter's string token as a single span.
    const pad = indent + "  ";
    const body = value
      .split("\n")
      .map((line) => JSON.stringify(line).slice(1, -1))
      .join("\n" + pad);
    return `"${body}"`;
  }

  const compact = JSON.stringify(value);
  if (compact === undefined) return "null";
  if (
    compact.length <= WRAP_WIDTH ||
    typeof value !== "object" ||
    value === null
  )
    return compact;

  const pad = indent + "  ";
  if (Array.isArray(value)) {
    const items = value.map((v) => pad + prettyJson(v, pad));
    return `[\n${items.join(",\n")}\n${indent}]`;
  }
  const items = Object.entries(value).map(
    ([k, v]) => `${pad}${JSON.stringify(k)}: ${prettyJson(v, pad)}`,
  );
  return `{\n${items.join(",\n")}\n${indent}}`;
}

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

  const source = prettyJson(value);
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
