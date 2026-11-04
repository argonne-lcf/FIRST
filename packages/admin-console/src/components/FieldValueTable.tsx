import { type LinkProps } from "@tanstack/react-router";
import { JsonBlock } from "@/components/JsonBlock";
import { ResourceLink } from "@/components/ResourceLink";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

/**
 * Two-column Field/Value table with syntax-highlighted JSON values, matching
 * the Configuration table in ClusterDetail. Renders one row per own key of
 * `obj` (in declaration order), skipping any key listed in `omit`.
 *
 * Fields named in `order` are floated to the top in that order; any remaining
 * fields keep their original declaration order below them.
 *
 * Fields with an entry in `links` (e.g. `cluster_name`) render their value as
 * a link to the referenced resource instead of as JSON.
 */
export function FieldValueTable({
  obj,
  omit = [],
  order = [],
  links = {},
}: {
  obj: Record<string, unknown>;
  omit?: readonly string[];
  order?: readonly string[];
  links?: Partial<Record<string, LinkProps>>;
}) {
  const rank = (field: string) => {
    const i = order.indexOf(field);
    return i === -1 ? order.length : i;
  };
  const rows = Object.entries(obj)
    .filter(([field]) => !omit.includes(field))
    .sort(([a], [b]) => rank(a) - rank(b));
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead className="w-56">Field</TableHead>
          <TableHead>Value</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map(([field, value]) => (
          <TableRow key={field}>
            <TableCell className="align-top font-mono text-xs text-muted-foreground">
              {field}
            </TableCell>
            <TableCell className="w-full">
              <FieldValue value={value} link={links[field]} />
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

function FieldValue({ value, link }: { value: unknown; link?: LinkProps }) {
  if (link && typeof value === "string") {
    return (
      <ResourceLink link={link} className="font-mono text-xs">
        {value}
      </ResourceLink>
    );
  }
  return <JsonBlock value={value} />;
}
