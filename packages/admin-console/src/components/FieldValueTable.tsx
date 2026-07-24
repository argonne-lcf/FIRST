import { JsonBlock } from "@/components/JsonBlock";
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
 */
export function FieldValueTable({
  obj,
  omit = [],
}: {
  obj: Record<string, unknown>;
  omit?: readonly string[];
}) {
  const rows = Object.entries(obj).filter(([field]) => !omit.includes(field));
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
              <JsonBlock value={value} />
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
