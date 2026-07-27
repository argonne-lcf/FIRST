/**
 * Page header for a single control-plane resource: the resource `kind` as an
 * eyebrow label, the `name` as the page title, the `uid` as muted subtext, and
 * an optional slot (`children`) for status badges beside the title.
 */
export function ResourceHeader({
  kind,
  name,
  uid,
  children,
}: {
  kind?: string;
  name: string;
  uid?: number;
  children?: React.ReactNode;
}) {
  return (
    <div className="space-y-1">
      {kind && (
        <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          {kind.replace(/([a-z])([A-Z])/g, "$1 $2")}
        </p>
      )}
      <div className="flex items-center gap-3">
        <h1 className="text-2xl font-semibold">{name}</h1>
        {children}
      </div>
      {uid != null && (
        <p className="font-mono text-xs text-muted-foreground">uid {uid}</p>
      )}
    </div>
  );
}
