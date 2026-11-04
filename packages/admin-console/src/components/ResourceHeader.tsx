import { Fragment } from "react";
import { type LinkProps } from "@tanstack/react-router";
import { ResourceLink } from "@/components/ResourceLink";

/** A labelled cross-reference to another resource, e.g. Cluster → polaris. */
export type RelatedResource = {
  label: string;
  name: string | null | undefined;
  link?: LinkProps;
};

/**
 * Page header for a single control-plane resource: the resource `kind` as an
 * eyebrow label, the `name` as the page title, the `uid` as muted subtext, and
 * an optional slot (`children`) for status badges beside the title.
 *
 * `related` renders a row of links to parent/sibling resources (cluster,
 * model, pilot job, ...) beneath the title for quick navigation.
 */
export function ResourceHeader({
  kind,
  name,
  uid,
  related,
  children,
}: {
  kind?: string;
  name: string;
  uid?: number;
  related?: RelatedResource[];
  children?: React.ReactNode;
}) {
  const shown = related?.filter((r) => r.name);
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
      {shown && shown.length > 0 && (
        <p className="flex flex-wrap items-center gap-x-2 gap-y-1 pt-1 text-sm text-muted-foreground">
          {shown.map((r, i) => (
            <Fragment key={r.label}>
              {i > 0 && <span aria-hidden>·</span>}
              <span>
                {r.label}{" "}
                {r.link ? (
                  <ResourceLink link={r.link} className="font-medium">
                    {r.name}
                  </ResourceLink>
                ) : (
                  <span className="font-medium text-foreground">{r.name}</span>
                )}
              </span>
            </Fragment>
          ))}
        </p>
      )}
    </div>
  );
}
