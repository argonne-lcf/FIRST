import { Fragment } from "react";
import { Link, type LinkProps } from "@tanstack/react-router";
import { ChevronRight } from "lucide-react";

export type Crumb = {
  label: string;
  /** Link target; omit for the current (last) crumb. */
  link?: LinkProps;
};

export function Breadcrumb({ items }: { items: Crumb[] }) {
  return (
    <nav className="flex h-11 items-center gap-1.5 border-b border-border px-4 text-sm">
      {items.map((item, i) => (
        <Fragment key={i}>
          {i > 0 && (
            <ChevronRight className="size-4 shrink-0 text-muted-foreground" />
          )}
          {item.link ? (
            <Link
              {...item.link}
              className="font-medium text-muted-foreground transition-colors hover:text-foreground"
            >
              {item.label}
            </Link>
          ) : (
            <span className="font-medium text-foreground">{item.label}</span>
          )}
        </Fragment>
      ))}
    </nav>
  );
}
