import { Link, type LinkProps } from "@tanstack/react-router";
import { cn } from "@/lib/utils";

/** An inline, underlined-on-hover link to another resource's detail view. */
export function ResourceLink({
  link,
  className,
  children,
}: {
  link: LinkProps;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <Link
      {...link}
      className={cn(
        "text-primary underline-offset-4 hover:underline",
        className,
      )}
      onClick={(e) => e.stopPropagation()}
    >
      {children}
    </Link>
  );
}
