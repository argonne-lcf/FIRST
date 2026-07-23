import { Link } from "@tanstack/react-router";
import { cn } from "@/lib/utils";

const TABS = [
  { to: "/health", label: "Health" },
  { to: "/deployments", label: "Deployments" },
  { to: "/clusters", label: "Clusters" },
] as const;

export function TabNav() {
  return (
    <nav className="flex h-11 items-center gap-1 border-b border-border px-4">
      {TABS.map((tab) => (
        <Link
          key={tab.to}
          to={tab.to}
          className={cn(
            "relative -mb-px flex h-11 items-center border-b-2 border-transparent px-3 text-sm font-medium text-muted-foreground transition-colors hover:text-foreground",
            "data-[status=active]:border-primary data-[status=active]:text-foreground",
          )}
        >
          {tab.label}
        </Link>
      ))}
    </nav>
  );
}
