import { Link } from "@tanstack/react-router";
import { UserMenu } from "@/components/UserMenu";

export function AppBar() {
  return (
    <header className="flex h-12 w-full items-center justify-between border-b border-border bg-background px-4">
      <Link
        to="/health"
        className="font-medium transition-colors hover:text-foreground/80"
      >
        FIRST Admin Console
      </Link>
      <UserMenu />
    </header>
  );
}
