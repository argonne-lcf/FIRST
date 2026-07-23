import { UserMenu } from "@/components/UserMenu";

export function AppBar() {
  return (
    <header className="flex h-12 w-full items-center justify-between border-b border-border bg-background px-4">
      <span className="font-medium">FIRST Admin Console</span>
      <UserMenu />
    </header>
  );
}
