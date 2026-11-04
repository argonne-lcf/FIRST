import { Outlet } from "@tanstack/react-router";

/** Padded content area for the top-level list views (detail pages own theirs). */
export function TabsLayout() {
  return (
    <main className="flex-1 p-6">
      <Outlet />
    </main>
  );
}
