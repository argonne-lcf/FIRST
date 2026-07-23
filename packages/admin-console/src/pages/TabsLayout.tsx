import { Outlet } from "@tanstack/react-router";
import { TabNav } from "@/components/TabNav";

export function TabsLayout() {
  return (
    <>
      <TabNav />
      <main className="flex-1 p-6">
        <Outlet />
      </main>
    </>
  );
}
