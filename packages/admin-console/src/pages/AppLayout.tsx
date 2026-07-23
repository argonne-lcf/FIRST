import { Navigate, Outlet } from "@tanstack/react-router";
import { useGlobusAuth } from "@globus/react-auth-context";
import { AppBar } from "@/components/AppBar";
import { TabNav } from "@/components/TabNav";

export function AppLayout() {
  const { isAuthenticated, authorization } = useGlobusAuth();

  if (!isAuthenticated || !authorization) {
    return <Navigate to="/" replace />;
  }

  return (
    <div className="flex min-h-screen flex-col bg-gradient-to-b from-muted/40 to-background">
      <AppBar />
      <TabNav />
      <main className="flex-1 p-6">
        <Outlet />
      </main>
    </div>
  );
}
