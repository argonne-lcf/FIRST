import { Navigate } from "@tanstack/react-router";
import { useGlobusAuth } from "@globus/react-auth-context";
import { Button } from "@/components/ui/button";

export function Login() {
  const { isAuthenticated, authorization } = useGlobusAuth();

  if (isAuthenticated) {
    return <Navigate to="/health" replace />;
  }

  return (
    <main className="mx-auto mt-[18vh] max-w-md space-y-4 px-6">
      <h1 className="text-2xl font-semibold">Inference Gateway</h1>
      <p className="text-muted-foreground">
        Log in with your Globus identity to request access to the console.
      </p>
      <Button onClick={() => void authorization?.login()}>
        Log in with Globus
      </Button>
    </main>
  );
}
