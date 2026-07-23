import { Navigate } from "@tanstack/react-router";
import { useGlobusAuth } from "@globus/react-auth-context";

export function Login() {
  const { isAuthenticated, authorization } = useGlobusAuth();

  if (isAuthenticated) {
    return <Navigate to="/dashboard" replace />;
  }

  return (
    <main className="page">
      <h1>Inference Gateway</h1>
      <p>Log in with your Globus identity to request access to the console.</p>
      <button onClick={() => void authorization?.login()}>
        Log in with Globus
      </button>
    </main>
  );
}
