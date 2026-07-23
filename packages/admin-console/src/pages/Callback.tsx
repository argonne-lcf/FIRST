import { useEffect, useRef } from "react";
import { Link, useNavigate } from "@tanstack/react-router";
import { useGlobusAuth } from "@globus/react-auth-context";

/**
 * Globus Auth redirects here with ?code=...&state=... — exchange the
 * code for tokens (PKCE), then move on to the app.
 */
export function Callback() {
  const { isAuthenticated, authorization } = useGlobusAuth();
  const navigate = useNavigate();
  const attempted = useRef(false);

  useEffect(() => {
    if (!authorization) return;

    if (isAuthenticated) {
      void navigate({ to: "/health", replace: true });
      return;
    }

    // Guard: the code is single-use, so only exchange it once even if
    // the effect re-runs (React StrictMode double-invokes effects in dev).
    if (attempted.current) return;
    attempted.current = true;

    // On success this emits the `authenticated` event, which flips
    // `isAuthenticated` and re-runs this effect to navigate away.
    void authorization.handleCodeRedirect({ shouldReplace: false });
  }, [authorization, isAuthenticated, navigate]);

  return (
    <main className="mx-auto mt-[18vh] max-w-md space-y-2 px-6">
      <p>Completing sign-in…</p>
      <p className="text-sm text-muted-foreground">
        Stuck here? <Link to="/">Return to login</Link>
      </p>
    </main>
  );
}
