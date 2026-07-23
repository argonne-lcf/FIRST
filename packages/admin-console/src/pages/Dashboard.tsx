import { useEffect, useState } from "react";
import { Navigate, useNavigate } from "@tanstack/react-router";
import { useGlobusAuth } from "@globus/react-auth-context";
import { whoamiWhoamiGet, type UserAuthEvent } from "../lib/client";
import { GATEWAY_CLIENT_ID } from "../config";

// Demo: call the authenticated /whoami route through the generated SDK.
// The bearer token is attached automatically (see lib/api.ts).
function WhoamiDemo() {
  const [state, setState] = useState<
    | { status: "loading" }
    | { status: "error"; message: string }
    | { status: "ok"; user: UserAuthEvent }
  >({ status: "loading" });

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const { data, error } = await whoamiWhoamiGet();
      if (cancelled) return;
      if (error || !data) {
        setState({ status: "error", message: JSON.stringify(error) });
      } else {
        setState({ status: "ok", user: data });
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <section>
      <h2>/whoami (SDK demo)</h2>
      {state.status === "loading" && <p className="muted">Loading…</p>}
      {state.status === "error" && (
        <p className="muted">Request failed: {state.message}</p>
      )}
      {state.status === "ok" && (
        <pre>{JSON.stringify(state.user, null, 2)}</pre>
      )}
    </section>
  );
}

export function Dashboard() {
  const { isAuthenticated, authorization } = useGlobusAuth();
  const navigate = useNavigate();

  if (!isAuthenticated || !authorization) {
    return <Navigate to="/" replace />;
  }

  const user = authorization.user;
  const gatewayToken =
    authorization.tokens.getByResourceServer(GATEWAY_CLIENT_ID);

  const logout = async () => {
    await authorization.revoke();
    await navigate({ to: "/", replace: true });
  };

  return (
    <main className="page">
      <h1>Signed in</h1>
      <dl>
        <dt>User</dt>
        <dd>{user?.name ?? user?.preferred_username ?? "unknown"}</dd>

        <dt>Identity ID</dt>
        <dd>{user?.sub ?? "unknown"}</dd>

        <dt>Gateway token</dt>
        <dd>
          {gatewayToken
            ? `issued for resource server ${gatewayToken.resource_server}`
            : "missing — check the consent screen included the gateway scope"}
        </dd>
      </dl>
      <WhoamiDemo />
      <button onClick={() => void logout()}>Log out</button>
    </main>
  );
}
