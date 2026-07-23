import { Navigate, useNavigate } from "@tanstack/react-router";
import { useGlobusAuth } from "@globus/react-auth-context";
import { useQuery } from "@tanstack/react-query";
import { userQueries } from "../queries/user";
import { GATEWAY_CLIENT_ID } from "../config";

// Demo: call the authenticated /whoami route through the generated SDK.
// The bearer token is attached automatically (see lib/api.ts).
function WhoamiDemo() {
  const { data, error, isPending } = useQuery(userQueries.whoami());

  return (
    <section>
      <h2>/whoami (SDK demo)</h2>
      {isPending && <p className="muted">Loading…</p>}
      {error && <p className="muted">Request failed: {String(error)}</p>}
      {data && <pre>{JSON.stringify(data, null, 2)}</pre>}
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
