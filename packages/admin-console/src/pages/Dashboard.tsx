import { Navigate, useNavigate } from "@tanstack/react-router";
import { useGlobusAuth } from "@globus/react-auth-context";
import { GATEWAY_CLIENT_ID } from "../config";

export function Dashboard() {
  const { isAuthenticated, authorization } = useGlobusAuth();
  const navigate = useNavigate();

  if (!isAuthenticated || !authorization) {
    return <Navigate to="/" replace />;
  }

  const user = authorization.user;
  const gatewayToken = authorization.tokens.getByResourceServer(GATEWAY_CLIENT_ID);

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
      <button onClick={() => void logout()}>Log out</button>
    </main>
  );
}
