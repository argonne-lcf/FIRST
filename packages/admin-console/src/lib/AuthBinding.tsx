import { useEffect } from "react";
import { useGlobusAuth } from "@globus/react-auth-context";
import { bindAuthorization } from "./api";

/**
 * Bridges the AuthorizationManager (created inside <Provider>) to the
 * codegen client. Renders nothing.
 */
export function AuthBinding() {
  const { authorization } = useGlobusAuth();

  useEffect(() => {
    bindAuthorization(authorization ?? undefined);
    return () => bindAuthorization(undefined);
  }, [authorization]);

  return null;
}
