import type { AuthorizationManager } from "@globus/sdk/core/authorization/AuthorizationManager";
import { client } from "./client/client.gen";
import { GATEWAY_CLIENT_ID } from "@/config";

let manager: AuthorizationManager | undefined;

/**
 * Bridge the live AuthorizationManager to the generated fetch client.
 */
export function bindAuthorization(instance: AuthorizationManager | undefined) {
  manager = instance;
}

client.setConfig({
  baseUrl: "",
  auth: () =>
    manager?.tokens.getByResourceServer(GATEWAY_CLIENT_ID)?.access_token,
});

// A 401 from the gateway means our bearer token is no longer accepted (e.g.
// token introspection reports it inactive). The token is dead server-side, so
// every subsequent request fails the same way and pages render a blank error.
// Clear the session: reset() drops the stored tokens and flips the auth
// context to unauthenticated, which bounces the user to the login screen (see
// AppLayout's guard) instead. It's synchronous and local-only — there's no
// live token left to revoke server-side.
//
// Latched so a burst of concurrent 401s only logs out once. It doesn't need
// resetting: logging back in goes through a full-page Globus redirect, which
// reloads this module fresh.
let loggingOut = false;

client.interceptors.error.use((error, response) => {
  if (response?.status === 401 && manager && !loggingOut) {
    loggingOut = true;
    manager.reset();
  }
  return error;
});

export { client };
