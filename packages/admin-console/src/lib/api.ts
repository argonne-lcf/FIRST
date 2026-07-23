import type { AuthorizationManager } from "@globus/sdk/core/authorization/AuthorizationManager";
import { client } from "./client/client.gen";
import { GATEWAY_CLIENT_ID } from "../config";

let manager: AuthorizationManager | undefined;

/**
 * Bridge the live AuthorizationManager to the generated fetch client.
 */
export function bindAuthorization(instance: AuthorizationManager | undefined) {
  manager = instance;
}

client.setConfig({
  baseUrl: "",
  auth: () => manager?.tokens.getByResourceServer(GATEWAY_CLIENT_ID)?.access_token,
});

export { client };
