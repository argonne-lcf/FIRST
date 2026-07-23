import axios from "axios";
import type { AuthorizationManager } from "@globus/sdk/core/authorization/AuthorizationManager";
import { GATEWAY_CLIENT_ID } from "../config";

let manager: AuthorizationManager | undefined;
export function bindAuthorization(instance: AuthorizationManager | undefined) {
  manager = instance;
}

export const api = axios.create({ baseURL: "" });

api.interceptors.request.use((config) => {
  const token = manager?.tokens.getByResourceServer(GATEWAY_CLIENT_ID);
  if (token) {
    config.headers.Authorization = `Bearer ${token.access_token}`;
  }
  return config;
});
