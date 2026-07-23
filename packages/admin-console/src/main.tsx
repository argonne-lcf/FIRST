import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "@tanstack/react-router";
import { Provider } from "@globus/react-auth-context";
import { router } from "./router";
import { AuthBinding } from "./lib/AuthBinding";
import { AUTH_CLIENT_ID, GATEWAY_SCOPE, REDIRECT_URI } from "./config";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <Provider
      client={AUTH_CLIENT_ID}
      scopes={GATEWAY_SCOPE}
      redirect={REDIRECT_URI}
      // @globus/sdk v5+ keeps tokens in memory by default; opt in to
      // localStorage so the session survives reloads. Refresh tokens are
      // already requested by default (useRefreshTokens: true)
      storage={localStorage}
    >
      <AuthBinding />
      <RouterProvider router={router} />
    </Provider>
  </StrictMode>
);
