import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "@tanstack/react-router";
import { Provider } from "@globus/react-auth-context";
import { router } from "./router";
import { AuthBinding } from "./lib/AuthBinding";
import { AUTH_CLIENT_ID, GATEWAY_SCOPE, REDIRECT_URI } from "./config";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ReactQueryDevtools } from "@tanstack/react-query-devtools";
import "./styles.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      retry: 1,
    },
  },
});

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
      <QueryClientProvider client={queryClient}>
        <AuthBinding />
        <RouterProvider router={router} />
        <ReactQueryDevtools initialIsOpen={false} />
      </QueryClientProvider>
    </Provider>
  </StrictMode>,
);
