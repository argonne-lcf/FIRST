import {
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
} from "@tanstack/react-router";
import { Login } from "./pages/Login";
import { Callback } from "./pages/Callback";
import { Dashboard } from "./pages/Dashboard";

const rootRoute = createRootRoute({
  component: () => <Outlet />,
});

const loginRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: Login,
});

const callbackRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/callback",
  component: Callback,
});

const dashboardRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/dashboard",
  component: Dashboard,
});

const routeTree = rootRoute.addChildren([loginRoute, callbackRoute, dashboardRoute]);

export const router = createRouter({ routeTree, basepath: "/admin-console" });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
