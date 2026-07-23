import {
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
} from "@tanstack/react-router";
import { Login } from "./pages/Login";
import { Callback } from "./pages/Callback";
import { AppLayout } from "./pages/AppLayout";
import { Health } from "./pages/Health";
import { Deployments } from "./pages/Deployments";
import { Clusters } from "./pages/Clusters";

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

// Layout route: the tabbed app shell. Its children render inside <Outlet />.
const shellRoute = createRoute({
  getParentRoute: () => rootRoute,
  id: "shell",
  component: AppLayout,
});

const healthRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/health",
  component: Health,
});

const deploymentsRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/deployments",
  component: Deployments,
});

const clustersRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/clusters",
  component: Clusters,
});

const routeTree = rootRoute.addChildren([
  loginRoute,
  callbackRoute,
  shellRoute.addChildren([healthRoute, deploymentsRoute, clustersRoute]),
]);

export const router = createRouter({ routeTree, basepath: "/admin-console" });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
