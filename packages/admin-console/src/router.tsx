import {
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
} from "@tanstack/react-router";
import { Login } from "./pages/Login";
import { Callback } from "./pages/Callback";
import { AppLayout } from "./pages/AppLayout";
import { TabsLayout } from "./pages/TabsLayout";
import { Health } from "./pages/Health";
import { Models } from "./pages/Models";
import { ModelDetail } from "./pages/ModelDetail";
import { Deployments } from "./pages/Deployments";
import { DeploymentDetail } from "./pages/DeploymentDetail";
import { ReplicaDetail } from "./pages/ReplicaDetail";
import { Clusters } from "./pages/Clusters";
import { ClusterDetail } from "./pages/ClusterDetail";
import { PilotJobDetail } from "./pages/PilotJobDetail";

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

// Layout route: the authenticated app shell (AppBar + auth guard).
// Its children render inside <Outlet />.
const shellRoute = createRoute({
  getParentRoute: () => rootRoute,
  id: "shell",
  component: AppLayout,
});

// Sub-layout: the primary tabbed views share a tab bar.
const tabsLayoutRoute = createRoute({
  getParentRoute: () => shellRoute,
  id: "tabs",
  component: TabsLayout,
});

const healthRoute = createRoute({
  getParentRoute: () => tabsLayoutRoute,
  path: "/health",
  component: Health,
});

const modelsRoute = createRoute({
  getParentRoute: () => tabsLayoutRoute,
  path: "/models",
  component: Models,
});

const deploymentsRoute = createRoute({
  getParentRoute: () => tabsLayoutRoute,
  path: "/deployments",
  component: Deployments,
});

const clustersRoute = createRoute({
  getParentRoute: () => tabsLayoutRoute,
  path: "/clusters",
  component: Clusters,
});

// Detail views render their own breadcrumb bar instead of the tab bar.
const modelDetailRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/models/$modelSlug",
  component: ModelDetail,
});

const deploymentDetailRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/deployments/$kind/$deploymentSlug",
  component: DeploymentDetail,
});

const replicaDetailRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/deployments/pilot/$deploymentSlug/replicas/$replicaSlug",
  component: ReplicaDetail,
});

const clusterDetailRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/clusters/$clusterSlug",
  component: ClusterDetail,
});

const pilotJobDetailRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/clusters/$clusterSlug/jobs/$jobSlug",
  component: PilotJobDetail,
});

const routeTree = rootRoute.addChildren([
  loginRoute,
  callbackRoute,
  shellRoute.addChildren([
    tabsLayoutRoute.addChildren([
      healthRoute,
      modelsRoute,
      deploymentsRoute,
      clustersRoute,
    ]),
    modelDetailRoute,
    deploymentDetailRoute,
    replicaDetailRoute,
    clusterDetailRoute,
    pilotJobDetailRoute,
  ]),
]);

export const router = createRouter({ routeTree, basepath: "/admin-console" });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
