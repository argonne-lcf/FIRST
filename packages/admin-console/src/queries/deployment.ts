import { queryOptions } from "@tanstack/react-query";
import {
  listPilotDeploymentsCatalogV1DeploymentsPilotGetOptions,
  getPilotDeploymentCatalogV1DeploymentsPilotNameGetOptions,
  listStaticDeploymentsCatalogV1DeploymentsStaticGetOptions,
  tailReplicaLogsControlV1PilotReplicasNameLogsGetOptions,
} from "@/lib/client/@tanstack/react-query.gen";

export const deploymentQueries = {
  pilots: () =>
    queryOptions(listPilotDeploymentsCatalogV1DeploymentsPilotGetOptions()),

  pilot: (name: string) =>
    queryOptions({
      ...getPilotDeploymentCatalogV1DeploymentsPilotNameGetOptions({
        path: { name },
      }),
      refetchInterval: 10_000,
    }),

  statics: () =>
    queryOptions({
      ...listStaticDeploymentsCatalogV1DeploymentsStaticGetOptions(),
      refetchInterval: 15_000,
    }),

  /**
   * On-demand tail of a replica's logs. Disabled by default; call `refetch()`
   * from a button to fetch the current tail. `staleTime: 0` so each refetch
   * hits the network for a fresh tail.
   */
  replicaLogs: (name: string) =>
    queryOptions({
      ...tailReplicaLogsControlV1PilotReplicasNameLogsGetOptions({
        path: { name },
      }),
      enabled: false,
      staleTime: 0,
    }),
};
