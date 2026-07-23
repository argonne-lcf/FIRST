import { queryOptions } from "@tanstack/react-query";
import {
  type ClusterSummary,
  type PilotDeploymentSummary,
  type StaticDeploymentSummary,
  listClustersCatalogV1ClustersGet,
  listStaticDeploymentsCatalogV1DeploymentsStaticGet,
  listPilotDeploymentsCatalogV1DeploymentsPilotGet,
} from "@/lib/client";
import {
  listClustersCatalogV1ClustersGetOptions,
  getClusterCatalogV1ClustersNameGetOptions,
} from "@/lib/client/@tanstack/react-query.gen";

/** A cluster joined with the deployments that reference it by name. */
export type ClusterOverview = {
  cluster: ClusterSummary;
  staticDeployments: StaticDeploymentSummary[];
  pilotDeployments: PilotDeploymentSummary[];
};

export const clusterQueries = {
  all: () => queryOptions(listClustersCatalogV1ClustersGetOptions()),

  detail: (name: string) =>
    queryOptions(getClusterCatalogV1ClustersNameGetOptions({ path: { name } })),

  /**
   * Live status dashboard feed: every cluster with its child StaticDeployments
   * and PilotDeployments joined in, refetched on an interval.
   */
  overview: () =>
    queryOptions({
      queryKey: ["clusterOverview"],
      refetchInterval: 15_000,
      queryFn: async ({ signal }): Promise<ClusterOverview[]> => {
        const [clusters, statics, pilots] = await Promise.all([
          listClustersCatalogV1ClustersGet({ signal, throwOnError: true }),
          listStaticDeploymentsCatalogV1DeploymentsStaticGet({
            signal,
            throwOnError: true,
          }),
          listPilotDeploymentsCatalogV1DeploymentsPilotGet({
            signal,
            throwOnError: true,
          }),
        ]);
        return clusters.data.map((cluster) => ({
          cluster,
          staticDeployments: statics.data.filter(
            (d) => d.cluster_name === cluster.name,
          ),
          pilotDeployments: pilots.data.filter(
            (d) => d.cluster_name === cluster.name,
          ),
        }));
      },
    }),
};
