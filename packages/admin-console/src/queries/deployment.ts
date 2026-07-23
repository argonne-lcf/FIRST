import { queryOptions } from "@tanstack/react-query";
import {
  listPilotDeploymentsCatalogV1DeploymentsPilotGetOptions,
  getPilotDeploymentCatalogV1DeploymentsPilotNameGetOptions,
  listStaticDeploymentsCatalogV1DeploymentsStaticGetOptions,
} from "@/lib/client/@tanstack/react-query.gen";

export const deploymentQueries = {
  pilots: () =>
    queryOptions(listPilotDeploymentsCatalogV1DeploymentsPilotGetOptions()),

  pilot: (name: string) =>
    queryOptions(
      getPilotDeploymentCatalogV1DeploymentsPilotNameGetOptions({
        path: { name },
      }),
    ),

  statics: () =>
    queryOptions(listStaticDeploymentsCatalogV1DeploymentsStaticGetOptions()),
};
