import { queryOptions } from "@tanstack/react-query";
import {
  listModelsCatalogV1ModelsGetOptions,
  getRouterConfigCatalogV1RouterConfigGetOptions,
} from "@/lib/client/@tanstack/react-query.gen";

export const modelQueries = {
  // Live-updating: the runtime column (in-flight / capacity rejects) changes
  // continuously, so refetch on a short interval.
  all: () =>
    queryOptions({
      ...listModelsCatalogV1ModelsGetOptions(),
      refetchInterval: 5_000,
      staleTime: 2_000,
    }),

  routerConfig: () =>
    queryOptions({
      ...getRouterConfigCatalogV1RouterConfigGetOptions(),
      refetchInterval: 15_000,
    }),
};
