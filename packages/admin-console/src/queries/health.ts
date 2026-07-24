import { queryOptions } from "@tanstack/react-query";
import { getSystemHealthCatalogV1SystemHealthGetOptions } from "@/lib/client/@tanstack/react-query.gen";

export const healthQueries = {
  system: () =>
    queryOptions({
      ...getSystemHealthCatalogV1SystemHealthGetOptions(),
      refetchInterval: 15_000,
      staleTime: 5_000,
    }),
};
