import { queryOptions } from "@tanstack/react-query";
import { listLaunchTemplatesCatalogV1LaunchTemplatesGetOptions } from "@/lib/client/@tanstack/react-query.gen";

export const launchTemplateQueries = {
  all: () =>
    queryOptions({
      ...listLaunchTemplatesCatalogV1LaunchTemplatesGetOptions(),
      staleTime: 30_000,
    }),

  /** One template by name (admin route only lists, so pick from the list). */
  byName: (name: string) =>
    queryOptions({
      ...launchTemplateQueries.all(),
      select: (templates) => templates.find((t) => t.name === name) ?? null,
    }),
};
