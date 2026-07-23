import { queryOptions } from "@tanstack/react-query";
import { whoamiWhoamiGetOptions } from "@/lib/client/@tanstack/react-query.gen";

export const userQueries = {
  whoami: () =>
    queryOptions({
      ...whoamiWhoamiGetOptions(),
    }),
};
