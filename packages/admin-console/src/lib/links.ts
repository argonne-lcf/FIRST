import { type LinkProps } from "@tanstack/react-router";
import { slugify } from "@/lib/utils";

/**
 * Link targets for each resource's detail view, built from resource *names*
 * (as they appear in foreign-key fields like `cluster_name`). One place for
 * the name → route mapping so every cross-reference links consistently.
 */
export const resourceLinks = {
  cluster: (name: string): LinkProps => ({
    to: "/clusters/$clusterSlug",
    params: { clusterSlug: slugify(name) },
  }),

  model: (name: string): LinkProps => ({
    to: "/models/$modelSlug",
    params: { modelSlug: slugify(name) },
  }),

  deployment: (kind: "pilot" | "static", name: string): LinkProps => ({
    to: "/deployments/$kind/$deploymentSlug",
    params: { kind, deploymentSlug: slugify(name) },
  }),

  pilotJob: (clusterName: string, jobName: string): LinkProps => ({
    to: "/clusters/$clusterSlug/jobs/$jobSlug",
    params: { clusterSlug: slugify(clusterName), jobSlug: slugify(jobName) },
  }),

  replica: (deploymentName: string, replicaName: string): LinkProps => ({
    to: "/deployments/pilot/$deploymentSlug/replicas/$replicaSlug",
    params: {
      deploymentSlug: slugify(deploymentName),
      replicaSlug: slugify(replicaName),
    },
  }),

  /** The launch template card on its pilot deployment's detail page. */
  launchTemplate: (deploymentName: string): LinkProps => ({
    to: "/deployments/$kind/$deploymentSlug",
    params: { kind: "pilot", deploymentSlug: slugify(deploymentName) },
    hash: "launch-template",
  }),
};
