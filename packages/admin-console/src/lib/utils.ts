import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/**
 * Reverse a resource `slug` back into its `name`. Slugs replace '/' (allowed in
 * names, unsafe in URLs) with '~'; this is the inverse of that bijection.
 */
export function unslug(slug: string): string {
  return slug.split("~").join("/");
}

/** Turn a resource `name` into its URL-safe `slug`. Inverse of {@link unslug}. */
export function slugify(name: string): string {
  return name.split("/").join("~");
}

/** MiB → GiB with one decimal, e.g. 81920 → "80.0". */
export function gib(mib: number): string {
  return (mib / 1024).toFixed(1);
}

/** Format an ISO timestamp for display, or "—" when absent. */
export function formatTimestamp(iso: string | null | undefined): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString();
}

/** Turn a snake_case status/state token into "Title case" prose. */
export function humanize(token: string): string {
  return token.replace(/_/g, " ").replace(/^\w/, (c) => c.toUpperCase());
}

/** Elapsed time since an ISO timestamp in prose, e.g. "2 hours ago". */
export function formatAgo(iso: string | null | undefined): string {
  if (!iso) return "—";
  const seconds = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  const hours = Math.floor(seconds / 3600);
  if (hours > 0) return `${hours} hour${hours === 1 ? "" : "s"} ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes === 0) return "just now";
  return `${minutes} minute${minutes === 1 ? "" : "s"} ago`;
}

/** Compact elapsed time since an ISO timestamp, e.g. "2h 14m", or "—". */
export function formatSince(iso: string | null | undefined): string {
  if (!iso) return "—";
  const seconds = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

/**
 * Compact time remaining until an ISO timestamp, relative to `now` (ms), e.g.
 * "in 4m 12s"; "due" once it has passed, "—" when absent.
 */
export function formatUntil(
  iso: string | null | undefined,
  now: number = Date.now(),
): string {
  if (!iso) return "—";
  const seconds = Math.round((new Date(iso).getTime() - now) / 1000);
  if (seconds <= 0) return "due";
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (h > 0) return `in ${h}h ${m}m`;
  if (m > 0) return `in ${m}m ${s}s`;
  return `in ${s}s`;
}
