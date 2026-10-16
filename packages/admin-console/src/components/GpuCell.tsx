import { TriangleAlert, Gauge, Thermometer, Zap } from "lucide-react";
import { type GpuInfo } from "@/lib/client";
import { type Severity } from "@/lib/severity";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { cn, formatAgo, formatTimestamp, gib } from "@/lib/utils";

/** Bits of nvidia-smi `clocks_throttle_reasons.active` (NVML's nvmlClocksThrottleReason*). */
const THROTTLE_REASONS: [bit: bigint, label: string, severity: Severity][] = [
  [0x1n, "GPU idle", "ok"],
  [0x2n, "Application clocks setting", "ok"],
  [0x4n, "Software power cap", "ok"],
  [0x8n, "Hardware slowdown", "warning"],
  [0x10n, "Sync boost", "ok"],
  [0x20n, "Software thermal slowdown", "warning"],
  [0x40n, "Hardware thermal slowdown", "warning"],
  [0x80n, "Hardware power brake slowdown", "warning"],
  [0x100n, "Display clock setting", "ok"],
];

type ThrottleReason = { label: string; severity: Severity };

/** Decode a throttle-reason bitmask such as "0x0000000000000004". */
function throttleReasons(mask: string): ThrottleReason[] | null {
  let bits: bigint;
  try {
    bits = BigInt(mask);
  } catch {
    return null;
  }
  const reasons: ThrottleReason[] = [];
  for (const [bit, label, severity] of THROTTLE_REASONS) {
    if (bits & bit) reasons.push({ label, severity });
    bits &= ~bit;
  }
  if (bits) {
    reasons.push({ label: `Unknown (0x${bits.toString(16)})`, severity: "ok" });
  }
  return reasons;
}

type GpuIssue = { severity: Exclude<Severity, "ok">; message: string };

/** Hardware faults worth an operator's attention, worst first. */
function gpuIssues(gpu: GpuInfo): GpuIssue[] {
  const issues: GpuIssue[] = [];
  const plural = (n: number) =>
    `${n} uncorrected ECC error${n === 1 ? "" : "s"}`;
  if (gpu.ecc_uncorrected_volatile) {
    issues.push({
      severity: "critical",
      message: `${plural(gpu.ecc_uncorrected_volatile)} since driver load`,
    });
  }
  if (gpu.ecc_uncorrected_aggregate) {
    issues.push({
      severity: "warning",
      message: `${plural(gpu.ecc_uncorrected_aggregate)} over the GPU's lifetime`,
    });
  }
  const slowdowns = gpu.clocks_throttle_reasons
    ? (throttleReasons(gpu.clocks_throttle_reasons) ?? []).filter(
        (r) => r.severity !== "ok",
      )
    : [];
  if (slowdowns.length > 0) {
    issues.push({
      severity: "warning",
      message: `Clocks throttled: ${slowdowns.map((r) => r.label).join(", ")}`,
    });
  }
  return issues;
}

/** Highest severity among `issues`, or null if there are none. */
function worst(issues: GpuIssue[]): GpuIssue["severity"] | null {
  if (issues.some((i) => i.severity === "critical")) return "critical";
  return issues.length > 0 ? "warning" : null;
}

const ISSUE_TEXT: Record<GpuIssue["severity"], string> = {
  critical: "text-destructive",
  warning: "text-warning",
};

const SEVERITY_BADGE: Record<Severity, "outline" | "warning" | "destructive"> =
  {
    ok: "outline",
    warning: "warning",
    critical: "destructive",
  };

function memoryPct(gpu: GpuInfo): number {
  return Math.min(
    100,
    Math.round((gpu.memory_used_mib / gpu.memory_total_mib) * 100),
  );
}

/** Memory occupancy bar, green → amber → red as the GPU fills. */
function MemoryBar({ pct, className }: { pct: number; className?: string }) {
  return (
    <span
      className={cn(
        "block h-1.5 overflow-hidden rounded-full bg-muted",
        className,
      )}
    >
      <span
        className={cn(
          "block h-full rounded-full transition-[width]",
          pct >= 90
            ? "bg-destructive"
            : pct >= 70
              ? "bg-warning"
              : "bg-success",
        )}
        style={{ width: `${pct}%` }}
      />
    </span>
  );
}

/** Live readings that fit the compact cell; each is hidden when unreported. */
function MetricsRow({ gpu }: { gpu: GpuInfo }) {
  const metrics = [
    gpu.utilization_gpu_pct != null && {
      icon: Gauge,
      text: `${gpu.utilization_gpu_pct}%`,
      title: "GPU utilization",
    },
    gpu.temperature_c != null && {
      icon: Thermometer,
      text: `${gpu.temperature_c}°C`,
      title: "Temperature",
    },
    gpu.power_draw_w != null && {
      icon: Zap,
      text: `${Math.round(gpu.power_draw_w)} W`,
      title: "Power draw",
    },
  ].filter((m) => m !== false);
  if (metrics.length === 0) return null;
  return (
    <span className="flex flex-wrap gap-x-2 gap-y-1 text-xs text-muted-foreground tabular-nums">
      {metrics.map(({ icon: Icon, text, title }) => (
        <span key={title} className="flex items-center gap-1" title={title}>
          <Icon className="size-3" />
          {text}
        </span>
      ))}
    </span>
  );
}

/** A value, or a muted dash when the GPU does not report it. */
function Reported({
  value,
  children,
}: {
  value: string | number | null | undefined;
  children?: React.ReactNode;
}) {
  if (value == null) {
    return (
      <span className="text-muted-foreground" title="Not reported by this GPU">
        —
      </span>
    );
  }
  return <>{children ?? String(value)}</>;
}

/** Labelled group of field/value rows in the details dialog. */
function DetailSection({
  title,
  rows,
}: {
  title: string;
  rows: [label: string, value: React.ReactNode][];
}) {
  return (
    <section className="space-y-2">
      <h3 className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
        {title}
      </h3>
      <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1">
        {rows.map(([label, value]) => (
          <div key={label} className="contents">
            <dt className="text-muted-foreground">{label}</dt>
            <dd className="min-w-0 tabular-nums">{value}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}

function ThrottleBadges({ mask }: { mask: string }) {
  const reasons = throttleReasons(mask);
  if (reasons === null) return <span className="font-mono">{mask}</span>;
  if (reasons.length === 0) return <>None</>;
  return (
    <span className="flex flex-wrap gap-1" title={mask}>
      {reasons.map((r) => (
        <Badge key={r.label} variant={SEVERITY_BADGE[r.severity]}>
          {r.label}
        </Badge>
      ))}
    </span>
  );
}

function GpuDetails({
  gpu,
  hostname,
  observedAt,
  issues,
}: {
  gpu: GpuInfo;
  hostname: string;
  observedAt: string | null | undefined;
  issues: GpuIssue[];
}) {
  const pct = memoryPct(gpu);
  const mib = (v: number | null | undefined) => (
    <Reported value={v}>{v?.toLocaleString()} MiB</Reported>
  );
  const percent = (v: number | null | undefined) => (
    <Reported value={v}>{v}%</Reported>
  );

  return (
    <DialogContent className="sm:max-w-md">
      <DialogHeader>
        <DialogTitle className="pr-8">
          GPU {gpu.index} · {gpu.name}
        </DialogTitle>
        <DialogDescription className="font-mono">{hostname}</DialogDescription>
      </DialogHeader>

      {issues.length > 0 && (
        <ul className="space-y-1">
          {issues.map((issue) => (
            <li
              key={issue.message}
              className={cn(
                "flex items-start gap-2",
                ISSUE_TEXT[issue.severity],
              )}
            >
              <TriangleAlert className="mt-0.5 size-4 shrink-0" />
              {issue.message}
            </li>
          ))}
        </ul>
      )}

      <section className="space-y-2">
        <div className="flex items-baseline justify-between gap-2 tabular-nums">
          <span>
            {gib(gpu.memory_used_mib)} / {gib(gpu.memory_total_mib)} GiB
          </span>
          <span className="text-muted-foreground">{pct}% used</span>
        </div>
        <MemoryBar pct={pct} className="h-2" />
      </section>

      <DetailSection
        title="Memory"
        rows={[
          ["Used", mib(gpu.memory_used_mib)],
          ["Free", mib(gpu.memory_free_mib)],
          ["Total", mib(gpu.memory_total_mib)],
          ["Bandwidth utilization", percent(gpu.utilization_memory_pct)],
        ]}
      />

      <DetailSection
        title="Compute"
        rows={[
          ["GPU utilization", percent(gpu.utilization_gpu_pct)],
          [
            "Temperature",
            <Reported value={gpu.temperature_c}>
              {gpu.temperature_c}°C
            </Reported>,
          ],
          [
            "Power draw",
            <Reported value={gpu.power_draw_w}>
              {gpu.power_draw_w?.toFixed(1)} W
            </Reported>,
          ],
          [
            "Performance state",
            <Reported value={gpu.pstate}>
              <span title="P0 is maximum performance, P12 minimum">
                {gpu.pstate}
              </span>
            </Reported>,
          ],
          [
            "Clock throttling",
            <Reported value={gpu.clocks_throttle_reasons}>
              <ThrottleBadges mask={gpu.clocks_throttle_reasons ?? ""} />
            </Reported>,
          ],
        ]}
      />

      <DetailSection
        title="Health & identity"
        rows={[
          [
            "Uncorrected ECC (volatile)",
            <Reported value={gpu.ecc_uncorrected_volatile} />,
          ],
          [
            "Uncorrected ECC (lifetime)",
            <Reported value={gpu.ecc_uncorrected_aggregate} />,
          ],
          ["Driver", <Reported value={gpu.driver_version} />],
          [
            "UUID",
            <Reported value={gpu.uuid}>
              <span className="font-mono text-xs break-all">{gpu.uuid}</span>
            </Reported>,
          ],
        ]}
      />

      {observedAt && (
        <DialogFooter className="text-xs text-muted-foreground sm:justify-start">
          <span title={formatTimestamp(observedAt)}>
            Observed {formatAgo(observedAt)}
          </span>
        </DialogFooter>
      )}
    </DialogContent>
  );
}

/**
 * One GPU as a compact grid cell: index, device name, memory bar and live
 * readings. Click opens the full nvidia-smi report.
 */
export function GpuCell({
  gpu,
  hostname,
  observedAt,
}: {
  gpu: GpuInfo;
  hostname: string;
  observedAt: string | null | undefined;
}) {
  const pct = memoryPct(gpu);
  const issues = gpuIssues(gpu);
  const severity = worst(issues);

  return (
    <Dialog>
      <DialogTrigger asChild>
        <button
          type="button"
          className={cn(
            "w-full cursor-pointer space-y-1 rounded-md border border-border bg-background p-2 text-left transition-colors outline-none hover:bg-muted/50 focus-visible:ring-[3px] focus-visible:ring-ring/50",
            severity === "critical" && "border-destructive/60",
            severity === "warning" && "border-warning/60",
          )}
        >
          <span className="flex items-center justify-between gap-2">
            <span className="flex items-center gap-1 font-mono text-xs text-muted-foreground">
              GPU {gpu.index}
              {severity && (
                <span title={issues.map((i) => i.message).join("\n")}>
                  <TriangleAlert
                    className={cn("size-3", ISSUE_TEXT[severity])}
                  />
                </span>
              )}
            </span>
            <span className="text-xs tabular-nums">{pct}%</span>
          </span>
          <span className="block truncate text-xs" title={gpu.name}>
            {gpu.name}
          </span>
          <MemoryBar pct={pct} />
          <span className="block text-xs text-muted-foreground tabular-nums">
            {gib(gpu.memory_used_mib)} / {gib(gpu.memory_total_mib)} GiB
          </span>
          <MetricsRow gpu={gpu} />
        </button>
      </DialogTrigger>
      <GpuDetails
        gpu={gpu}
        hostname={hostname}
        observedAt={observedAt}
        issues={issues}
      />
    </Dialog>
  );
}
