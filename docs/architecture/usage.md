# ALCF Inference Service Usage Accounting

The facility hosts a dynamic catalog of models across four heterogeneous clusters. Users receive allocations through a merit process.

A naive token-based allocation scheme is fundamentally unusable for capacity planning, because the cost of a token varies by orders of magnitude depending on the model and workload.  Awarding a project "50 million tokens" is analogous to awarding "1000 simulations" on Aurora: the unit is ambiguous with respect to the amount of consumed compute.

The accounting system should ration the finite service resources in units that track the capacity users actually consume.

1. **A unit is a fixed amount of work.** One unit corresponds to a fixed amount of real GPU time, so the total units issued per period equals fleet capacity.
2. **Users are charged what their request cost at published rates.** A live rate card is updated
when model deployments change.  Users can easily infer the cost of 1M output tokens for any given model.
3. **Rates are empirical.** Every number in the rate card comes from a standard benchmark tool.

## Hardware and the accounting unit

### Hardware

| Cluster | Nodes | Devices | Total devices |
|---|---|---|---|
| GH200 | 672 | 4× GH200 (H100 96GB + Grace) | 2,688 |
| B200 | 8 | 8× B200 | 64 |
| A100 | 10 + 1 | 8× A100 40GB; one node 8× A100 80GB | 88 |
| SambaNova | 6 | 16× SN40L | 96 |

### Inference Service Unit (ISU)

**1 ISU = 1 GH200-second of work.** GH200 is the reference because it is over 90% of fleet capacity.

### Device weights

Each device type $g$ has a weight $w_g$ equal to its measured aggregate throughput relative to a GH200 on the calibration suite. Ballpark values, to be replaced by measurement:

| Device | Ballpark $w_g$ |
|---|---|
| GH200 | 1.0 |
| B200 | 2.0–2.5 |
| A100 80GB | ~0.4 |
| A100 40GB | ~0.3 |
| SN40L | measure |

The weight is a throughput ratio and nothing else. Scarcity of the B200s is handled by rate-limiting, not by inflating the weight.

### Total capacity

For a period of $H$ hours at target utilization $u$:

$
Capacity (ISU) = \sum_{g} n_g \cdot w_g \cdot H \cdot 3600 \cdot u
$


Illustrative yearly (31.5M seconds) figures at 100% utilization:

- GH200 ≈ 84.7B
- B200 ≈ 4.7B
- A100 ≈ 0.86B
- SN40L ≈ 2.9B

Total ≈ 93B. At *u* = 0.8 that is ≈ 74B; after a 15% reserve, ≈ 63B allocatable.

Committees now have a finite number to ration, with a unit that directly relates
to ALCF capacity.

## Model Rate

For model *m* on cluster *c* with device type *g*:

$r(m, c) = w_g /T_g(m)$

where *T_g(m)* is the measured aggregate tokens per device-second for *m* on *g* at saturation, summed across all concurrent requests. Concurrency is inside *T*: a model that batches 64 requests on a device shows a *T* eight times higher than one that batches 8 at the same per-request speed.

### Why the same model costs about the same on any cluster

Since *w_g = T_g(cal) / T_ref(cal)*, the rate becomes:

```
r(m, c) = T_g(cal) / ( T_ref(cal) · T_g(m) )
```

Rates match across clusters exactly when the model's speedup on *g* relative to the reference equals the calibration suite's speedup. A device that halves GPU-seconds per token has a weight about twice as large, and the two cancel. Concurrency, per-request speed, and precision paths are all inside *T* on both sides, so a higher batch size on one cluster is not an extra denominator; it is already in the weight.

The cancellation breaks when a model batches differently than the calibration suite. A 70B model may reach concurrency 64 on B200 but only 8 on A100 40GB because the KV cache runs out of room, while the calibration models fit comfortably on both. That model's A100 rate then comes out well above its B200 rate. This is the right outcome under "unit = work": the A100 is consuming more real capacity per token for that model. It is also the operational signal that the model belongs on the B200s. These residuals are why per-model rates are measured per cluster rather than derived from the weight alone.

### Benchmark rate versus actual load

Benchmarks measure at saturation. Production may run at 30% load, where actual device-seconds per token are higher. Users are charged the published saturation rate regardless. Under-utilization is the facility's cost and already sits in the utilization factor *u*. Charging attributed actual occupancy would make a user's bill depend on who else was on the cluster, which is the placement-variance problem in a different coat.

## Calibration suite versus per-model benchmarks

Two distinct measurement activities.

**Calibration suite (sets weights).** Its only job is to compare devices, so it must run everywhere.

- Two or three dense open models that fit on every device, e.g. an 8B-class and a 70B-class model, plus one MoE if SambaNova supports it.
- Request distribution sampled from the facility's own input and output length histograms once traffic exists; a standard public dataset until then.
- Sweep concurrency to maximum sustainable throughput under a latency floor (e.g. time per output token ≤ 50 ms).
- *w_g* = geometric mean across the suite of tokens per device-second relative to GH200. Geometric mean so no single model dominates.
- For SambaNova, run the intersection of supported models and accept a wider error bar for the first cycle. It optimizes single-stream speed over batch throughput; if its measured weight looks anomalous, treat that cluster as a separately allocated resource rather than forcing it into the unit.
- Rerun once per allocation cycle or when a serving stack changes materially.

**Per-model benchmarks (set rates).** Run on whatever each cluster actually serves, in whatever configuration it serves it. A disaggregated prefill/decode setup or multinode parallelism on GH200 simply produces its own measured *T_g(m)*. If the configuration is 30% more efficient, the rate is 30% lower and the weight never changes. Run at onboarding, on any move or reconfiguration, and on stack changes.

Both activities use the same tool (§6).

## Serving modes, endpoints, and charging

### Shared models (hot, concurrent)

Charged per token, with prefill and decode measured separately because prefill is compute-bound and parallel while decode is memory-bandwidth-bound and sequential (typically 3–5× more per token):

```
cost = N_in · r_in(m, c)  +  N_out · r_out(m, c)
```

Rates are published in context-length bands (e.g. ≤4k, 4k–32k, 32k–128k) to capture quadratic attention in prefill and KV-cache growth in decode. A memory-time term is recorded in telemetry but not charged initially.

### Dedicated models (on demand, single tenant)

Loaded for one project on devices nobody else shares. Per-token rates are meaningless; charged for occupancy:

```
cost = N_devices · w_g · seconds_loaded
```

This is node-hour accounting. Idle time while loaded is charged.

### Cluster-pinned endpoints

The request runs on the named cluster and is charged that cluster's exact rate *r(m, c)*. Intended for power users studying hardware and software stack effects.

### Federated endpoints (default)

The facility chooses placement for availability, maintenance, and failover. One published rate per model regardless of landing cluster. Because *r(m, c)* already includes the device weight, member rates are directly comparable, and with calibrated weights they differ by only a few percent (§3.2).

- Models with a home cluster that federate only for failover: federated rate = home-cluster rate. Failover is the facility's decision and rare; the facility absorbs the delta.
- Models spread across clusters in steady state: federated rate = traffic-weighted average of member rates when the scheduler controls the split and the fractions are known; the higher member rate when the split is unpredictable and the facility prefers never to under-charge.
- A model with a large gap between member rates is a signal it runs much better on one architecture and should probably be pinned there.

The ledger records the actual landing cluster for every request so the federated delta can be monitored.

## Benchmark tool

One tool produces every weight and every rate. For a given (model, cluster, serving configuration) it:

1. Sweeps concurrency up to the maximum sustainable level under the latency floor.
2. Uses the facility's input and output length distributions, covering each context-length band.
3. Runs to steady state and records aggregate tokens per second for prefill and decode separately, device count, and wall-clock seconds.
4. Emits *T_g(m)* per phase per band, from which *r_in* and *r_out* follow, and (in calibration mode) the throughput ratios that set *w_g*.

Results are stored with the rate-card version they produced.

## Rate lifecycle

**Rate card.** Generated from the device weights, per-model benchmark results, and the current model-to-cluster mapping. Updates automatically when a model is onboarded, moved, or reconfigured. Users do not need the catalog in advance; they need the weights to hold and the estimator to reflect today's mapping.

**Cadence.** Weights and calibration baselines are recalibrated once per allocation cycle. Out-of-cycle rate changes only for model additions, removals, moves, or reconfigurations, with notice.

**No retroactivity.** A project awarded 100M units that has spent 10M keeps 90M when rates change; what changes is what those 90M buy going forward. If recalibration reveals under-charging, the reserve absorbs it.

**Versioning.** Every rate card is versioned; every ledger entry records the version it was charged under.

## Allocation, metering, and access

**Allocations** are granted in units per cycle, spendable on any model or endpoint at published rates.

**Metering.** The serving layer emits per request: model, endpoint type, landing cluster, device type, serving configuration, input and output tokens, context band, attributed device-seconds, KV bytes and residency time, and effective concurrency. The charge is computed from the published rate and decrements the balance in near-real time. Attributed actual occupancy is stored for calibration and utilization analysis, not for charging.

**Access policy for scarce hardware.** Per-project concurrency caps on the B200 cluster, and explicit committee grants for B200 pinned access. This keeps the fast cluster fairly shared without distorting the unit.

**Reporting.** Projects see units by model, endpoint, cluster, and day, split by input and output (or occupancy for dedicated models), with balance and burn-rate projection. A public estimator takes token counts or device-hours and returns units from the live rate card.

## Planning loop

Each cycle:

1. Compute fleet capacity in units; subtract reserve.
2. Rerun the calibration suite; rerun per-model benchmarks where configurations changed; publish the rate card.
3. Grant allocations against the pool.
4. During the cycle, monitor utilization per cluster, queue depth, the federated delta, and the gap between published and attributed cost. Utilization low with allocations exhausted means rates are too high; utilization saturated with balances unspent means rates are too low.
5. Hardware changes enter through *n_g* and *w_g* and flow through without changing the user-facing unit.

## Rollout

- **Phase 1, measure.** Deploy telemetry and the benchmark tool. Run the calibration suite on all four clusters. Populate the ledger for a full cycle without charging.
- **Phase 2, shadow.** Publish the rate card and estimator; show projects what they would have been charged.
- **Phase 3, live.** Grant balances, enforce decrements and concurrency caps.
- **Phase 4, refine.** Decide whether to charge the memory-time term for long-context requests, whether SambaNova stays in the unit or becomes a separate resource, and tune the reserve from observed variance.

## Open decisions

- Whether to fold the single A100 80GB node into the 40GB pool at one weight for simplicity.
- Latency floor used to define saturation in the benchmark tool.
- Context-length band boundaries per model family.
- Reserve percentage after two cycles of data.
- Whether failed or cancelled requests are charged, and at what fraction.
- Whether dedicated-model load time is charged or waived.
- Whether to discount batch jobs schedulable onto whichever cluster has slack.