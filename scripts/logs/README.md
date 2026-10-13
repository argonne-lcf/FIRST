# Log tooling

Offline tools over the gateway's JSONL logs. They are not part of the runtime
analytics pipeline — that is `docs/architecture/structured-logging-clickhouse.md`
and `deploy/analytics/`.

| Tool | Corpus | Output |
| --- | --- | --- |
| `first-slogs` (Rust) | the V1 gateway's stream set: `access_log`, `app`, `batch_log`, `batch_metrics`, `request_log`, `request_metrics`, and `user` | partitioned parquet under `--dataset-dir`, plus squashfs bundles of large request payloads |
| `first_stats_query` (Python) | those parquet partitions | tables, sums, and plots (cluster, institution, status code, tokens, inference overview) |

`first-slogs` does not parse the V2 gateway's `request` / `response` /
`inference` events, which carry an `event_id` and reach ClickHouse through
Vector instead. Porting the tool to them would change its parquet layout and its
upsert/streaming-metrics handling, so it is a separate piece of work.

`scripts/` holds convenience wrappers (`all-stats.sh`, `atpesc-2026-stats.sh`).
