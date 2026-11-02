-- Analytics schema for the FIRSTv2 structured-logging pipeline.
-- Source of truth: docs/architecture/structured-logging-clickhouse.md
--
-- Safe to re-run. Apply with:
--   export CLICKHOUSE_VECTOR_PASSWORD=... CLICKHOUSE_GRAFANA_PASSWORD=...
--   envsubst < schema.sql | docker compose exec -T clickhouse clickhouse-client --multiquery
--
-- Two deviations from the design doc's DDL, both noted in deploy/analytics/README.md:
--   * usage_inference uses ReplacingMergeTree(timestamp); the doc's
--     ReplacingMergeTree(event_id) is rejected because a UUID cannot be a
--     version column.
--   * the 13-month TTL has no TO VOLUME 'cold' clause, because a cold volume
--     must exist in the server's storage config first.

CREATE DATABASE IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- Consumers: Vector inserts; Grafana reads.
-- ---------------------------------------------------------------------------
CREATE USER IF NOT EXISTS vector IDENTIFIED BY '$CLICKHOUSE_VECTOR_PASSWORD'
SETTINGS async_insert = 1;
GRANT INSERT ON analytics.events_raw TO vector;

CREATE USER IF NOT EXISTS grafana IDENTIFIED BY '$CLICKHOUSE_GRAFANA_PASSWORD';
GRANT SELECT ON analytics.* TO grafana;

-- ---------------------------------------------------------------------------
-- Layer 1: events_raw — the Vector landing table. Analytics never query it.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.events_raw
(
    event_id UUID,
    request_id UUID,
    stream LowCardinality(String),            -- request | response | inference
    timestamp DateTime64(3, 'UTC'),
    user_id LowCardinality(String) DEFAULT '',
    model LowCardinality(String) DEFAULT '',
    deployment LowCardinality(String) DEFAULT '',
    cluster LowCardinality(String) DEFAULT '',
    backend_id LowCardinality(String) DEFAULT '',
    endpoint LowCardinality(String) DEFAULT '',
    outcome LowCardinality(String) DEFAULT '',
    attempt UInt8 DEFAULT 0,
    status_code UInt16 DEFAULT 0,
    path String DEFAULT '',
    input_tokens Nullable(UInt32),
    output_tokens Nullable(UInt32),
    total_tokens Nullable(UInt32),
    body String DEFAULT '' CODEC(ZSTD(3)),    -- inline only when <= 2 KB
    body_stored Bool DEFAULT false,           -- emitted by the app today
    body_ref String DEFAULT '',               -- object-store key when off-lined
    body_bytes UInt64 DEFAULT 0,
    body_sha256 FixedString(64) DEFAULT '',
    raw String CODEC(ZSTD(3)) TTL toDateTime(timestamp) + INTERVAL 30 DAY
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (event_id)
TTL toDateTime(timestamp) + INTERVAL 13 MONTH;

-- ---------------------------------------------------------------------------
-- Layer 2: usage_inference — one row per inference attempt, sorted for the
-- per-user and per-model queries the usage API and dashboards run.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.usage_inference
(
    timestamp DateTime64(3, 'UTC'),
    user_id LowCardinality(String), model LowCardinality(String),
    cluster LowCardinality(String), deployment LowCardinality(String),
    endpoint LowCardinality(String), outcome LowCardinality(String), attempt UInt8,
    request_id UUID, event_id UUID,
    input_tokens UInt32, output_tokens UInt32, total_tokens UInt32
)
ENGINE = ReplacingMergeTree(timestamp)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (user_id, model, toStartOfHour(timestamp), request_id, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_usage_inference
TO analytics.usage_inference AS
SELECT timestamp, user_id, model, cluster, deployment, endpoint, outcome, attempt,
       request_id, event_id,
       ifNull(input_tokens, 0) AS input_tokens,
       ifNull(output_tokens, 0) AS output_tokens,
       ifNull(total_tokens, 0) AS total_tokens
FROM analytics.events_raw WHERE stream = 'inference';

-- Second physical sort order for "top models" queries with no user filter.
ALTER TABLE analytics.usage_inference
ADD PROJECTION IF NOT EXISTS by_model_time (
    SELECT model, cluster, timestamp, total_tokens, input_tokens, output_tokens
    ORDER BY (model, toStartOfHour(timestamp)));

-- Re-running this re-materializes the projection; it is cheap while the table
-- is small and expensive once it is not.
ALTER TABLE analytics.usage_inference MATERIALIZE PROJECTION by_model_time;

-- ---------------------------------------------------------------------------
-- Layer 3: usage_daily — the hot per-user rollup the usage API reads.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.usage_daily
(
    day Date, user_id LowCardinality(String), model LowCardinality(String),
    cluster LowCardinality(String),
    total_tokens AggregateFunction(sum, UInt32),
    input_tokens AggregateFunction(sum, UInt32),
    output_tokens AggregateFunction(sum, UInt32),
    requests AggregateFunction(uniq, UUID)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (user_id, model, cluster, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_usage_daily
TO analytics.usage_daily AS
SELECT toDate(timestamp) AS day, user_id, model, cluster,
       sumState(total_tokens) AS total_tokens,
       sumState(input_tokens) AS input_tokens,
       sumState(output_tokens) AS output_tokens,
       uniqState(request_id) AS requests
FROM analytics.usage_inference WHERE outcome = 'success'
GROUP BY day, user_id, model, cluster;

-- ---------------------------------------------------------------------------
-- Loss detection (the query behind the Grafana alert). A successful response
-- with no successful inference attempt is a dropped usage row.
-- ---------------------------------------------------------------------------
-- SELECT count() AS lost_usage_rows
-- FROM (SELECT request_id, path FROM analytics.events_raw
--       WHERE stream = 'request' AND timestamp > now() - INTERVAL 1 HOUR) r
-- LEFT JOIN (SELECT request_id, argMax(status_code, timestamp) AS status_code
--            FROM analytics.events_raw WHERE stream = 'response'
--            GROUP BY request_id) resp USING (request_id)
-- LEFT JOIN (SELECT DISTINCT request_id FROM analytics.events_raw
--            WHERE stream = 'inference' AND outcome = 'success') inf USING (request_id)
-- WHERE resp.status_code BETWEEN 200 AND 299
--   AND r.path LIKE '%/v1/%' AND inf.request_id IS NULL
-- SETTINGS join_use_nulls = 1;
