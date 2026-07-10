-- The single idempotent compensator.  Every cleanup path (request `finally`,
-- lease sweep, retries of either) converges here; absent row => no-op, so
-- concurrent callers race safely and exactly one applies.
--
-- The caller pre-reads the reservation to build all KEYS from the stored
-- model/user/backend.  A concurrent settle between pre-read and this script
-- is safe: the GET guard below detects it and returns {0}.
--
-- KEYS[1]  rt:reserve:{request_id}          reservation blob
-- KEYS[2]  rt:deadlines                     ZSET request_id → deadline_ts
-- KEYS[3]  rt:model:{model}:inflight        HASH backend_id → count
-- KEYS[4]  quota:{model}:{user}:inflight    user concurrency counter
-- KEYS[5]  quota:{model}:{user}:tokens      GCRA TAT
-- KEYS[6]  rt:model:{model}:demand          HASH {inflight, ...}
--
-- ARGV[1]  actual_tokens ('' if unknown/estimated)
-- ARGV[2]  request_id (for ZREM in the race-loss path)
--
-- Returns {1} if applied, {0} if already settled.

local reservation_key    = KEYS[1]
local deadlines_key      = KEYS[2]
local model_inflight_key = KEYS[3]
local user_inflight_key  = KEYS[4]
local quota_tokens_key   = KEYS[5]
local model_demand_key   = KEYS[6]

local actual_tokens = ARGV[1]
local request_id    = ARGV[2]

local raw = redis.call('GET', reservation_key)
if not raw then
  redis.call('ZREM', deadlines_key, request_id)
  return {0}
end

local row = cjson.decode(raw)

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6

-- backend inflight (clamped to zero)
if redis.call('HINCRBY', model_inflight_key, row.backend_id, -1) < 0 then
  redis.call('HSET', model_inflight_key, row.backend_id, 0)
end

-- user inflight (delete at zero)
if redis.call('DECR', user_inflight_key) <= 0 then
  redis.call('DEL', user_inflight_key)
end

-- GCRA correction: adjust TAT by (actual - estimated) / rate
if row.tokens_per_sec > 0 and actual_tokens ~= '' then
  local delta = tonumber(actual_tokens) - row.est_tokens
  if delta ~= 0 then
    local arrival_time = tonumber(redis.call('GET', quota_tokens_key) or '0')
    if arrival_time > 0 then
      arrival_time = arrival_time + delta / row.tokens_per_sec
      if arrival_time <= now then
        redis.call('DEL', quota_tokens_key)
      else
        local tau = row.burst_tokens / row.tokens_per_sec
        redis.call('SET', quota_tokens_key, tostring(arrival_time),
                   'EX', math.ceil((arrival_time - now) + tau) + 1)
      end
    end
  end
end

-- demand gauge (clamped to zero)
if redis.call('HINCRBY', model_demand_key, 'inflight', -1) < 0 then
  redis.call('HSET', model_demand_key, 'inflight', 0)
end

redis.call('ZREM', deadlines_key, row.request_id)
redis.call('DEL', reservation_key)
return {1}
