-- The single idempotent compensator.  Every cleanup path (request `finally`,
-- lease sweep, retries of either) converges here; absent row => no-op, so
-- concurrent callers race safely and exactly one applies.
--
-- The caller pre-reads the reservation to build all KEYS from the stored
-- model/user/backend.  A concurrent settle between pre-read and this script
-- is safe: the GET guard below detects it and returns {0}.
--
-- KEYS[1]  rt:reserve:{request_id}                        reservation blob
-- KEYS[2]  rt:deadlines                                   ZSET request_id -> deadline_ts
-- KEYS[3]  rt:model:{model}:backend:{backend}:inflight    ZSET request_id -> admit_ts
-- KEYS[4]  rt:user-inflight:{model}:{user}                ZSET request_id -> admit_ts
-- KEYS[5]  quota:{model}:{user}:tokens                    GCRA TAT
-- KEYS[6]  rt:model:{model}:inflight                      ZSET request_id -> admit_ts
--
-- ARGV[1]  actual_tokens ('' if unknown/estimated)
-- ARGV[2]  request_id (for ZREM in the race-loss path)
--
-- Returns {1} if applied, {0} if already settled

local reservation_key      = KEYS[1]
local deadlines_key        = KEYS[2]
local backend_inflight_key = KEYS[3]
local user_inflight_key    = KEYS[4]
local quota_tokens_key     = KEYS[5]
local model_inflight_key   = KEYS[6]

local actual_tokens = ARGV[1]
local request_id    = ARGV[2]

-- clean up inflight membership and deadline
redis.call('ZREM', backend_inflight_key, request_id)
redis.call('ZREM', user_inflight_key,    request_id)
redis.call('ZREM', model_inflight_key,   request_id)
redis.call('ZREM', deadlines_key, request_id)


local raw = redis.call('GET', reservation_key)
if not raw then
  -- reservation has already been settled:
  return {0}
end

-- delete reservation
redis.call('DEL', reservation_key)

-- attempt to load reservation to settle token usage quota
local ok, row = pcall(cjson.decode, raw)
if not ok or type(row) ~= "table" then
  redis.log(redis.LOG_WARNING, "failed to decode JSON: " .. tostring(row))
  return {1}
end

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6

-- GCRA correction: adjust TAT by (actual - estimated) / rate.
-- The guard requires all three fields present.  Lua treats 0 as truthy,
-- so non-LLM rows (est_tokens=0) still get corrected when actuals arrive.
local tps = row.tokens_per_sec or 0
if tps > 0 and actual_tokens ~= '' and row.est_tokens and row.burst_tokens then
  local delta = tonumber(actual_tokens) - row.est_tokens
  if delta ~= 0 then
    local arrival_time = tonumber(redis.call('GET', quota_tokens_key) or '0')
    if arrival_time > 0 then
      arrival_time = arrival_time + delta / tps
      if arrival_time <= now then
        redis.call('DEL', quota_tokens_key)
      else
        local tau = row.burst_tokens / tps
        redis.call('SET', quota_tokens_key, tostring(arrival_time),
                   'EX', math.ceil((arrival_time - now) + tau) + 1)
      end
    end
  end
end

return {1}
