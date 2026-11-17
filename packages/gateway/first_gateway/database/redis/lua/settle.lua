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
-- KEYS[7]  usage:{model}:{user}:input_token_chars_ewma    chars per input token
-- KEYS[8]  usage:{model}:{user}:output_token_ewma         mean output tokens
--
-- ARGV[1]  actual_tokens ('' if unknown/estimated)
-- ARGV[2]  request_id (for ZREM in the race-loss path)
-- ARGV[3]  chars-per-token sample ('' for none)
-- ARGV[4]  output-tokens sample ('' for none)
-- ARGV[5]  EWMA smoothing factor
-- ARGV[6]  EWMA key TTL, seconds
--
-- Returns {applied, chars_per_token_ewma, output_tokens_ewma}: applied is 1,
-- or 0 if already settled; the EWMAs are their values after this call, as
-- strings ('' when unset), and only reported when a sample was given.

local reservation_key      = KEYS[1]
local deadlines_key        = KEYS[2]
local backend_inflight_key = KEYS[3]
local user_inflight_key    = KEYS[4]
local quota_tokens_key     = KEYS[5]
local model_inflight_key   = KEYS[6]

local actual_tokens = ARGV[1]
local request_id    = ARGV[2]

-- Token-estimation calibration: fold this request's samples into the EWMAs.
-- Best-effort and independent of the reservation: it only sharpens estimates.
local function fold(key, sample)
  local prev = tonumber(redis.call('GET', key) or '')
  if sample == '' then
    return prev and tostring(prev) or ''
  end
  local alpha = tonumber(ARGV[5])
  local value = tonumber(sample)
  if prev then
    value = alpha * value + (1 - alpha) * prev
  end
  local text = string.format('%.17g', value)
  redis.call('SET', key, text, 'EX', tonumber(ARGV[6]))
  return text
end

local chars_ewma, output_ewma = '', ''
if ARGV[3] ~= '' or ARGV[4] ~= '' then
  chars_ewma  = fold(KEYS[7], ARGV[3])
  output_ewma = fold(KEYS[8], ARGV[4])
end

-- clean up inflight membership and deadline
redis.call('ZREM', backend_inflight_key, request_id)
redis.call('ZREM', user_inflight_key,    request_id)
redis.call('ZREM', model_inflight_key,   request_id)
redis.call('ZREM', deadlines_key, request_id)


local raw = redis.call('GET', reservation_key)
if not raw then
  -- reservation has already been settled:
  return {0, chars_ewma, output_ewma}
end

-- delete reservation
redis.call('DEL', reservation_key)

-- attempt to load reservation to settle token usage quota
local ok, row = pcall(cjson.decode, raw)
if not ok or type(row) ~= "table" then
  redis.log(redis.LOG_WARNING, "failed to decode JSON: " .. tostring(row))
  return {1, chars_ewma, output_ewma}
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

return {1, chars_ewma, output_ewma}
