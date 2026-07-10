-- Admission: quota checks once, then walk the ordered candidate list.
-- Commit atomically for the first backend with capacity.
--
-- KEYS[1]    quota:{model}:{user}:tokens     GCRA TAT
-- KEYS[2]    quota:{model}:{user}:rpm        GCRA TAT
-- KEYS[3]    quota:{model}:{user}:inflight   user concurrency counter
-- KEYS[4]    rt:model:{model}:inflight       HASH backend_id → count
-- KEYS[5]    rt:model:{model}:demand         HASH {inflight, capacity_rejects_total, last_reject_ts}
-- KEYS[6]    rt:deadlines                    ZSET request_id → deadline_ts
-- KEYS[7]    rt:reserve:{request_id}         reservation blob
-- KEYS[8..N] rt:backend:{id}:errors          one per candidate (parallel to ARGV triples)
--
-- ARGV[1]  request_id
-- ARGV[2]  model_name
-- ARGV[3]  user_id
-- ARGV[4]  est_tokens
-- ARGV[5]  max_user_concurrency
-- ARGV[6]  tokens_per_sec
-- ARGV[7]  burst_tokens
-- ARGV[8]  requests_per_sec
-- ARGV[9]  burst_requests
-- ARGV[10] lease_sec
-- ARGV[11..] candidate triples: backend_id, max_backend_concurrency, cooldown_threshold
--
-- Returns:
--   {1, backend_id}                     ADMITTED
--   {2, reason, retry_after_sec}          REJECT_QUOTA
--   {3, reason}                         REJECT_CAPACITY

local quota_tokens_key    = KEYS[1]
local quota_rpm_key       = KEYS[2]
local quota_inflight_key  = KEYS[3]
local model_inflight_key  = KEYS[4]
local model_demand_key    = KEYS[5]
local deadlines_key       = KEYS[6]
local reservation_key     = KEYS[7]

local NUM_FIXED_KEYS = 7
local NUM_FIXED_ARGS = 10

local request_id           = ARGV[1]
local model_name           = ARGV[2]
local user_id              = ARGV[3]
local est_tokens           = tonumber(ARGV[4])
local max_user_concurrency = tonumber(ARGV[5])
local tokens_per_sec       = tonumber(ARGV[6])
local burst_tokens         = tonumber(ARGV[7])
local requests_per_sec     = tonumber(ARGV[8])
local burst_requests       = tonumber(ARGV[9])
local lease_sec              = tonumber(ARGV[10])

local time = redis.call('TIME')
local now = tonumber(time[1]) + tonumber(time[2]) / 1e6

local function gcra_eval(key, rate, burst, cost)
  if rate <= 0 then
    return true, nil, nil, nil
  end
  local tat_raw = redis.call('GET', key)
  local tat = tat_raw and tonumber(tat_raw) or now
  if tat < now then tat = now end
  local new_tat = tat + cost / rate
  local tau = burst / rate
  local allow_at = new_tat - tau
  if allow_at > now then
    return false, nil, allow_at - now, nil
  end
  return true, new_tat, nil, math.ceil((new_tat - now) + tau) + 1
end

-- ---- quota ----------------------------------------------------------------

local user_inflight = tonumber(redis.call('GET', quota_inflight_key) or '0')
if user_inflight >= max_user_concurrency then
  return {2, 'user_concurrency', -1}
end

local ok_r, tat_r, wait_r, ttl_r = gcra_eval(quota_rpm_key, requests_per_sec, burst_requests, 1)
if not ok_r then
  return {2, 'user_rpm', wait_r}
end

local ok_t, tat_t, wait_t, ttl_t = gcra_eval(quota_tokens_key, tokens_per_sec, burst_tokens, est_tokens)
if not ok_t then
  return {2, 'user_tpm', wait_t}
end

-- ---- capacity: walk candidates in router-chosen order ---------------------

local chosen_backend_id = nil
local num_benched = 0
local num_candidates = #KEYS - NUM_FIXED_KEYS

local argv_i = NUM_FIXED_ARGS + 1
for ci = 1, num_candidates do
  local error_key = KEYS[NUM_FIXED_KEYS + ci]
  local backend_id = ARGV[argv_i]
  local max_backend_concurrency = tonumber(ARGV[argv_i + 1])
  local cooldown_threshold = tonumber(ARGV[argv_i + 2])

  local errs = tonumber(redis.call('GET', error_key) or '0')

  if cooldown_threshold > 0 and errs >= cooldown_threshold then
    num_benched = num_benched + 1
  else
    local inflight = tonumber(redis.call('HGET', model_inflight_key, backend_id) or '0')
    if inflight < max_backend_concurrency then
      chosen_backend_id = backend_id
      break
    end
  end
  argv_i = argv_i + 3
end

-- ---- capacity reject ------------------------------------------------------

if not chosen_backend_id then
  redis.call('HINCRBY', model_demand_key, 'capacity_rejects_total', 1)
  redis.call('HSET', model_demand_key, 'last_reject_ts', tostring(now))

  local reason = 'saturated'
  if num_candidates == 0 then
    reason = 'no_candidates'
  elseif num_benched >= num_candidates then
    reason = 'all_benched'
  end

  return {3, reason}
end

-- ---- commit ---------------------------------------------------------------

if tat_r then redis.call('SET', quota_rpm_key, tostring(tat_r), 'EX', ttl_r) end
if tat_t then redis.call('SET', quota_tokens_key, tostring(tat_t), 'EX', ttl_t) end

redis.call('INCR', quota_inflight_key)
redis.call('HINCRBY', model_inflight_key, chosen_backend_id, 1)
redis.call('HINCRBY', model_demand_key, 'inflight', 1)

local row = cjson.encode({
  request_id     = request_id,
  model_name     = model_name,
  user_id        = user_id,
  backend_id     = chosen_backend_id,
  est_tokens     = est_tokens,
  admit_ts       = now,
  tokens_per_sec = tokens_per_sec,
  burst_tokens   = burst_tokens,
})
redis.call('SET', reservation_key, row)
redis.call('ZADD', deadlines_key, now + lease_sec, request_id)
return {1, chosen_backend_id}
