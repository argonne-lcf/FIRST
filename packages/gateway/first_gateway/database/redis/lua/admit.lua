-- Admission: perform quota checks, then walk the ordered candidate list.
-- Commit atomically for the first replica with capacity.
--
-- Returns:
--   {1, replica_id, 0}                   ADMITTED
--   {2, reason, retry_after_s}           REJECT_QUOTA
--   {3, reason, -1}                      REJECT_CAPACITY

local quota_tokens_key    = KEYS[1]
local quota_rpm_key       = KEYS[2]
local quota_inflight_key  = KEYS[3]
local model_inflight_key  = KEYS[4]
local model_demand_key    = KEYS[5]
local deadlines_key       = KEYS[6]

local request_id           = ARGV[1]
local model                = ARGV[2]
local user                 = ARGV[3]
local est_tokens           = tonumber(ARGV[4])
local max_user_concurrency = tonumber(ARGV[5])
local token_rate           = tonumber(ARGV[6])
local token_burst          = tonumber(ARGV[7])
local request_rate         = tonumber(ARGV[8])
local request_burst        = tonumber(ARGV[9])
local lease_s              = tonumber(ARGV[10])
-- ARGV[11..] candidate triples: replica_id, per_replica_concurrency, cooldown_threshold

local time = redis.call('TIME')
local now = tonumber(time[1]) + tonumber(time[2]) / 1e6
local reserve_key = 'rt:reserve:' .. request_id

local function gcra_eval(key, rate, burst, cost)
  -- GCRA Rate Limiting Helper
  -- Returns: (ok, new_tat, wait_s, ttl_s)

  if rate <= 0 then
    -- rate limiting disabled
    return true, nil, nil, nil
  end

  -- Get stored arrival time (future or now)
  local tat = tonumber(redis.call('GET', key) or tostring(now))
  if tat < now then
    tat = now
  end

  -- Current request advances arrival time by cost/rate
  local new_tat = tat + cost / rate

  -- Request allowed within burst window (tau) of new_tat:
  local tau = burst / rate
  local allow_at = new_tat - tau

  if allow_at > now then
    -- Too soon: ask client to wait until allowed
    return false, nil, allow_at - now, nil
  end

  -- OK: return new arrival time and max TTL
  return true, new_tat, nil, math.ceil((new_tat - now) + tau) + 1
end

-- Reject if user is over concurrency limit
local user_inflight = tonumber(redis.call('GET', quota_inflight_key) or '0')
if user_inflight >= max_user_concurrency then
  return {2, 'user_concurrency', -1}
end

-- Check User*Model Request Rate Limit
local ok_r, tat_r, wait_r, ttl_r = gcra_eval(quota_rpm_key, request_rate, request_burst, 1)
if not ok_r then
  return {2, 'user_rpm', wait_r}
end

-- Check User*Model Token Rate Limit
local ok_t, tat_t, wait_t, ttl_t = gcra_eval(quota_tokens_key, token_rate, token_burst, est_tokens)
if not ok_t then
  return {2, 'user_tpm', wait_t}
end

-- Walk candidate replicas in order, choose the first with headroom
local chosen_replica_id = nil
local num_benched = 0
local num_candidates = 0

local i = 11
while i + 2 <= #ARGV do
  num_candidates = num_candidates + 1

  local replica_id = ARGV[i]
  local replica_concurrency = tonumber(ARGV[i + 1])
  local replica_cooldown_threshold = tonumber(ARGV[i + 2])

  local errs = tonumber(redis.call('GET', 'rt:replica:' .. replica_id .. ':errors') or '0')

  if replica_cooldown_threshold > 0 and errs >= replica_cooldown_threshold then
    num_benched = num_benched + 1
  else
    local inflight = tonumber(redis.call('HGET', model_inflight_key, replica_id) or '0')
    if inflight < replica_concurrency then
      chosen_replica_id = replica_id
      break
    end
  end
  i = i + 3
end

-- Capacity Reject: update demand
if not chosen_replica_id then
  redis.call('HINCRBY', model_demand_key, 'capacity_rejects_total', 1)
  redis.call('HSET', model_demand_key, 'last_reject_ts', tostring(now))

  local reason = 'saturated'
  if num_candidates == 0 then
    reason = 'no_candidates'
  elseif num_benched >= num_candidates then
    reason = 'all_benched'
  end

  return {3, reason, -1}
end

-- Commit
if tat_r then redis.call('SET', quota_rpm_key, tostring(tat_r), 'EX', ttl_r) end
if tat_t then redis.call('SET', quota_tokens_key, tostring(tat_t), 'EX', ttl_t) end

redis.call('INCR', quota_inflight_key)
redis.call('HINCRBY', model_inflight_key, chosen_replica_id, 1)
redis.call('HINCRBY', model_demand_key, 'inflight', 1)

local row = cjson.encode({
  request_id = request_id,
  model = model,
  user = user,
  replica_id = chosen_replica_id,
  est_tokens = est_tokens,
  admit_ts = now,
  token_rate = token_rate,
  token_burst = token_burst,
})
redis.call('SET', reserve_key, row)
redis.call('ZADD', deadlines_key, now + lease_s, request_id)
return {1, chosen_replica_id, 0}