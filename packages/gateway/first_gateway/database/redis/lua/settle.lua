-- The single idempotent compensator.  Every cleanup path (request `finally`,
-- lease sweep, retries of either) converges here; absent row => no-op, so
-- concurrent callers race safely and exactly one applies.
-- Returns {1} if applied, {0} if the row was already settled.

local deadlines_key = KEYS[1]
local request_id = ARGV[1]
local actual_tokens = ARGV[2]

local RT_PREFIX = 'rt:'
local QUOTA_PREFIX = 'quota:'
local RESERVE_PREFIX = 'rt:reserve:'

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6

local rkey = RESERVE_PREFIX .. request_id
local raw = redis.call('GET', rkey)

if not raw then
  redis.call('ZREM', deadlines_key, request_id)
  return {0}
end

local row = cjson.decode(raw)

-- replica inflight (clamped)
local infkey = RT_PREFIX .. row.model .. ':inflight'
if redis.call('HINCRBY', infkey, row.replica_id, -1) < 0 then
  redis.call('HSET', infkey, row.replica_id, 0)
end

-- user inflight (delete-at-zero)
local uikey = QUOTA_PREFIX .. row.model .. ':' .. row.user .. ':inflight'
local u = redis.call('DECR', uikey)
if u <= 0 then redis.call('DEL', uikey) end

-- Token Rate Limit correction: (actual - reserved)
if row.token_rate > 0 and actual_tokens ~= '' then
  local delta = tonumber(actual_tokens) - row.est_tokens
  if delta ~= 0 then
    local qkey = QUOTA_PREFIX .. row.model .. ':' .. row.user .. ':tokens'
    local arrival_time = tonumber(redis.call('GET', qkey) or '0')
    if arrival_time > 0 then
      arrival_time = arrival_time + delta / row.token_rate
      if arrival_time <= now then
        redis.call('DEL', qkey)
      else
        local tau = row.token_burst / row.token_rate
        redis.call('SET', qkey, tostring(arrival_time),
                   'EX', math.ceil((arrival_time - now) + tau) + 1)
      end
    end
  end
end

-- demand gauge
local dkey = RT_PREFIX .. row.model .. ':demand'
if redis.call('HINCRBY', dkey, 'inflight', -1) < 0 then
  redis.call('HSET', dkey, 'inflight', 0)
end

redis.call('ZREM', deadlines_key, request_id)
redis.call('DEL', rkey)
return {1}