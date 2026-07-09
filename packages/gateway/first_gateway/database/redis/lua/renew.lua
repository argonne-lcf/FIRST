-- Batched lease renewal.  Conditional: never resurrects a settled row (EXISTS
-- guard + ZADD XX) and capped at admit_ts + max_request_sec so a stuck-but-alive
-- handler eventually lapses into the sweeper.
--
-- KEYS[1]    rt:deadlines                   ZSET request_id → deadline_ts
-- KEYS[2..N] rt:reserve:{id}               reservation blobs (one per request)
--
-- ARGV[1]    lease_sec
-- ARGV[2]    max_request_sec
-- ARGV[3..N] request_ids                    parallel to KEYS[2..N]
--
-- Returns the number of leases actually extended.

local deadlines_key = KEYS[1]
local lease_sec       = tonumber(ARGV[1])
local max_request_sec  = tonumber(ARGV[2])

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6

local renewed = 0
local num_requests = #KEYS - 1

for i = 1, num_requests do
  local rkey = KEYS[1 + i]
  local request_id = ARGV[2 + i]
  local raw = redis.call('GET', rkey)
  if raw then
    local row = cjson.decode(raw)
    local cap = (row.admit_ts or now) + max_request_sec
    local deadline = math.min(now + lease_sec, cap)
    if deadline > now then
      redis.call('ZADD', deadlines_key, 'XX', deadline, request_id)
      renewed = renewed + 1
    end
  end
end
return renewed
