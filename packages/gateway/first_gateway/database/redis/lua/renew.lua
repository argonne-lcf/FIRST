-- Batched lease renewal.  Conditional (never resurrects a settled row: EXISTS
-- guard + ZADD XX) and capped at admit_ts + max_stream_s so a stuck-but-alive
-- handler eventually lapses into the sweeper like any other death.
--
-- KEYS[1] reservation deadlines zset
-- ARGV[1] lease_s
-- ARGV[2] max_stream_s
-- ARGV[3] reserve prefix
-- ARGV[4..] request_ids
--
-- Returns the number of leases actually extended.

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6

local renewed = 0
for i = 4, #ARGV do
  local id = ARGV[i]
  local raw = redis.call('GET', ARGV[3] .. id)
  if raw then
    local row = cjson.decode(raw)
    local cap = (row.admit_ts or now) + tonumber(ARGV[2])
    local deadline = math.min(now + tonumber(ARGV[1]), cap)
    if deadline > now then
      redis.call('ZADD', KEYS[1], 'XX', deadline, id)
      renewed = renewed + 1
    end
  end
end
return renewed