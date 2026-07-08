-- Error counter with cooldown collapsed in: count >= threshold IS the bench.
-- First error arms the window TTL; crossing the threshold re-arms the TTL to
-- the bench duration.  Absence == healthy.  Incarnation-unique replica IDs
-- guarantee a counter can never haunt a relaunched replica.
--
-- KEYS[1] rt:replica:{id}:errors
-- ARGV[1] window_s   ARGV[2] threshold   ARGV[3] bench_s
--
-- Returns {count, benched(0|1)}

local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1]))
end
if n == tonumber(ARGV[2]) then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
end
local benched = 0
if n >= tonumber(ARGV[2]) then benched = 1 end
return {n, benched}