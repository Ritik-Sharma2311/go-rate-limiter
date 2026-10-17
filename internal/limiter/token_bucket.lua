-- Token bucket evaluated atomically inside Redis.
--
-- KEYS[1]  bucket key
-- ARGV[1]  capacity (burst size)
-- ARGV[2]  refill rate in tokens per second
-- ARGV[3]  tokens requested by this call
-- ARGV[4]  key TTL in milliseconds
--
-- Returns {allowed (1/0), whole tokens remaining, retry-after in ms}.
--
-- Time comes from the Redis server (TIME), not from the caller, so gateway
-- replicas with drifting clocks still agree on how many tokens have refilled.

local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local cost     = tonumber(ARGV[3])
local ttl_ms   = tonumber(ARGV[4])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2]) -- microseconds

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * rate / 1000000)

local allowed  = 0
local retry_ms = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_ms = math.ceil((cost - tokens) / rate * 1000)
end

-- string.format keeps the microsecond timestamp exact; tostring would round
-- numbers this large to 14 significant digits.
redis.call('HSET', key, 'tokens', string.format('%.6f', tokens), 'ts', string.format('%.0f', now))
redis.call('PEXPIRE', key, ttl_ms)

return {allowed, math.floor(tokens), retry_ms}
