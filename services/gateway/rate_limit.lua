-- token bucket rate limiter
-- KEYS[1] = ratelimit:{account_id}
-- ARGV[1] = now_ms, ARGV[2] = refill_rate (tokens/sec), ARGV[3] = max_tokens

local key        = KEYS[1]
local now        = tonumber(ARGV[1])
local refill     = tonumber(ARGV[2])
local max_tokens = tonumber(ARGV[3])

local data        = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens      = tonumber(data[1]) or max_tokens
local last_refill = tonumber(data[2]) or now

local elapsed = (now - last_refill) / 1000.0
tokens = math.min(max_tokens, tokens + elapsed * refill)

if tokens < 1 then
    return 0
end

tokens = tokens - 1
redis.call('HSET', key, 'tokens', tokens, 'last_refill', now)
redis.call('PEXPIRE', key, 60000)
return 1
