-- atomically reserve shares for a sell order
-- KEYS[1] = balance:{account_id}:position:{symbol}
-- KEYS[2] = reserved:{account_id}:position:{symbol}
-- ARGV[1] = quantity
local available = tonumber(redis.call('GET', KEYS[1])) or 0
local qty       = tonumber(ARGV[1])

if available < qty then
    return 0
end

redis.call('INCRBYFLOAT', KEYS[1], -qty)
redis.call('INCRBYFLOAT', KEYS[2],  qty)
return 1
