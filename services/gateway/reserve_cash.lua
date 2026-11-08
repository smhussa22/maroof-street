-- atomically reserve cash for a buy order
-- KEYS[1] = balance:{account_id}:cash
-- KEYS[2] = reserved:{account_id}:cash
-- ARGV[1] = cost (price * quantity)
local available = tonumber(redis.call('GET', KEYS[1])) or 0
local cost      = tonumber(ARGV[1])

if available < cost then
    return 0
end

redis.call('INCRBYFLOAT', KEYS[1], -cost)
redis.call('INCRBYFLOAT', KEYS[2],  cost)
return 1
