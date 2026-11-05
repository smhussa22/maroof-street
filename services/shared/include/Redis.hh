#ifndef REDIS_HH
#define REDIS_HH

#include <hiredis/hiredis.h>
#include <memory>

using RedisContext = std::unique_ptr<redisContext, decltype(&redisFree)>;
using RedisReply   = std::unique_ptr<redisReply,   decltype(&freeReplyObject)>;

inline RedisContext redis_connect(const char* host, int port)
{
    return RedisContext { redisConnect(host, port), redisFree };
}

#endif // REDIS_HH
