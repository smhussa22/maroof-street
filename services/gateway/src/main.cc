#include <libpq-fe.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <iostream>
#include <vector>
#include <string>

#include "OrderService.hh"
#include "Redis.hh"

static constexpr const char* KAFKA_BROKERS  = "localhost:9092";
static constexpr const char* GRPC_LISTEN    = "0.0.0.0:50051";
static constexpr const char* POSTGRES_CONN  = "host=localhost dbname=exchange user=exchange password=exchange";
static constexpr const char* REDIS_HOST     = "127.0.0.1";
static constexpr int         REDIS_PORT     = 6379;

int main()
{
    // connect to Redis and verify
    RedisContext redis { redisConnect(REDIS_HOST, REDIS_PORT), redisFree };
    if (!redis || redis->err)
    {
        std::cerr << "redis connect failed: " << (redis ? redis->errstr : "null context") << "\n";
        return 1;
    }

    // load api key -> account_id mappings from Postgres into Redis at startup
    PGconn* pg = PQconnectdb(POSTGRES_CONN);
    if (PQstatus(pg) != CONNECTION_OK)
    {
        std::cerr << "postgres connect failed: " << PQerrorMessage(pg) << "\n";
        PQfinish(pg);
        return 1;
    }

    PGresult* res = PQexec(pg, "SELECT key_hash, account_id FROM api_keys");
    if (PQresultStatus(res) == PGRES_TUPLES_OK)
    {
        for (int i = 0; i < PQntuples(res); ++i)
        {
            const char* key_hash   = PQgetvalue(res, i, 0);
            const char* account_id = PQgetvalue(res, i, 1);
            RedisReply reply {
                static_cast<redisReply*>(redisCommand(redis.get(), "SET apikey:%s %s", key_hash, account_id)),
                freeReplyObject
            };
        }
    }
    PQclear(res);
    PQfinish(pg);

    const std::vector<std::string> symbols { "AAPL", "TSLA", "NVDA", "MSFT", "GOOGL" };
    GatewayOrderService order_service { symbols, KAFKA_BROKERS, redis.get() };

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(GRPC_LISTEN, grpc::InsecureServerCredentials());
    builder.RegisterService(&order_service);

    auto server = builder.BuildAndStart();
    std::cout << "gateway listening on " << GRPC_LISTEN << "\n";
    server->Wait();
}
