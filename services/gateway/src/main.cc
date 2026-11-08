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

    res = PQexec(pg, "SELECT account_id, cash FROM balances");
    if (PQresultStatus(res) == PGRES_TUPLES_OK)
    {
        for (int i = 0; i < PQntuples(res); ++i)
        {
            const char* account_id = PQgetvalue(res, i, 0);
            const char* amount     = PQgetvalue(res, i, 1);
            RedisReply reply {
                static_cast<redisReply*>(redisCommand(redis.get(), "SET balance:%s:cash %s", account_id, amount)),
                freeReplyObject
            };
        }
    }
    PQclear(res);

    res = PQexec(pg, "SELECT account_id, symbol, quantity FROM positions");
    if (PQresultStatus(res) == PGRES_TUPLES_OK)
    {
        for (int i = 0; i < PQntuples(res); ++i)
        {
            const char* account_id = PQgetvalue(res, i, 0);
            const char* symbol     = PQgetvalue(res, i, 1);
            const char* quantity   = PQgetvalue(res, i, 2);
            RedisReply reply {
                static_cast<redisReply*>(redisCommand(redis.get(), "SET balance:%s:position:%s %s", account_id, symbol, quantity)),
                freeReplyObject
            };
        }
    }
    PQclear(res);

    PQfinish(pg);

    auto load_lua = [](const char* path) -> std::string {
        std::ifstream f { path };
        if (!f) { std::cerr << "failed to open " << path << "\n"; std::exit(1); }
        return { std::istreambuf_iterator<char>(f), {} };
    };

    const std::string rate_limit_script    = load_lua("services/gateway/rate_limit.lua");
    const std::string reserve_cash_script  = load_lua("services/gateway/reserve_cash.lua");
    const std::string reserve_shares_script = load_lua("services/gateway/reserve_shares.lua");

    const std::vector<std::string> symbols { "AAPL", "TSLA", "NVDA", "MSFT", "GOOGL" };
    GatewayOrderService order_service { symbols, KAFKA_BROKERS, redis.get(), rate_limit_script, reserve_cash_script, reserve_shares_script };

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(GRPC_LISTEN, grpc::InsecureServerCredentials());
    builder.RegisterService(&order_service);

    auto server = builder.BuildAndStart();
    std::cout << "gateway listening on " << GRPC_LISTEN << "\n";
    server->Wait();
}
