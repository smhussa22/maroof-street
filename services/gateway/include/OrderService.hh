#ifndef ORDER_SERVICE_HH
#define ORDER_SERVICE_HH

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <chrono>
#include <fstream>
#include <optional>
#include <sstream>

#include <grpcpp/grpcpp.h>
#include "orders.grpc.pb.h"

#include "KafkaPublisher.hh"
#include "Redis.hh"
#include "Crypto.hh"

class GatewayOrderService : public maroof_street::orders::OrderService::Service
{
public:

    explicit GatewayOrderService(const std::vector<std::string>& symbols, std::string_view brokers, redisContext* redis, std::string_view rate_limit_script);
    ~GatewayOrderService() = default;

    GatewayOrderService(const GatewayOrderService&) = delete;
    GatewayOrderService& operator=(const GatewayOrderService&) = delete;

    GatewayOrderService(GatewayOrderService&&) = delete;
    GatewayOrderService& operator=(GatewayOrderService&&) = delete;

    // both wrap the request in a Command proto, look up the publisher for that symbol, and publish to commands.<SYMBOL>
    grpc::Status submit_order(grpc::ServerContext* context, const maroof_street::orders::SubmitOrder* request, maroof_street::orders::OrderAck* response) override;
    grpc::Status cancel_order(grpc::ServerContext* context, const maroof_street::orders::CancelOrder* request, maroof_street::orders::OrderAck* response) override;

private:

    // https://www.nyse.com/publicdocs/nyse/data/Pillar_Equities_Common_Client_Specification_v2.4k.pdf 3.6.3
    // stock exchanges usually keep 1 dedicated engine for each symbol instead of one thread doing it all.
    // symbol e.g. "AAPL" -> Kafka producer for the AAPL matching engine
    std::unordered_map<std::string, std::unique_ptr<KafkaPublisher<4096>>> m_publishers {};
    redisContext* m_redis { nullptr };
    std::string m_rate_limit_script {};
    
};

namespace
{

inline grpc::Status forward_command(const std::string& symbol, maroof_street::orders::Command& cmd, maroof_street::orders::OrderAck* response, std::unordered_map<std::string, std::unique_ptr<KafkaPublisher<4096>>>& publishers)
{

    if (!publishers.contains(symbol))
    {

        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "unknown symbol: " + symbol);
    
    }

    std::string bytes;
    cmd.SerializeToString(&bytes);
    publishers[symbol]->publish(bytes);

    response->set_accepted(true);
    return grpc::Status::OK;

}

inline bool check_rate_limit(redisContext* redis_ctx, const std::string& script, int account_id)
{
    const long long now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    const std::string key = "ratelimit:" + std::to_string(account_id);

    RedisReply reply {
        static_cast<redisReply*>(redisCommand(redis_ctx,
            "EVAL %s 1 %s %lld 10 100", script.c_str(), key.c_str(), now_ms)),
        freeReplyObject
    };

    return reply && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1;
}

// hashes the x-api-key header with SHA-256, looks it up in Redis, returns the account_id or nullopt
inline std::optional<int> lookup_account(redisContext* redis_ctx, grpc::ServerContext* grpc_ctx)
{
    auto it { grpc_ctx->client_metadata().find("x-api-key") };
    if (it == grpc_ctx->client_metadata().end())
        return std::nullopt;

    const std::string hash { sha256_hex({ it->second.data(), it->second.size() }) };

    RedisReply reply {
        static_cast<redisReply*>(redisCommand(redis_ctx, "GET apikey:%s", hash.c_str())),
        freeReplyObject
    };

    if (!reply || reply->type != REDIS_REPLY_STRING)
        return std::nullopt;

    return std::stoi(reply->str);
    
}

} // namespace


inline GatewayOrderService::GatewayOrderService(const std::vector<std::string>& symbols, std::string_view brokers, redisContext* redis, std::string_view rate_limit_script)
    : m_redis { redis }, m_rate_limit_script { rate_limit_script }
{
    for (const auto& symbol : symbols)
        m_publishers[symbol] = std::make_unique<KafkaPublisher<4096>>("commands." + symbol, brokers);
}

inline grpc::Status GatewayOrderService::submit_order(grpc::ServerContext* grpc_ctx, const maroof_street::orders::SubmitOrder* request, maroof_street::orders::OrderAck* response)
{
    const auto account_id = lookup_account(m_redis, grpc_ctx);
    if (!account_id)
        return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "invalid or missing api key");

    if (!check_rate_limit(m_redis, m_rate_limit_script, *account_id))
        return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "rate limit exceeded");

    maroof_street::orders::Command command;
    command.mutable_submit()->CopyFrom(*request);
    return forward_command(request->symbol(), command, response, m_publishers);
}

inline grpc::Status GatewayOrderService::cancel_order(grpc::ServerContext* grpc_ctx, const maroof_street::orders::CancelOrder* request, maroof_street::orders::OrderAck* response)
{
    const auto account_id = lookup_account(m_redis, grpc_ctx);
    if (!account_id)
        return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "invalid or missing api key");

    if (!check_rate_limit(m_redis, m_rate_limit_script, *account_id))
        return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "rate limit exceeded");

    maroof_street::orders::Command command;
    command.mutable_cancel()->CopyFrom(*request);
    return forward_command(request->symbol(), command, response, m_publishers);
}

#endif // ORDER_SERVICE_HH
