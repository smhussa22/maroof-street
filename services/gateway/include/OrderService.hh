#ifndef ORDER_SERVICE_HH
#define ORDER_SERVICE_HH

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "orders.grpc.pb.h"
#include "KafkaPublisher.hh"

class GatewayOrderService : public maroof_street::orders::OrderService::Service
{
public:

    explicit GatewayOrderService(const std::vector<std::string>& symbols, std::string_view brokers);
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

} // namespace


inline GatewayOrderService::GatewayOrderService(const std::vector<std::string>& symbols, std::string_view brokers)
{
    for (const auto& symbol : symbols)
        m_publishers[symbol] = std::make_unique<KafkaPublisher<4096>>("commands." + symbol, brokers);
}

inline grpc::Status GatewayOrderService::submit_order(grpc::ServerContext*, const maroof_street::orders::SubmitOrder* request, maroof_street::orders::OrderAck* response)
{
    maroof_street::orders::Command command;
    command.mutable_submit()->CopyFrom(*request);
    return forward_command(request->symbol(), command, response, m_publishers);
}

inline grpc::Status GatewayOrderService::cancel_order(grpc::ServerContext*, const maroof_street::orders::CancelOrder* request, maroof_street::orders::OrderAck* response)
{
    maroof_street::orders::Command command;
    command.mutable_cancel()->CopyFrom(*request);
    return forward_command(request->symbol(), command, response, m_publishers);
}

#endif // ORDER_SERVICE_HH
