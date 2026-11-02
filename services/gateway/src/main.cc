#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <iostream>
#include <vector>
#include <string>

#include "OrderService.hh"

int main()
{
    const std::vector<std::string> symbols { "AAPL", "TSLA", "NVDA", "MSFT", "GOOGL" };
    GatewayOrderService order_service { symbols, "localhost:9092" };

    grpc::EnableDefaultHealthCheckService(true);

    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());
    builder.RegisterService(&order_service);

    auto server = builder.BuildAndStart();
    std::cout << "gateway listening on 0.0.0.0:50051\n";
    server->Wait();
}
