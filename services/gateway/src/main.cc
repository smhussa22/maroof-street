#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <print>

int main()
{

    grpc::EnableDefaultHealthCheckService(true);

    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());

    auto server = builder.BuildAndStart();
    std::println("gateway listening on 0.0.0.0:50051");
    server->Wait();

}
