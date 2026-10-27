#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <iostream>

int main()
{
    grpc::EnableDefaultHealthCheckService(true);

    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());

    auto server = builder.BuildAndStart();
    std::cout << "gateway listening on 0.0.0.0:50051\n";
    server->Wait();
}
