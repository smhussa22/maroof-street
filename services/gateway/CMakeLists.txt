add_executable(gateway src/main.cc)

target_link_libraries(gateway PRIVATE
    exchange_proto          # generated protobuf + gRPC stubs
    gRPC::grpc++            # gRPC server runtime
    PkgConfig::RdKafkaCpp   # forward accepted orders to Kafka
    PkgConfig::Hiredis      # rate limiting + risk check (Redis)
)
