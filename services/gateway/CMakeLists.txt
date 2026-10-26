add_executable(gateway src/main.cc)

target_link_libraries(gateway PRIVATE
    exchange_proto          # generated protobuf + gRPC stubs
    gRPC::grpc++            # gRPC server runtime
    RdKafka::rdkafka++      # forward accepted orders to Kafka
    hiredis::hiredis        # rate limiting + risk check (Redis)
)
