add_executable(gateway
    src/main.cc
)

target_link_libraries(gateway PRIVATE
    gRPC::grpc++            # gRPC server (SubmitOrder, CancelOrder, StreamOrderEvents)
    protobuf::libprotobuf   # serialize/deserialize gateway protos
    RdKafka::rdkafka++      # forward accepted orders to Kafka
    hiredis::hiredis        # rate limiting + risk check (Redis)
)
