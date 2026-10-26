add_executable(engine-host
    src/main.cc
)

target_link_libraries(engine-host PRIVATE
    lob_engine              # matching engine + maroof_net (transitive)
    RdKafka::rdkafka++      # Kafka producer (publish events) + consumer (receive commands)
    protobuf::libprotobuf   # serialize/deserialize ExchangeEvent protos
)
