add_executable(engine-host
    src/main.cc
)

target_include_directories(engine-host PRIVATE include)

target_link_libraries(engine-host PRIVATE
    lob_engine              # matching engine + maroof_net (transitive)
    exchange_proto          # generated protobuf types (ExchangeEvent, SubmitOrder, etc.)
    PkgConfig::RdKafkaCpp   # Kafka producer (publish events) + consumer (receive commands)
)
