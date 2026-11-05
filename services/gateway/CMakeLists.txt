add_executable(gateway src/main.cc)

target_include_directories(gateway PRIVATE
    include
    ../shared/include
)

target_link_libraries(gateway PRIVATE
    exchange_proto          # generated protobuf + gRPC stubs
    gRPC::grpc++            # gRPC server runtime
    PkgConfig::RdKafkaCpp   # forward accepted orders to Kafka
    PkgConfig::Hiredis      # rate limiting + risk check (Redis)
    PkgConfig::LibPQ        # Postgres: seed api key -> account_id into Redis at startup
    OpenSSL::Crypto         # SHA-256 for API key hashing
)
