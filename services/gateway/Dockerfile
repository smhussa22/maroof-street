# build from the repository root:  docker build -f services/gateway/Dockerfile -t maroof-street/gateway .
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git ca-certificates pkg-config \
    libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc libssl-dev \
    librdkafka-dev libhiredis-dev libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build --target gateway -j$(nproc)

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++1.51t64 libprotobuf32t64 libssl3t64 librdkafka++1 libhiredis1.1.0 libpq5 \
    && rm -rf /var/lib/apt/lists/*

# the gateway loads its Lua scripts by relative path, so it runs from /app
WORKDIR /app
COPY --from=builder /src/build/services/gateway/gateway /usr/local/bin/gateway
COPY services/gateway/*.lua services/gateway/

EXPOSE 50051
ENTRYPOINT ["/usr/local/bin/gateway"]
