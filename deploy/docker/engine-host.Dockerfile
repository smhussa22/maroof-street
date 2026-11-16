# one image for every symbol; the SYMBOL environment variable picks the stock
# build from the repository root:  docker build -f deploy/docker/engine-host.Dockerfile -t maroof-street/engine-host .
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git ca-certificates pkg-config \
    libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc libssl-dev \
    librdkafka-dev libhiredis-dev libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build --target engine-host -j$(nproc)

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    libprotobuf32t64 librdkafka++1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/services/engine-host/engine-host /usr/local/bin/engine-host

ENTRYPOINT ["/usr/local/bin/engine-host"]
