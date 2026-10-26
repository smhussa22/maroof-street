# --- build stage ---
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y \
    build-essential cmake git \
    libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc \
    librdkafka-dev libhiredis-dev libpqxx-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build --target gateway -j$(nproc)

# --- runtime stage ---
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    libgrpc++-dev libprotobuf-dev \
    librdkafka-dev libhiredis-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/services/gateway/gateway /usr/local/bin/gateway

EXPOSE 50051

ENTRYPOINT ["/usr/local/bin/gateway"]
