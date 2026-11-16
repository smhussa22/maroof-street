# build from the repository root:  docker build -f deploy/docker/bot-rust.Dockerfile -t maroof-street/rust-bot .
FROM rust:1.82 AS builder
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
# build.rs reads ../../../proto, so keep the repository layout
WORKDIR /src/services/bots/rust
COPY proto /src/proto
COPY services/bots/rust .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /src/services/bots/rust/target/release/rust-bot /usr/local/bin/rust-bot
ENTRYPOINT ["/usr/local/bin/rust-bot"]
