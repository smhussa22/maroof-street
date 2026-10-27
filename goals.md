# exchange-platform: Goals

A distributed stock exchange built around my C++ matching engine
(`smhussa22/matching-engine`) and my reliable-UDP library (`smhussa22/maroof-net`),
trading real symbols anchored to live market data.

**One-liner:** bots trade real symbols using live market data over gRPC, trades are
matched in microseconds, and every event is logged to Kafka, settled in Postgres, and
streamed live to a trading UI. The whole thing runs on Kubernetes.

**Stack:** C++23, protobuf, gRPC, Kafka, Redis, Postgres, Docker, Kubernetes (kind), React, Polygon.io (market data feed).

**Development environment:** WSL2 Ubuntu, GCC 14, CMake. Dependencies installed via
apt. Repos live in the WSL filesystem, not under `/mnt/c`.

---

## Design rules

1. **Kafka never sits on the matcher's hot path.** The engine pushes events to its
   outbound queue, and a separate publisher thread drains that queue into Kafka.
2. **The engine is deterministic.** Replaying the Kafka event log must rebuild the
   exact same order book.
3. **Every service consumes events at-least-once and must be idempotent.**
4. **One engine instance per symbol.** The engine is single-threaded, so scaling
   means sharding by symbol, with Kafka partitioned the same way.
5. **Libraries stay libraries.** The engine and maroof-net keep their own repos and
   are pulled in as dependencies (submodule or CMake `FetchContent`), never copied.

---

## Repo layout

```
exchange-platform/
  proto/              # shared .proto files (orders, executions, market data)
  services/
    gateway/          # gRPC order gateway
    risk/             # pre-trade risk checks
    engine-host/      # wraps the matching engine, publishes to Kafka
    ledger/           # settlement consumer -> Postgres
    market-data/      # book/trade streaming + Redis cache
    bots/             # simulated traders
  web/                # trading UI
  deploy/             # Dockerfiles, docker-compose, k8s manifests, Helm
  third_party/        # matching-engine, maroof-net
  docs/               # architecture diagram, benchmarks
  goals.md
```

---

## Stage 0: Prerequisites (in the engine repo)

**Goal:** make the engine usable by another project.

- [ ] Expose the engine as a CMake library target (`add_library(matching_engine ...)`)
      with exported headers, separate from the `matcher`, `server`, and `engine_bench`
      executables.
- [ ] Add a **sequence number** to every outbound event so consumers can detect gaps
      and replay correctly.
- [ ] Confirm whether the engine handles one symbol or many. If one, plan to run one
      instance per symbol (see Stage 5).
- [ ] (Optional, later) more order types: market orders, IOC, modify.

**Done when:** a separate test project can `FetchContent` the engine, link it, and
submit an order.

---

## Stage 1: Foundations

**Goal:** a working skeleton with one gRPC service, built and containerized.

- [ ] Create the `exchange-platform` repo with the layout above.
- [ ] Write the protobuf schemas:
  - `orders.proto`: `SubmitOrder`, `CancelOrder`, order/side/type enums
  - `events.proto`: ack, fill, cancel, reject (with sequence number and symbol)
  - `marketdata.proto`: top of book, depth, trade
- [ ] CMake setup — dependencies provided by apt (grpc, protobuf, librdkafka, hiredis, libpqxx).
- [ ] A minimal gRPC server with a health check endpoint.
- [ ] Multi-stage Dockerfile that produces a small runtime image.
- [ ] `docker-compose.yml` with Kafka, Redis, and Postgres for local development.

**Done when:** `docker-compose up` starts the infrastructure and the skeleton service
answers a gRPC call from a test client.

---

## Stage 2: Event pipeline (engine host + Kafka)

**Goal:** every engine output lands in Kafka, without slowing the matcher.

- [ ] `engine-host` service that wraps the engine and accepts commands.
- [ ] Publisher thread drains the engine's outbound queue into Kafka
      (librdkafka / cppkafka).
- [ ] Topics: `orders.in`, `executions`, `book.updates`, partitioned by symbol.
- [ ] Events serialized as protobuf, carrying sequence numbers.
- [ ] Re-run the engine benchmark with the publisher attached to confirm the
      hot path numbers (20 ns p50) are unchanged.

**Done when:** submitting orders produces a complete, correctly ordered stream of
events in Kafka, and engine latency is unaffected.

---

## Stage 3: Settlement (ledger)

**Goal:** trades change balances and positions correctly, exactly once in effect.

- [ ] Postgres schema: accounts, balances, positions, trades, processed-event IDs.
- [ ] `ledger` service consumes `executions` and applies each fill.
- [ ] Idempotency: dedupe on event ID or sequence number so redelivery never
      double-pays.
- [ ] Both sides of every trade are applied in one database transaction.

**Done when:** replaying the same Kafka messages twice leaves balances identical,
and total cash and shares across all accounts are conserved.

---

## Stage 4: Market data

**Goal:** live prices and book depth for anyone watching.

- [ ] `market-data` service consumes `book.updates` and `executions`.
- [ ] Maintains top of book, depth, last price, and recent trades per symbol.
- [ ] Caches the latest values in Redis for fast reads.
- [ ] gRPC server-streaming APIs: `SubscribeBook`, `SubscribeTrades`.
- [ ] Snapshot plus incremental updates, with sequence numbers so clients can detect
      gaps and resync.

**Done when:** a test client subscribes, receives a snapshot, then a gap-free stream
of updates that matches the engine's real book.

---

## Stage 5: Gateway and risk

**Goal:** a safe front door for orders.

- [ ] `gateway` service exposes `SubmitOrder` and `CancelOrder` over gRPC and
      forwards to the engine over maroof-net.
- [ ] API-key authentication (basic is fine for v1).
- [ ] Per-account rate limiting in Redis (token bucket via Lua script).
- [ ] `risk` service: pre-trade checks against Redis (available cash, position
      limits, max order size). Rejects before the order reaches the engine.
- [ ] Redis balances kept in sync with the ledger (reserve on order, release on
      cancel or fill).
- [ ] Multi-symbol routing: one engine-host instance per symbol, with the gateway
      routing by symbol.

**Done when:** an under-funded order is rejected without touching the engine, and a
flood of requests from one account gets rate-limited.

---

## Stage 6: Kubernetes

**Goal:** run the whole system on a local cluster.

- [ ] Deployments and Services for every component.
- [ ] ConfigMaps and Secrets for configuration and credentials.
- [ ] gRPC health-check readiness and liveness probes.
- [ ] Kafka, Redis, and Postgres via Helm charts or operators (StatefulSets).
- [ ] One engine pod per symbol.
- [ ] Autoscaling for `market-data` and `ledger` consumers on Kafka lag (KEDA or HPA).
- [ ] Runs on `kind`. No cloud account is required.

**Done when:** a single command brings up the full system on `kind`, and all pods
report ready.

---

## Stage 7: Crash recovery

**Goal:** prove the event-sourcing design works.

- [ ] On startup, the engine host rebuilds its order book by replaying the Kafka log
      (from a snapshot plus the tail, if replay time becomes a problem).
- [ ] Verify the rebuilt book is identical to the pre-crash book.
- [ ] Kill the engine pod mid-trading and show that:
  - Kubernetes restarts it
  - the book is restored
  - no trades are lost or duplicated
  - the ledger stays consistent
- [ ] Decide and document what happens to in-flight orders when the engine dies.

**Done when:** the kill-the-pod demo works repeatably with balances still conserved.

---

## Stage 8: Frontend and bots

**Goal:** something you can show off.

- [ ] Web UI (React + TypeScript): live order book depth, trade tape, price chart,
      order entry form, and the user's positions and balance.
- [ ] gRPC-Web (via Envoy) or a thin REST/WebSocket layer for the browser.
- [ ] `bots` service: market-maker bots and random traders, anchored to live prices
      from Polygon.io for a fixed set of symbols (AAPL, TSLA, NVDA, MSFT, GOOGL),
      so the exchange always has realistic activity.

**Done when:** opening the UI shows a live, moving market that you can also trade in.

---

## Stage 9: Observability and benchmarks

**Goal:** measured numbers for the README.

- [ ] Prometheus metrics: orders/sec, gateway latency, Kafka consumer lag, rejects.
- [ ] Grafana dashboards.
- [ ] OpenTelemetry tracing from gateway through engine to ledger.
- [ ] Load test with the bots at high rates, recording end-to-end latency
      (p50, p99, p99.9) and platform throughput.
- [ ] Compare the gRPC path with the direct maroof-net path.
- [ ] Architecture diagram and results table in the README.

**Done when:** the README includes real numbers and an architecture diagram.

---

## Definition of done (whole project)

A two-minute demo: bots trading on the live UI, then the engine pod is killed,
Kubernetes restarts it, the book rebuilds from Kafka, and trading continues with no
lost or duplicated trades.

## Out of scope (v1)

Real money, full authentication, margin or derivatives, and multi-region deployment.
Market data is real (Polygon.io) but execution is simulated.

## Stretch goals

- Circuit breakers and retries with deadlines on gRPC calls
- Chaos testing beyond a single pod kill (Kafka broker loss, Redis loss)
- Rebuilding Redis state from Kafka after a Redis failure
- Additional order types (market, IOC, modify)
- A short cloud deployment (EKS or k3s) with Terraform, torn down afterward
- Surveillance or analytics consumer (wash-trade detection, OHLCV candles)

## System design topics this project covers

- Kafka partitioning by symbol and its tradeoffs
- At-least-once delivery with idempotent consumers
- Event sourcing and deterministic replay
- Scaling a single-threaded core by sharding
- Pre-trade risk without slowing matching
- Market data fan-out with snapshots, increments, and gap detection
- Backpressure when consumers fall behind
- Failure behavior of each component