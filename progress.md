# progress

## Goals
- public demo: a hosted page where anyone can submit a trade, see it accepted or rejected, and view an interactive diagram showing how the order matched (aggressor vs passive, price/qty, book state before and after)



## Stage 0: Prerequisites
- [x] expose `lob_engine` as a CMake INTERFACE library target
- [x] gate executables and tests behind `PROJECT_IS_TOP_LEVEL` so FetchContent doesn't build them
- [x] add sequence number to every outbound event in `Protocol.hh` and `ExchangeListener`
- [x] confirmed single symbol per engine instance — no code change needed

## Stage 1: Foundations
- [x] repo layout (`services/`, `proto/`, `deploy/`, `web/`, `docs/`)
- [x] `orders.proto` — SubmitOrder, CancelOrder, OrderDirection, OrderType enums
- [x] `events.proto` — ExchangeEvent wrapper, oneof payload, all five event types, RejectedReason
- [x] `marketdata.proto` — MarketData service, streaming RPCs, TopOfBook, DepthUpdate, Trade
- [x] CMake proto codegen — `exchange_proto` library built from all `.proto` files
- [x] minimal gRPC gateway with health check endpoint
- [x] multi-stage Dockerfile for gateway
- [x] `docker-compose.yml` with Kafka, Redis, Postgres
- [x] verify: `docker-compose up` starts infra and gateway answers a health check

## Stage 2: Event pipeline (engine-host + Kafka)
- [x] `engine-host` reads commands from Kafka topic `commands.<SYMBOL>`
- [x] `KafkaListener` — maps all 5 engine callbacks to `ExchangeEvent` protos, serializes, hands to publisher
- [x] `KafkaPublisher` — owns rdkafka producer, writes serialized bytes to `events.<SYMBOL>` topic
- [x] verified end-to-end: test command in → engine processes → ExchangeEvents out on `events.AAPL`
- [x] SpscQueue — lock-free SPSC ring buffer, power-of-two capacity, cache-line aligned head/tail
- [x] KafkaPublisher — publish() enqueues bytes onto SpscQueue; background drain thread calls rdkafka produce
- [x] re-run engine benchmark with publisher attached — p99.9 under 1µs confirmed

## Stage 3: Settlement (ledger)
- [x] Postgres schema — accounts, balances, positions, trades
- [x] Python ledger consumes `events.<SYMBOL>`, applies fills to Postgres
- [x] idempotent on seq — ON CONFLICT DO NOTHING on (seq, symbol) prevents double-settling
- [x] verified end-to-end: grpcurl buy+sell → engine → events.AAPL → ledger → trades row in Postgres
- [x] balance/position settlement — order_meta in Redis maps order_id → account_id; wired up in Stage 5

## Stage 4: Market data
- [x] Python market-data service consumes `events.<SYMBOL>`
- [x] maintains top of book, depth, last price, recent trades per symbol in Redis
- [x] implements `MarketData` gRPC service — StreamTopOfBook, StreamDepth, StreamTrades
- [x] first message on each stream is a full snapshot, subsequent are deltas

## Stage 5: Gateway and risk
- [x] gateway implements `SubmitOrder` and `CancelOrder` gRPC endpoints
- [x] gateway routes by symbol to the correct `commands.<SYMBOL>` Kafka topic
- [x] verified end-to-end: grpcurl → gateway → commands.AAPL topic confirmed
- [x] API key → account mapping: SHA-256 hashed keys stored in Postgres, seeded into Redis at startup, looked up per request
- [x] verified: valid key accepted, missing key returns UNAUTHENTICATED
- [x] per-account rate limiting in Redis (token bucket Lua script, 100 token max, 10/sec refill)
- [x] balance check in Redis before forwarding to Kafka — BUY reserves cash, SELL reserves shares via atomic Lua scripts
- [x] balances and positions seeded from Postgres into Redis at gateway startup
- [x] Redis balances kept in sync by ledger — fills consume reservations and credit the other side, cancels release remaining reservation

## Stage 6: Kubernetes (EKS)
- [x] Deployments and Services for every component
- [x] ConfigMaps and Secrets for config and credentials
- [x] gRPC health check as readiness and liveness probes
- [x] Kafka via Strimzi operator (3 brokers, RF=3), Redis and Postgres via Bitnami Helm charts
- [x] one engine-host Deployment per symbol (SYMBOL env var), c6i node affinity
- [x] gateway HPA — scales 2→10 on CPU + gRPC connection count
- [x] AWS NLB exposes gateway; EBS gp3 for all persistent volumes
- [x] Cluster Autoscaler for node scaling across 3 AZs

## Stage 7: Crash recovery
- [x] engine-host replays `events.<SYMBOL>` from last committed offset on startup to rebuild book
- [x] kill-the-pod demo: Kubernetes restarts engine, book rebuilds, trading resumes, no lost trades
- [x] ledger balances conserved across crash and recovery

## Stage 8: Frontend and bots
- [x] React UI — live order book, trade tape, price chart, order entry, positions and balance
- [x] bots — market maker and random trader anchored to live Polygon.io prices
- [x] symbols: AAPL, TSLA, NVDA, MSFT, GOOGL

## Stage 9: Observability and benchmarks
- [x] Prometheus metrics — orders/sec, latency, Kafka consumer lag, rejects
- [x] Grafana dashboards
- [x] load test — p99.9 end-to-end latency under 1µs on engine hot path
- [x] architecture diagram and results table in README
