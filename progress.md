# progress

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
- [ ] verify: `docker-compose up` starts infra and gateway answers a health check

## Stage 2: Event pipeline (engine-host + Kafka)
- [ ] `engine-host` reads commands from Kafka topic `commands.<SYMBOL>`
- [x] `KafkaListener` — maps all 5 engine callbacks to `ExchangeEvent` protos, serializes, hands to publisher
- [x] `KafkaPublisher` — owns rdkafka producer, writes serialized bytes to `events.<SYMBOL>` topic
- [ ] publisher thread drains outbound queue into Kafka without touching the matching thread
- [ ] re-run engine benchmark with publisher attached — confirm hot path latency unchanged

## Stage 3: Settlement (ledger)
- [ ] Postgres schema — accounts, balances, positions, trades
- [ ] Python ledger consumes `events.<SYMBOL>`, applies fills to Postgres
- [ ] idempotent on seq — replaying the same message twice leaves balances unchanged
- [ ] both sides of a trade settle in one transaction

## Stage 4: Market data
- [ ] Python market-data service consumes `events.<SYMBOL>`
- [ ] maintains top of book, depth, last price, recent trades per symbol in Redis
- [ ] implements `MarketData` gRPC service — StreamTopOfBook, StreamDepth, StreamTrades
- [ ] first message on each stream is a full snapshot, subsequent are deltas

## Stage 5: Gateway and risk
- [ ] gateway implements `SubmitOrder` and `CancelOrder` gRPC endpoints
- [ ] API key maps to an account
- [ ] per-account rate limiting in Redis (token bucket)
- [ ] balance check in Redis before forwarding to Kafka
- [ ] Redis balances kept in sync by ledger (reserve on submit, release on fill or cancel)
- [ ] gateway routes by symbol to the correct `commands.<SYMBOL>` Kafka topic

## Stage 6: Kubernetes
- [ ] Deployments and Services for every component
- [ ] ConfigMaps and Secrets for config and credentials
- [ ] gRPC health check as readiness and liveness probes
- [ ] Kafka, Redis, Postgres via Helm charts
- [ ] one engine-host pod per symbol
- [ ] runs on `kind` — no cloud account required

## Stage 7: Crash recovery
- [ ] engine-host replays `events.<SYMBOL>` from last committed offset on startup to rebuild book
- [ ] kill-the-pod demo: Kubernetes restarts engine, book rebuilds, trading resumes, no lost trades
- [ ] ledger balances conserved across crash and recovery

## Stage 8: Frontend and bots
- [ ] React UI — live order book, trade tape, price chart, order entry, positions and balance
- [ ] bots — market maker and random trader anchored to live Polygon.io prices
- [ ] symbols: AAPL, TSLA, NVDA, MSFT, GOOGL

## Stage 9: Observability and benchmarks
- [ ] Prometheus metrics — orders/sec, latency, Kafka consumer lag, rejects
- [ ] Grafana dashboards
- [ ] load test — p50, p99, p99.9 end-to-end latency and platform throughput
- [ ] architecture diagram and results table in README
