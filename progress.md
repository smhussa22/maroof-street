# progress

## Goals
- public demo: a hosted page where anyone can submit a trade, see it accepted or rejected, and follow it through the system
  - [x] page built on the personal site: live order book and depth chart, trade feed with trader names, order entry, a live order journey, and a market-closed state
  - [x] each visitor gets their own wallet (anonymous id in a cookie), and every demo order completes: a house account takes any unfilled remainder at the visitor's price
  - [x] simulated market: when no exchange is running, the page runs a market generated from the clock (random weeks of prices, book depth and bot trades) and matches visitor orders in it, labelled "Simulated market"
  - [x] `scripts/record_session.py` records a live session (not used by the site)
  - [ ] always-on hosting so visitors can use it without me running it

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
- [x] producers send immediately (`linger.ms=0`) instead of batching for 5 ms

## Stage 3: Settlement (ledger)
- [x] Postgres schema — accounts, balances, positions, trades, sessions
- [x] Python ledger consumes `events.<SYMBOL>`, records trades in Postgres
- [x] idempotent — ON CONFLICT DO NOTHING on (session, seq, symbol); engine seq restarts every session, so the session is part of the key
- [x] verified end-to-end: orders → engine → events → ledger → trades rows in Postgres, across several sessions
- [x] balance/position settlement in Redis via `order_meta`
- [ ] write balances and positions back to Postgres so they survive a restart (today they are re-seeded from the seed data on every start)

## Stage 4: Market data
- [x] Python market-data service consumes `events.<SYMBOL>`
- [x] maintains top of book, depth, last price, recent trades per symbol in Redis
- [x] implements `MarketData` gRPC service — StreamTopOfBook, StreamDepth, StreamTrades
- [x] first message on each stream is a full snapshot, subsequent are deltas
- [x] trader attribution on every trade, a global trade tape, and session counters
- [x] book levels sorted by price (they were sorted by volume)

## Stage 5: Gateway and risk
- [x] gateway implements `SubmitOrder` and `CancelOrder` gRPC endpoints
- [x] gateway routes by symbol to the correct `commands.<SYMBOL>` Kafka topic
- [x] verified end-to-end: grpcurl → gateway → commands.AAPL topic confirmed
- [x] API key → account mapping: SHA-256 hashed keys stored in Postgres, seeded into Redis at startup, looked up per request
- [x] verified: valid key accepted, missing key returns UNAUTHENTICATED
- [x] per-account rate limiting in Redis (token bucket Lua script, 100 token max, 10/sec refill)
- [x] balance check in Redis before forwarding to Kafka — BUY reserves cash, SELL reserves shares via atomic Lua scripts
- [x] balances and positions seeded from Postgres into Redis at gateway startup
- [x] client order ids must be below 1,000,000 (the engine's id-slot limit); the gateway rejects larger ones
- [x] ledger gives back only what is still reserved on cancel, reject, partial fills and the unfilled remainder of FAK orders (unit-tested)
- [x] live check: money and shares conserved across all accounts after 40 trades with cancelled remainders (`scripts/resilience_check.sh`); a longer soak run is still open

## Stage 6: Kubernetes
- [x] manifests: Deployments and Services for every component, namespace, ConfigMap, Secret template
- [x] gRPC health checks as readiness and liveness probes (gateway, http gateway)
- [x] one engine-host Deployment per symbol (SYMBOL env var), c6i node affinity
- [x] load balancer annotations on the gateway and http gateway
- [x] Dockerfiles for the engine host, services and bots, and a compose file that runs everything (`deploy/docker-compose.prod.yml`); written but not yet built or pushed
- [ ] images built and pushed
- [ ] Kafka via Strimzi, Redis and Postgres via Bitnami Helm charts
- [ ] gateway HPA, EBS volumes, cluster autoscaler
- [ ] actually deployed to a cluster

## Stage 7: Crash recovery
- [x] engine-host replays `commands.<SYMBOL>` from offset 0 on startup to rebuild the book, without republishing events
- [x] kill-and-restart test: engine killed with SIGKILL, restarted, and orders that were resting before the crash still match (`scripts/resilience_check.sh`)
- [x] ledger balances conserved across a crash and recovery (same script)

## Stage 8: Frontend and bots
- [x] demo page on the personal site (not a separate React app): order book, depth chart, trade feed, order entry, order journey, trader cards
- [x] bots in four languages: market maker and random trader (Python), momentum (Go), mean reversion (Rust), TWAP (Java)
- [x] symbols: AAPL, TSLA, NVDA, MSFT, GOOGL
- [x] closing bell: `./start stop` settles, records the session and tears everything down
- [ ] bots anchored to live prices (they use fixed reference prices today)

## Stage 9: Observability and benchmarks
- [x] engine hot path: 99.9% of orders matched in under 1µs
- [x] end-to-end order-to-fill latency (gateway → Kafka → engine → Kafka): p50 2.9 ms, p99 4.7 ms over 1,000 orders (re-measured after the gateway concurrency fixes), one machine over loopback (`scripts/latency_bench.sh`)
- [x] Prometheus metrics from the HTTP gateway, ledger and market data: orders by outcome, order-ack latency, Kafka consumer lag, events handled (the C++ gateway and engine do not export metrics yet)
- [x] Grafana dashboard provisioned (`deploy/monitoring`, `--profile monitoring`); written but not yet run against live traffic
- [ ] throughput test through the whole system
- [ ] architecture diagram and results table in README
