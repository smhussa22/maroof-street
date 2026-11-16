#!/usr/bin/env bash
# Runs the conservation and crash-recovery checks against a minimal real stack (Kafka, Redis, Postgres, one AAPL
# engine, the gateway and the ledger), then tears it down. Stop the main stack first: it shares ports and topics.
set -u
cd "$(dirname "$0")/.."

# 5432 sits inside a port range Windows sometimes reserves, so this check uses 15432
export POSTGRES_PORT="${POSTGRES_PORT:-15432}"
export DATABASE_URL="host=localhost port=$POSTGRES_PORT dbname=exchange user=exchange password=exchange"
PYTHON=/mnt/c/Users/faraz/maroof-street/.venv/bin/python3
PIDS=()
ENGINE_PID=""
OVERRIDE="$(mktemp)"
COMPOSE=(docker-compose -f deploy/docker-compose.yml -f "$OVERRIDE")

cat > "$OVERRIDE" <<'YAML'
services:
  kafka:
    environment:
      KAFKA_HEAP_OPTS: "-Xms256m -Xmx512m"
YAML

cleanup() {
    for pid in "${PIDS[@]}" $ENGINE_PID; do
        kill "$pid" 2>/dev/null
    done
    sleep 1
    "${COMPOSE[@]}" down >/dev/null 2>&1
    rm -f "$OVERRIDE"
    echo "stack torn down"
}
trap cleanup EXIT INT TERM

wait_for() {
    local label="$1"; shift
    local tries=0
    printf "waiting for %s..." "$label"
    until "$@" >/dev/null 2>&1; do
        tries=$((tries + 1))
        if [ "$tries" -ge 120 ]; then echo " gave up after 120s"; exit 1; fi
        sleep 1; printf "."
    done
    echo " ready"
}

start_engine() {
    SYMBOL=AAPL nice -n 10 ./build/services/engine-host/engine-host > /tmp/resilience_engine.log 2>&1 &
    ENGINE_PID=$!
}

wait_for "the docker engine" docker info
"${COMPOSE[@]}" up -d kafka redis postgres >/dev/null 2>&1
wait_for kafka "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
wait_for redis nc -z localhost 6379
wait_for postgres "$PYTHON" -c "import os, psycopg2; psycopg2.connect(os.environ['DATABASE_URL']).close()"

"$PYTHON" - <<'PY'
import os, psycopg2
conn = psycopg2.connect(os.environ["DATABASE_URL"])
with conn, conn.cursor() as cur:
    cur.execute(open("services/ledger/schema.sql").read())
PY
for topic in commands.AAPL events.AAPL; do
    "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null
done

start_engine
nice -n 10 ./build/services/gateway/gateway > /dev/null 2>&1 &
PIDS+=($!)
# a fresh session per run: trades are keyed by (session, seq, symbol), so reusing one would skip every fill as a duplicate
EXCHANGE_SESSION="resilience-$(date +%s)" PYTHONPATH=services/ledger nice -n 10 "$PYTHON" services/ledger/main.py > /tmp/resilience_ledger.log 2>&1 &
PIDS+=($!)
wait_for gateway nc -z localhost 50051

echo "--- before the crash ---"
PYTHONPATH=services/ledger "$PYTHON" scripts/resilience_check.py before; BEFORE=$?

echo "--- killing the engine (SIGKILL) and restarting it ---"
kill -9 "$ENGINE_PID"; sleep 1
start_engine

echo "--- after the restart ---"
PYTHONPATH=services/ledger "$PYTHON" scripts/resilience_check.py after; AFTER=$?

[ "$BEFORE" -eq 0 ] && [ "$AFTER" -eq 0 ] && echo "ALL CHECKS PASSED" || echo "SOME CHECKS FAILED"
