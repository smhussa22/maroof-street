#!/usr/bin/env bash
# Measures order-to-fill latency through the real gateway, Kafka and engine path.
# Brings up only what that needs (Kafka, Redis, Postgres, one AAPL engine, the gateway), runs the probe,
# then tears it all down. Stop the main stack first: the probe uses the same accounts, ports and topics.
set -u
cd "$(dirname "$0")/.."

PYTHON=/mnt/c/Users/faraz/maroof-street/.venv/bin/python3
PIDS=()
OVERRIDE="$(mktemp)"
COMPOSE=(docker-compose -f deploy/docker-compose.yml -f "$OVERRIDE")

cat > "$OVERRIDE" <<'EOF'
services:
  kafka:
    environment:
      KAFKA_HEAP_OPTS: "-Xms256m -Xmx512m"
EOF

cleanup() {
    for pid in "${PIDS[@]}"; do
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
        if [ "$tries" -ge 120 ]; then
            echo " gave up after 120s"
            exit 1
        fi
        sleep 1
        printf "."
    done
    echo " ready"
}

wait_for "the docker engine" docker info

"${COMPOSE[@]}" up -d kafka redis postgres >/dev/null 2>&1

wait_for kafka "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
wait_for redis nc -z localhost 6379
wait_for postgres "$PYTHON" -c "import psycopg2; psycopg2.connect('host=localhost dbname=exchange user=exchange password=exchange').close()"

# make sure the accounts, keys and balances the gateway seeds from exist, then create the two topics up front
"$PYTHON" - <<'EOF'
import psycopg2
conn = psycopg2.connect("host=localhost dbname=exchange user=exchange password=exchange")
with conn, conn.cursor() as cur:
    cur.execute(open("services/ledger/schema.sql").read())
EOF
for topic in commands.AAPL events.AAPL; do
    "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null
done

# low priority so the machine stays usable while this runs
SYMBOL=AAPL nice -n 10 ./build/services/engine-host/engine-host > /dev/null 2>&1 &
PIDS+=($!)
nice -n 10 ./build/services/gateway/gateway > /dev/null 2>&1 &
PIDS+=($!)
wait_for gateway nc -z localhost 50051

PYTHONPATH=services/ledger nice -n 10 "$PYTHON" scripts/latency_probe.py
