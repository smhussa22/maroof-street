#!/usr/bin/env bash
# generates *_pb2.py and *_pb2_grpc.py for all protos into services/ledger/
# (ledger and market-data both import from services/ledger via PYTHONPATH)
set -euo pipefail

VENV_PYTHON="/mnt/c/Users/faraz/maroof-street/.venv/bin/python3"
PROTO_DIR="proto"
OUT_DIR="services/ledger"

for proto in "$PROTO_DIR"/*.proto; do
    "$VENV_PYTHON" -m grpc_tools.protoc \
        --python_out="$OUT_DIR" \
        --grpc_python_out="$OUT_DIR" \
        -I"$PROTO_DIR" \
        "$proto"
    echo "generated: $(basename $proto)"
done

echo "done — stubs in $OUT_DIR"
