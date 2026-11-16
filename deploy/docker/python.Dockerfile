# ledger, market data, HTTP gateway and the two Python bots share this image recipe; ENTRY is the command to run
# build from the repository root:
#   docker build -f deploy/docker/python.Dockerfile --build-arg ENTRY="python services/ledger/main.py" -t maroof-street/ledger .
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir grpcio grpcio-tools protobuf redis confluent-kafka psycopg2-binary \
    fastapi "uvicorn[standard]" pydantic prometheus-client

# the generated gRPC stubs are not committed, so they are built here from the .proto files
COPY proto proto
RUN mkdir -p services/ledger && for p in proto/*.proto; do \
        python -m grpc_tools.protoc --python_out=services/ledger --grpc_python_out=services/ledger -Iproto "$p"; \
    done
COPY services services

ENV PYTHONPATH=/app/services/ledger:/app/services/market-data:/app/services/gateway-http
ARG ENTRY
ENV ENTRY=${ENTRY}
CMD ["sh", "-c", "exec $ENTRY"]
