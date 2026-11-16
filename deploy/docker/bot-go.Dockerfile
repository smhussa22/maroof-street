# build from the repository root:  docker build -f deploy/docker/bot-go.Dockerfile -t maroof-street/go-bot .
FROM golang:1.22 AS builder
WORKDIR /src
COPY services/bots/go/ .
RUN CGO_ENABLED=0 go build -o /go-bot .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /go-bot /go-bot
ENTRYPOINT ["/go-bot"]
