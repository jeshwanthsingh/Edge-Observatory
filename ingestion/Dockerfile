FROM golang:1.25-alpine AS builder

RUN apk add --no-cache protoc protobuf-dev
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.33.0 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN protoc --go_out=. --go-grpc_out=. proto/telemetry.proto

RUN CGO_ENABLED=0 go build -o /gateway ./cmd/gateway/main.go
RUN CGO_ENABLED=0 go build -o /simulator ./cmd/simulator/main.go

FROM alpine:3.19

WORKDIR /app
COPY --from=builder /gateway /app/gateway
COPY --from=builder /simulator /app/simulator

CMD ["/app/gateway"]
EXPOSE 9090 8080 8081
