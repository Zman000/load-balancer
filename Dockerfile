# Stage 1: Build binaries
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/loadbalancer ./cmd/loadbalancer
RUN CGO_ENABLED=0 go build -o /bin/backend ./cmd/backend

# Stage 2: Backend runtime
FROM alpine:3.21 AS backend
WORKDIR /app
COPY --from=builder /bin/backend .
EXPOSE 9000
ENTRYPOINT ["./backend"]

# Stage 3: Load Balancer runtime (default target)
FROM alpine:3.21 AS loadbalancer
WORKDIR /app
COPY --from=builder /bin/loadbalancer .
COPY config.yaml .
EXPOSE 8080 2112
CMD ["./loadbalancer"]
