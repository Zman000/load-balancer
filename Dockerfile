# Stage 1: Build the binary
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o loadbalancer ./cmd/loadbalancer

# Stage 2: Run the binary
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/loadbalancer .
COPY config.yaml .
EXPOSE 8080 2112 
CMD ["./loadbalancer"]