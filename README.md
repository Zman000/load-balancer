# High-Performance TCP Load Balancer

A production-ready, Layer 4 (TCP) Load Balancer written entirely in Go. Built from first principles without relying on standard HTTP reverse-proxy libraries, this project demonstrates advanced concurrent socket programming, resilient system design, and comprehensive infrastructure observability.

## Core Architecture & Features

*   **Raw TCP Proxying:** Manages raw `net.Conn` interfaces with concurrent goroutines and zero-copy `io.Copy` bidirectional streaming. Features custom sliding-window idle timeouts to gracefully reap zombie connections without disrupting active payloads.
*   **Token-Bucket Rate Limiting:** Implements a thread-safe, IP-based sliding window rate limiter. Includes background garbage collection to automatically purge stale client records and prevent memory leaks.
*   **Stateful Circuit Breaker:** Features a localized three-state (Closed, Half-Open, Open) circuit breaker pattern. 
    *   Utilizes pure evaluation functions to prevent accidental state mutations.
    *   Implements jittered exponential backoff for retry storms.
    *   Uses watchdog timers to isolate and dispatch single probe requests during the `Half-Open` recovery state.
*   **Automated Observability Stack:** Fully containerized with Prometheus and Grafana via Docker Compose. Exposes custom PromQL metrics tracking p50/p95/p99 latency histograms, rate limit rejections, active semaphore slots, and real-time backend health.

## Project Structure

```text
.
├── cmd/
│   ├── loadbalancer/   # Main load balancer entrypoint
│   ├── backend/        # Dummy HTTP backend server for testing
│   └── loadtest/       # Custom high-concurrency benchmarking tool
├── internal/
│   ├── config/         # YAML configuration parsing and validation
│   ├── pool/           # Backend state management & Circuit Breaker logic
│   └── proxy/          # Rate Limiting & TCP socket tunneling
├── grafana/            # Auto-provisioned dashboards and datasources
├── prometheus/         # Prometheus scraping configurations
├── config.yaml         # Load Balancer runtime configuration
└── docker-compose.yml  # Infrastructure-as-Code cluster definition
```

## Getting Started

The entire cluster, including the load balancer, 3 test backends, Prometheus, and Grafana, can be provisioned with a single command.

### Prerequisites
* Docker & Docker Compose
* Go 1.22+ (for local development)

### Booting the Cluster

```bash
# Build and run the load balancer, backends, and observability stack in detached mode
docker compose up --build -d
```

Once running, the following services are available:
* **Load Balancer:** `localhost:8080`
* **Grafana Dashboard:** `localhost:3000` (Auto-authenticated)
* **Prometheus Targets:** `localhost:9090`
* **Raw Metrics JSON:** `localhost:2112/status`

## Configuration (config.yaml)

The load balancer is highly configurable without recompilation. 

```yaml
server:
  listen_address: ":8080"
  max_connections: 1000

load_balancer:
  algorithm: "least_connections" # Options: round_robin, least_connections
  connection_timeout: "2s"

rate_limit:
  requests_per_second: 100
  burst_capacity: 50

circuit_breaker:
  max_failures: 3
  cooldown: "5s"

health_check:
  enabled: true
  interval: "10s"
  timeout: "2s"

backends:
  - "backend1:8081"
  - "backend2:8082"
  - "backend3:8083"
```

## Benchmarking & Load Testing

This repository includes a custom concurrent load generator designed to pierce Docker's network proxy and test internal limits.

To fire a benchmark of 500 concurrent clients against the rate limiter and circuit breakers:

```bash
# Run the disposable load testing container
docker compose --profile bench run --rm loadtest
```

**Expected Output:**
```text
Starting benchmark: 500 clients, 1024 byte payload, targeting lb:8080

--- Benchmark Summary ---
Total Requests: 500
Successes:      138  (Throttled perfectly by token-bucket capacity)
Failures:       362  (Gracefully rejected with 429 status)
p50 Latency:    38.00 ms
p99 Latency:    1024.00 ms
```