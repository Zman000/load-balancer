.PHONY: all build test test-race bench-pool vet up down ps logs bench chaos clean

all: build

build:
	go build -o bin/loadbalancer ./cmd/loadbalancer
	go build -o bin/backend ./cmd/backend
	go build -o bin/loadtest ./cmd/loadtest

test:
	go test -v ./internal/...

test-race:
	go test -race ./internal/...

bench-pool:
	go test -bench=BenchmarkAcquireRelease -benchtime=3s ./internal/pool

vet:
	go vet ./...

up:
	docker compose up --build -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f lb

bench:
	docker compose --profile bench run --rm loadtest

chaos:
	python run_chaos_test.py

clean:
	rm -rf bin/ *.test *.out benchmark_results.csv plots/chaos_test.csv

