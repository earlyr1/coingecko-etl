SHELL := /bin/bash
GO ?= go
COMPOSE ?= docker compose
BIN := bin/etl

# Overridable endpoints used by `make demo` (ports are fixed in docker-compose.yml, Task 17).
PROMETHEUS_URL ?= http://localhost:9090
GRAFANA_URL ?= http://localhost:3000
CONSOLE_URL ?= http://localhost:8081
DEMO_WAIT ?= 180

.PHONY: build test test-unit lint up down demo demo-report

build:
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN) ./cmd/etl

# All tests, including Docker-backed integration tests (skipped when Docker is unavailable).
test:
	$(GO) test -race -count=1 ./...

# Unit tests only: integration tests skip themselves under -short.
test-unit:
	$(GO) test -short -count=1 ./...

lint:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "gofmt needed on:"; echo "$$unformatted"; exit 1; fi
	$(GO) vet ./...

up:
	mkdir -p data logs .s3meta
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

demo: up demo-report

# demo-report prints the summary without (re)starting the stack.
demo-report:
	@echo "Waiting up to $(DEMO_WAIT)s for the first processed object..."
	@for i in $$(seq 1 $(DEMO_WAIT)); do \
	  if [ -n "$$(find data/processed -name '*.jsonl' 2>/dev/null | head -n 1)" ]; then break; fi; \
	  sleep 1; \
	done
	@printf '%-10s %8s %10s\n' zone objects records
	@for zone in raw processed rejected dlq; do \
	  objects=$$(find data/$$zone -name '*.jsonl' 2>/dev/null | wc -l | tr -d ' '); \
	  records=$$(find data/$$zone -name '*.jsonl' -exec cat {} + 2>/dev/null | wc -l | tr -d ' '); \
	  printf '%-10s %8s %10s\n' "$$zone" "$$objects" "$$records"; \
	done
	@set -o pipefail; echo -n "duplicate share (dropped / (dropped + written)): "; \
	curl -fsG "$(PROMETHEUS_URL)/api/v1/query" \
	  --data-urlencode 'query=sum(etl_dedup_dropped_total) / (sum(etl_dedup_dropped_total) + sum(etl_records_total{outcome="written"}))' \
	  | sed -nE 's/.*"value":\[[^,]*,"([^"]*)"\].*/\1/p' || echo "n/a (Prometheus not reachable)"
	@echo "Grafana:          $(GRAFANA_URL)"
	@echo "Redpanda Console: $(CONSOLE_URL)"
	@echo "Prometheus:       $(PROMETHEUS_URL)"
