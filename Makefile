SHELL := /bin/bash
GO ?= go
COMPOSE ?= docker compose
BIN := bin/etl

# Files written through bind mounts (./data, ./logs, ./.s3meta) belong to the
# invoking user, so the containers run with the same uid/gid (see
# docker-compose.yml). make passes them to compose; `up` also writes them into
# a new .env, so plain `docker compose` commands later use the same ids.
export ETL_UID ?= $(shell id -u)
export ETL_GID ?= $(shell id -g)

PROMETHEUS_URL ?= http://localhost:9090
GRAFANA_URL ?= http://localhost:3000
CONSOLE_URL ?= http://localhost:8081
DEMO_WAIT ?= 150

.PHONY: build test test-unit lint alerts-test up down clean demo demo-report

build:
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN) ./cmd/etl

# All tests, including Docker-backed integration tests (skipped when Docker is
# unavailable). -p 2 limits how many packages start containers at once (as in CI).
test:
	$(GO) test -race -p 2 -count=1 -timeout 15m ./...

# Unit tests only: integration tests skip themselves under -short.
test-unit:
	$(GO) test -short -count=1 ./...

lint:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "gofmt needed on:"; echo "$$unformatted"; exit 1; fi
	$(GO) vet ./...

# Validate the Prometheus config and unit-test the alert rules with promtool.
alerts-test:
	docker run --rm --entrypoint promtool -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" \
	  prom/prometheus:v3.15.0 check config /etc/prometheus/prometheus.yml
	docker run --rm --entrypoint promtool -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" -w /etc/prometheus \
	  prom/prometheus:v3.15.0 test rules alerts_test.yml

up:
	mkdir -p data logs .s3meta
	@test -f .env || sed -e "s/^ETL_UID=.*/ETL_UID=$$(id -u)/" -e "s/^ETL_GID=.*/ETL_GID=$$(id -g)/" .env.example > .env
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

# Wipes everything: the containers, the named volumes (Postgres, Redpanda,
# Prometheus, Grafana), the lake and the logs. The lake (./data, ./.s3meta)
# and the Postgres/Redpanda volumes are reset together because object keys
# are unique only within one Postgres/Redpanda lifetime.
clean:
	$(COMPOSE) down -v && rm -rf data logs .s3meta

demo: up
	@echo "Pipeline started; waiting $(DEMO_WAIT)s for data to flow through every stage..."
	@sleep $(DEMO_WAIT)
	@$(MAKE) --no-print-directory demo-report

# demo-report prints the summary without (re)starting the stack.
demo-report:
	@printf '%-10s %8s %10s\n' bucket objects records
	@for b in raw processed rejected dlq; do \
	  objects=$$(find data/$$b -type f -name '*.jsonl' 2>/dev/null | wc -l | tr -d ' '); \
	  records=$$(find data/$$b -type f -name '*.jsonl' -exec cat {} + 2>/dev/null | wc -l | tr -d ' '); \
	  printf '%-10s %8s %10s\n' "$$b" "$$objects" "$$records"; \
	done
	@set -o pipefail; echo -n "duplicate share (dropped / (dropped + written)): "; \
	curl -fsG "$(PROMETHEUS_URL)/api/v1/query" \
	  --data-urlencode 'query=sum(etl_dedup_dropped_total) / (sum(etl_dedup_dropped_total) + sum(etl_records_total{outcome="written"}))' \
	  | sed -nE 's/.*"value":\[[^,]*,"([^"]*)"\].*/\1/p' | grep . || echo "n/a (no data yet or Prometheus not reachable)"
	@echo "Grafana:          $(GRAFANA_URL)  (dashboard: ETL / CoinGecko ETL)"
	@echo "Redpanda Console: $(CONSOLE_URL)"
	@echo "Prometheus:       $(PROMETHEUS_URL)/alerts"
	@echo "Lake on disk:     $(CURDIR)/data/{raw,processed,rejected,dlq}"
