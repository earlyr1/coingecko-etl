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
CRONICLE_URL ?= http://localhost:3012
# Upper bound for make demo: with ETL_WINDOW=1m the first processed records
# appear about 3 minutes after the containers start.
DEMO_TIMEOUT ?= 300
NODE_IMAGE := node:24.21.0-trixie-slim

.PHONY: build test test-unit lint alerts-test cronicle-test up down clean demo demo-report

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

# Unit-test the Cronicle provisioning script (schedule from ETL_WINDOW), and check
# that compose hands one ETL_WINDOW to the exporter, the transformers and the scheduler.
cronicle-test:
	docker run --rm -v "$(CURDIR)/deploy/cronicle:/w:ro" -w /w $(NODE_IMAGE) node --test
	ETL_WINDOW=15m $(COMPOSE) config --format json \
	  | jq -e '[.services.exporter, .services.transformer, .services.cronicle] | all(.environment.ETL_WINDOW == "15m")' >/dev/null

up:
	mkdir -p data logs .s3meta
	@test -f .env || sed -e "s/^ETL_UID=.*/ETL_UID=$$(id -u)/" -e "s/^ETL_GID=.*/ETL_GID=$$(id -g)/" .env.example > .env
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

# Wipes everything: the containers (orphans of services this file does not
# define included), the named volumes (Postgres,
# Redpanda, Prometheus, Grafana, Pushgateway, Cronicle), the lake and the logs.
# Object names carry only time, so the lake may also be kept when the stack is re-created.
clean:
	$(COMPOSE) down -v --remove-orphans && rm -rf data logs .s3meta

# One-minute windows, so a window is exported, published by Cronicle and
# processed within the wait. The variable reaches `up` (and compose) too.
demo: export ETL_WINDOW = 1m
demo: up
	@echo "Pipeline started with ETL_WINDOW=1m; waiting up to $(DEMO_TIMEOUT)s for processed records..."
	@for i in $$(seq $(DEMO_TIMEOUT)); do \
	  if find data/processed -type f -name '*.jsonl' 2>/dev/null | grep -q .; then break; fi; \
	  sleep 1; \
	done
	@sleep 60 # one more window, so the duplicate share has data
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
	@echo -n "raw windows closed (_SUCCESS): "; find data/raw -type f -name _SUCCESS 2>/dev/null | wc -l | tr -d ' '
	@set -o pipefail; echo -n "last successful publish (s ago): "; \
	curl -fsG "$(PROMETHEUS_URL)/api/v1/query" \
	  --data-urlencode 'query=time() - max(etl_job_last_success_timestamp_seconds{job="etl_publisher"})' \
	  | sed -nE 's/.*"value":\[[^,]*,"([^".]*)[^"]*"\].*/\1/p' | grep . || echo "n/a (no successful publish yet)"
	@echo "Grafana:          $(GRAFANA_URL)  (dashboard: ETL / CoinGecko ETL)"
	@echo "Cronicle:         $(CRONICLE_URL)  (jobs etl-publish, etl-retention, etl-vacuum; login admin)"
	@echo "Redpanda Console: $(CONSOLE_URL)"
	@echo "Prometheus:       $(PROMETHEUS_URL)/alerts"
	@echo "Lake on disk:     $(CURDIR)/data/{raw,processed,rejected,dlq}"
