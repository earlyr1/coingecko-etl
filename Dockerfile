# syntax=docker/dockerfile:1

# ---- build: static Go binary ----
FROM golang:1.27 AS build
WORKDIR /src
# Download modules in their own layer so code changes do not refetch them.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/etl ./cmd/etl

# ---- runtime: no shell, no package manager, runs as uid 65532 ----
FROM gcr.io/distroless/static-debian13:nonroot
WORKDIR /app
COPY --from=build /out/etl /etl
ENV ETL_LOG_DIR=/app/logs \
    ETL_HTTP_ADDR=:8080
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/etl"]
