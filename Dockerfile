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
# The runtime image has no shell to create directories. /app/logs is
# world-writable with the sticky bit, so a service can create
# logs/<service>/ under any --user, with or without a ./logs bind mount.
# It is copied through its parent: COPY of a directory copies its contents,
# and the target directory itself would be created with mode 0755.
RUN mkdir -p /out/app/logs && chmod 1777 /out/app/logs

# ---- runtime: no shell, no package manager, runs as uid 65532 ----
FROM gcr.io/distroless/static-debian13:nonroot
WORKDIR /app
COPY --from=build /out/etl /etl
COPY --from=build --chown=nonroot:nonroot /out/app/ /app/
ENV ETL_LOG_DIR=/app/logs \
    ETL_HTTP_ADDR=:8080
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/etl"]
