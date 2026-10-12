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

# ---- cronicle-build: Cronicle from the pinned upstream release ----
# Cronicle publishes no official image; it is built from the release tarball.
FROM node:24.21.0-trixie-slim AS cronicle-build
ARG CRONICLE_VERSION=0.9.135
WORKDIR /opt/cronicle
ADD --checksum=sha256:96dcfebd7682df31134cfea2fa5e0f5d372fcddbfb82488e9ce0a12122367cd1 \
    https://github.com/jhuckaby/Cronicle/archive/refs/tags/v${CRONICLE_VERSION}.tar.gz /tmp/cronicle.tar.gz
# --ignore-scripts skips the postinstall hook that registers a systemd service.
RUN tar -xzf /tmp/cronicle.tar.gz --strip-components 1 \
 && rm /tmp/cronicle.tar.gz \
 && npm ci --omit=dev --ignore-scripts --no-audit --no-fund \
 && node bin/build.js dist \
 && rm -rf /root/.npm

# ---- cronicle: the scheduler of the batch jobs, with the etl binary ----
FROM node:24.21.0-trixie-slim AS cronicle
# procps: Cronicle samples the CPU and memory of running jobs with ps.
RUN apt-get update \
 && apt-get install -y --no-install-recommends procps \
 && rm -rf /var/lib/apt/lists/*
COPY --from=cronicle-build /opt/cronicle /opt/cronicle
COPY --from=build /out/etl /usr/local/bin/etl
COPY deploy/cronicle/entrypoint.sh deploy/cronicle/setup-gen.js deploy/cronicle/provision.js /opt/cronicle/etl/
# Storage (data/) and logs (logs/, including the job files Cronicle uses to
# record a job cut short by an unclean stop) live on one volume at
# /var/lib/cronicle. The container runs as the host uid, so the volume root and
# /app/logs are world-writable (Docker copies the mode into a fresh volume),
# and setup-gen.js may rewrite conf/setup.json.
RUN mkdir -p /var/lib/cronicle /app/logs \
 && chmod 1777 /var/lib/cronicle /app/logs \
 && rm -rf /opt/cronicle/data /opt/cronicle/logs \
 && ln -s /var/lib/cronicle/data /opt/cronicle/data \
 && ln -s /var/lib/cronicle/logs /opt/cronicle/logs \
 && chmod 0666 /opt/cronicle/conf/setup.json \
 && chmod 0755 /opt/cronicle/etl/entrypoint.sh
WORKDIR /opt/cronicle
# foreground: no daemon fork; echo: only the error log to stdout (the
# transaction log would print the request headers of every API call, the
# provisioning X-API-Key included; job results are in the UI and in
# logs/<service>/etl.log); master: a single server becomes primary at once
# instead of after a 60 s election timeout.
ENV CRONICLE_foreground=1 \
    CRONICLE_echo="Error" \
    CRONICLE_color=0 \
    CRONICLE_master=1 \
    CRONICLE_queue_dir=logs/queue \
    CRONICLE_job_data_expire_days=30 \
    ETL_LOG_DIR=/app/logs
EXPOSE 3012
ENTRYPOINT ["/opt/cronicle/etl/entrypoint.sh"]

# ---- runtime: no shell, no package manager, runs as uid 65532 ----
# The last stage is the default target (docker build . and the etl services).
FROM gcr.io/distroless/static-debian13:nonroot
WORKDIR /app
COPY --from=build /out/etl /etl
COPY --from=build --chown=nonroot:nonroot /out/app/ /app/
ENV ETL_LOG_DIR=/app/logs \
    ETL_HTTP_ADDR=:8080
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/etl"]
