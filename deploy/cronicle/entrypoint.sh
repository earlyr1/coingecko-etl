#!/bin/sh
# Entrypoint of the cronicle container: one-time storage setup on a fresh
# volume, background provisioning of the ETL events, then Cronicle itself in the
# foreground. exec keeps this PID, so Cronicle receives SIGTERM from tini
# (compose init: true), aborts running jobs and rewinds their event cursor.
set -eu
cd /opt/cronicle
mkdir -p /var/lib/cronicle/data /var/lib/cronicle/logs

if [ ! -f data/.etl-setup-done ]; then
  node etl/setup-gen.js          # conf/setup.json: admin password, provisioning API key
  node bin/storage-cli.js setup  # users, plugins, server groups, the API key
  touch data/.etl-setup-done
fi

# A restarted container keeps the pid file of the previous run.
rm -f logs/cronicled.pid

# The subshell detaches provisioning from this PID, so tini reaps it. On
# failure it sends SIGTERM to Cronicle ($$): the container stops and the
# restart policy retries, instead of a scheduler running without its events.
( node etl/provision.js "$$" & )

exec node lib/main.js
