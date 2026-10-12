-- +goose Up
-- Raw zone: one row per HTTP call to CoinGecko, including failed calls.
-- Rows are written before any deduplication; export_batch_id is the only column changed after insert (set once by the exporter).
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.api_calls (
  seq              bigint      GENERATED ALWAYS AS IDENTITY,
  event_id         uuid        NOT NULL,
  idempotency_key  text        CHECK (idempotency_key ~ '^[0-9a-f]{64}$'),
  schema_version   smallint    NOT NULL,
  source           text        NOT NULL,
  endpoint         text        NOT NULL,
  request          jsonb       NOT NULL,
  fetched_at       timestamptz NOT NULL,
  outcome          text        NOT NULL CHECK (outcome IN ('ok','http_error','network_error')),
  http             jsonb,
  error            text,
  payload          json,
  payload_sha256   bytea       CHECK (octet_length(payload_sha256) = 32),
  export_batch_id  bigint,
  CONSTRAINT api_calls_pkey PRIMARY KEY (event_id, fetched_at),
  CONSTRAINT api_calls_payload_iff_ok CHECK ((outcome = 'ok') = (payload IS NOT NULL))
) PARTITION BY RANGE (fetched_at);

COMMENT ON COLUMN raw.api_calls.payload IS 'Response body byte-for-byte (json, not jsonb).';
COMMENT ON COLUMN raw.api_calls.export_batch_id IS 'pipeline.export_batches.batch_id of the batch that exported this row; NULL = not exported yet (rows with outcome <> ok stay NULL forever).';

-- Exporter claim (and the first window on an empty ledger): unexported ok
-- rows in seq order. Small in steady state; behaves like a queue index (dead
-- entries until vacuum).
CREATE INDEX api_calls_unexported_idx ON raw.api_calls (seq) WHERE outcome = 'ok' AND export_batch_id IS NULL;
-- Exporter read-back of one batch.
CREATE INDEX api_calls_export_batch_idx ON raw.api_calls (export_batch_id, seq) WHERE export_batch_id IS NOT NULL;
CREATE INDEX api_calls_idempotency_key_idx ON raw.api_calls (idempotency_key);

-- +goose Down
DROP TABLE IF EXISTS raw.api_calls;
DROP SCHEMA IF EXISTS raw;
