-- +goose Up
-- Raw zone: one row per HTTP call to CoinGecko, including failed calls.
-- Rows are written before any deduplication; idempotency_key is informational.
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
  ingest_txid      xid8        NOT NULL DEFAULT pg_current_xact_id(),
  CONSTRAINT api_calls_pkey PRIMARY KEY (event_id, fetched_at),
  CONSTRAINT api_calls_payload_iff_ok CHECK ((outcome = 'ok') = (payload IS NOT NULL))
) PARTITION BY RANGE (fetched_at);

COMMENT ON COLUMN raw.api_calls.payload IS 'Response body byte-for-byte (json, not jsonb).';
COMMENT ON COLUMN raw.api_calls.ingest_txid IS 'xid8 of the inserting transaction; exporter cursor (ingest_txid, seq).';

CREATE INDEX api_calls_export_cursor_idx ON raw.api_calls (ingest_txid, seq) WHERE outcome = 'ok';
CREATE INDEX api_calls_idempotency_key_idx ON raw.api_calls (idempotency_key);

-- +goose Down
DROP TABLE IF EXISTS raw.api_calls;
DROP SCHEMA IF EXISTS raw;
