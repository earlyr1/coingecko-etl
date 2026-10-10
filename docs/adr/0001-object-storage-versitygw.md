# ADR 0001: Object storage through versitygw over plain files

- Status: accepted
- Date: 2026-10-10

## Context

The assignment wants raw and processed data under `data/raw/` and `data/processed/`, append-only. We also want the pipeline code to use the same API it would use in production (Amazon S3), and we need write-once semantics: a retried write must never replace an object with different bytes and must be recognisable as a retry.

Options considered:

- **Local filesystem writes from Go.** Simple, but the code would not be the production code, and "create if absent" plus a checksum check would have to be built by hand on every writer.
- **MinIO.** The usual choice, but the repository was archived on 2026-04-24 and the official images can no longer be pulled anonymously.
- **rclone serve s3.** Serves a directory over S3, but silently ignores `If-None-Match`, so write-once cannot be enforced.
- **versitygw (posix backend).** An S3 gateway that maps buckets to directories and objects to plain files. A spike confirmed `If-None-Match: *` (412 on an existing key, exactly one winner among 20 parallel writers), user metadata, ListObjectsV2 and multipart uploads with aws-sdk-go-v2.

## Decision

Run `versity/versitygw:v1.8.0` with the posix backend. The gateway root `/lake` is the bind mount `./data`; the buckets `raw`, `processed`, `rejected` and `dlq` are its top-level directories. Metadata is stored with `--sidecar /meta` in `./.s3meta`, because Docker Desktop bind mounts on macOS do not support extended attributes (the default xattr mode refuses to start). All code talks to it through aws-sdk-go-v2 with path-style addressing. Every write is `PutObject` with `If-None-Match: *` and a `sha256` user-metadata entry; on 412 the writer reads the stored checksum and treats equal bytes as success.

## Consequences

- Reviewers can `cat` every object; the same code targets Amazon S3 by changing the endpoint.
- Files placed in `./data` by hand have no stored metadata, and versitygw then lets `If-None-Match: *` overwrite them. The lake must only be written through the gateway; the README says so.
- `./data` and `./.s3meta` belong together: deleting only the metadata makes existing objects look foreign. Buckets are created through the gateway by `etl init` so that they get metadata. `make up` creates `./data`, `./logs` and `./.s3meta` but never the bucket directories: versitygw answers `CreateBucket` with `500 InternalError` for a bucket directory it did not create, so `etl init` checks `HeadBucket` first and skips `CreateBucket` when the bucket answers.
- Sidecar metadata is marked experimental upstream; the integration tests run against the same image and mode.
