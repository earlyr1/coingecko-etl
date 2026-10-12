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

Run versitygw with the posix backend. The gateway root `/lake` is the bind mount `./data`; the buckets `raw`, `processed`, `rejected` and `dlq` are its top-level directories. Object metadata lives in a sidecar directory, `./.s3meta`, because Docker Desktop bind mounts lack extended attributes. All code talks to it through aws-sdk-go-v2 with path-style addressing. Every write is `PutObject` with `If-None-Match: *` and a `sha256` user-metadata entry; on 412 the writer reads the stored checksum and treats equal bytes as success.

## Consequences

- Reviewers can `cat` every object; the same code targets Amazon S3 by changing the endpoint.
- Files placed in `./data` by hand have no stored metadata, and versitygw then lets `If-None-Match: *` overwrite them. The lake must only be written through the gateway.
- `./data` and `./.s3meta` belong together: deleting only the metadata makes existing objects look foreign. `etl init` creates the buckets through the gateway so that they get metadata, and skips a bucket that already answers `HeadBucket`, because versitygw rejects `CreateBucket` for a bucket directory it did not create.
- Sidecar metadata is marked experimental upstream; the integration tests run against the same image and mode.
