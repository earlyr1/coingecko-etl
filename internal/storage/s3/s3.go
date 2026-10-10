// Package s3 is the only way the pipeline touches object storage. It wraps
// aws-sdk-go-v2 for an S3-compatible endpoint (versitygw locally) with
// write-once semantics: PutIfAbsent never overwrites an existing object.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"coingecko-etl/internal/event"
)

const (
	BucketRaw       = "raw"
	BucketProcessed = "processed"
	BucketRejected  = "rejected"
	BucketDLQ       = "dlq"
)

// ErrChecksumMismatch means the key already holds different bytes
// (non-determinism or a foreign writer). Callers decide how to react; most
// treat it as fatal for the batch.
var ErrChecksumMismatch = errors.New("s3: object exists with different checksum")

const (
	metaSHA256  = "sha256" // sent as x-amz-meta-sha256
	contentType = "application/x-ndjson"
)

type Config struct{ Endpoint, AccessKey, SecretKey, Region string }

type Client struct{ api *s3.Client }

// New builds a path-style client (http://host:port/bucket/key). Path style is
// required because versitygw has no virtual-host bucket DNS.
func New(ctx context.Context, cfg Config) (*Client, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("s3: load config: %w", err)
	}
	api := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
	})
	return &Client{api: api}, nil
}

// PutIfAbsent writes body under bucket/key only if the key does not exist.
// created=true: this call wrote it. created=false, err=nil: the object was
// already there with the same sha256 (an idempotent retry). ErrChecksumMismatch:
// the object exists with other content.
func (c *Client) PutIfAbsent(ctx context.Context, bucket, key string, body []byte) (bool, error) {
	sum := event.SHA256Hex(body)
	_, err := c.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		// bytes.Reader is seekable: the SDK must rewind the body to compute the
		// payload checksum; an unseekable io.Reader fails over plain http.
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		ContentType:   aws.String(contentType),
		IfNoneMatch:   aws.String("*"),
		Metadata:      map[string]string{metaSHA256: sum},
	})
	if err == nil {
		return true, nil
	}
	if !isPreconditionFailed(err) {
		return false, fmt.Errorf("s3: put %s/%s: %w", bucket, key, err)
	}
	head, herr := c.api.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if herr != nil {
		return false, fmt.Errorf("s3: head %s/%s after 412: %w", bucket, key, herr)
	}
	if got, ok := metaValue(head.Metadata, metaSHA256); ok && strings.EqualFold(got, sum) {
		return false, nil
	}
	return false, fmt.Errorf("%w: %s/%s", ErrChecksumMismatch, bucket, key)
}

// Get returns the whole object body.
func (c *Client) Get(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := c.api.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("s3: get %s/%s: %w", bucket, key, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3: read %s/%s: %w", bucket, key, err)
	}
	return b, nil
}

// EnsureBuckets creates each bucket; an existing bucket is not an error.
// HeadBucket goes first: versitygw v1.8.0 answers 500 InternalError to
// CreateBucket when the bucket directory already exists on disk but was not
// created through the gateway (mkdir data/raw, or a deleted .s3meta), while
// HeadBucket on it returns 200 and PUT/GET work.
func (c *Client) EnsureBuckets(ctx context.Context, buckets ...string) error {
	for _, b := range buckets {
		if _, err := c.api.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(b)}); err == nil {
			continue
		}
		_, err := c.api.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)})
		if err == nil || isBucketExists(err) {
			continue
		}
		return fmt.Errorf("s3: create bucket %s: %w", b, err)
	}
	return nil
}

// Ping is the readiness check: a signed ListBuckets proves endpoint and credentials.
func (c *Client) Ping(ctx context.Context) error {
	if _, err := c.api.ListBuckets(ctx, &s3.ListBucketsInput{}); err != nil {
		return fmt.Errorf("s3: ping: %w", err)
	}
	return nil
}

// isPreconditionFailed reports whether err is the 412 answer to If-None-Match:*.
func isPreconditionFailed(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) && ae.ErrorCode() == "PreconditionFailed" {
		return true
	}
	var re *awshttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == http.StatusPreconditionFailed
}

func isBucketExists(err error) bool {
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return true
	}
	var ae smithy.APIError
	return errors.As(err, &ae) && (ae.ErrorCode() == "BucketAlreadyOwnedByYou" || ae.ErrorCode() == "BucketAlreadyExists")
}

// metaValue looks up user metadata case-insensitively: servers and the SDK
// may return "Sha256" or "sha256".
func metaValue(m map[string]string, key string) (string, bool) {
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return "", false
}
