// Package testutil starts throwaway infrastructure containers for
// integration tests (testcontainers-go). Every helper skips the test when
// Docker is unavailable and removes its container in t.Cleanup.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	PostgresImage  = "postgres:18.6"
	RedpandaImage  = "redpandadata/redpanda:v26.2.4"
	VersityGWImage = "versity/versitygw:v1.8.0"

	// Credentials baked into StartVersityGW (match .env.example defaults).
	VersityGWAccessKey = "etl"
	VersityGWSecretKey = "etlsecret123"
)

// SkipIfNoDocker skips the test when no healthy Docker daemon is reachable,
// or when the test binary runs with -short.
func SkipIfNoDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: skipped with -short")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
}

// nameOpt names the container "<prefix><kind>-<random>" when
// ETL_TEST_CONTAINER_PREFIX is set (handy for spotting/cleaning test
// containers); otherwise Docker picks a random name.
func nameOpt(kind string) testcontainers.CustomizeRequestOption {
	prefix := os.Getenv("ETL_TEST_CONTAINER_PREFIX")
	if prefix == "" {
		return func(*testcontainers.GenericContainerRequest) error { return nil }
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return testcontainers.WithName(prefix + kind + "-" + hex.EncodeToString(b))
}

// StartPostgres runs postgres:18.6 (user/password/db = etl) and returns a DSN.
func StartPostgres(t *testing.T) (dsn string) {
	t.Helper()
	SkipIfNoDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ctr, err := tcpostgres.Run(ctx, PostgresImage,
		tcpostgres.WithDatabase("etl"),
		tcpostgres.WithUsername("etl"),
		tcpostgres.WithPassword("etl"),
		// The image restarts once after initdb; wait for the second "ready"
		// log line and then for the mapped TCP port.
		tcpostgres.BasicWaitStrategies(),
		nameOpt("pg"),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	dsn, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}
	return dsn
}

// StartRedpanda runs a single-node Redpanda in dev-container mode (topic
// auto-creation off, the module default) and returns the seed brokers.
func StartRedpanda(t *testing.T) (brokers []string) {
	t.Helper()
	SkipIfNoDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ctr, err := tcredpanda.Run(ctx, RedpandaImage, nameOpt("redpanda"))
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	seed, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("redpanda seed broker: %v", err)
	}
	return []string{seed}
}

// StartVersityGW runs versitygw with the posix backend: the gateway root
// /lake is a host temp dir (dataDir), bucket = top-level directory, object
// key = file path. Metadata goes to a separate sidecar dir (/meta) because
// Docker Desktop bind mounts do not support xattrs. The buckets raw,
// processed, rejected and dlq are pre-created as directories. Returns the
// S3 endpoint ("http://host:port") and the host path of the gateway root.
func StartVersityGW(t *testing.T) (endpoint, dataDir string) {
	t.Helper()
	SkipIfNoDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dataDir = t.TempDir()
	metaDir := t.TempDir()
	for _, b := range []string{"raw", "processed", "rejected", "dlq"} {
		if err := os.MkdirAll(filepath.Join(dataDir, b), 0o755); err != nil {
			t.Fatalf("create bucket dir %s: %v", b, err)
		}
	}

	ctr, err := testcontainers.Run(ctx, VersityGWImage,
		testcontainers.WithEnv(map[string]string{
			"ROOT_ACCESS_KEY_ID":     VersityGWAccessKey,
			"ROOT_SECRET_ACCESS_KEY": VersityGWSecretKey,
		}),
		testcontainers.WithCmd("--health", "/health", "posix", "--sidecar", "/meta", "/lake"),
		testcontainers.WithExposedPorts("7070/tcp"),
		// Run as the test user so files written into dataDir stay removable
		// by t.TempDir cleanup on Linux CI.
		testcontainers.WithConfigModifier(func(c *container.Config) {
			c.User = fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
		}),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.Binds = append(hc.Binds, dataDir+":/lake", metaDir+":/meta")
		}),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/health").
				WithPort("7070/tcp").
				WithStatusCodeMatcher(func(s int) bool { return s == http.StatusOK }).
				WithStartupTimeout(60*time.Second),
		),
		nameOpt("versitygw"),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start versitygw: %v", err)
	}
	hostPort, err := ctr.PortEndpoint(ctx, "7070/tcp", "http")
	if err != nil {
		t.Fatalf("versitygw endpoint: %v", err)
	}
	return hostPort, dataDir
}
