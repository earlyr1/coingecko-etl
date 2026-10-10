package testutil

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestStartPostgres(t *testing.T) {
	dsn := StartPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var version string
	if err := conn.QueryRow(ctx, `SHOW server_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(version, "18.6") {
		t.Errorf("server_version = %s, want 18.6", version)
	}
}

func TestStartRedpanda(t *testing.T) {
	brokers := StartRedpanda(t)
	if len(brokers) != 1 {
		t.Fatalf("brokers = %v", brokers)
	}
	c, err := net.DialTimeout("tcp", brokers[0], 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", brokers[0], err)
	}
	_ = c.Close()
}

func TestStartVersityGW(t *testing.T) {
	endpoint, dataDir := StartVersityGW(t)
	resp, err := http.Get(endpoint + "/health")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health = %d", resp.StatusCode)
	}
	// Unsigned S3 calls must be refused: the gateway enforces credentials.
	resp, err = http.Get(endpoint + "/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anonymous ListBuckets = %d, want 403", resp.StatusCode)
	}
	for _, b := range []string{"raw", "processed", "rejected", "dlq"} {
		if fi, err := os.Stat(filepath.Join(dataDir, b)); err != nil || !fi.IsDir() {
			t.Errorf("bucket dir %s missing: %v", b, err)
		}
	}
}
