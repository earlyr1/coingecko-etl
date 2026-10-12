// Command etl is the single binary of the pipeline. The first argument selects
// the subcommand; see usage below.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/bootstrap"
	"coingecko-etl/internal/coingecko"
	"coingecko-etl/internal/config"
	"coingecko-etl/internal/exporter"
	"coingecko-etl/internal/fetcher"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/transform"
	"coingecko-etl/internal/transformer"
	"coingecko-etl/internal/validation"
	"coingecko-etl/schemas"
)

const usageText = `usage: etl <command>

commands:
  init         run migrations, create partitions, topic and buckets (one-shot)
  fetcher      poll CoinGecko into raw.api_calls
  exporter     export raw.api_calls to s3://raw in ETL_WINDOW windows (JSONL parts + _SUCCESS)
  publish      publish one closed raw window to the broker and exit (--now <unix seconds|RFC3339>)
  transformer  consume, clean, dedup and write s3://processed
  healthcheck  exit 0 if the local /readyz returns 200 (for container healthchecks)
`

// startupTimeout bounds connecting to dependencies when a service starts.
// init has already waited for them, so a failure here is real; the process
// exits 1 and the compose restart policy tries again.
const startupTimeout = 30 * time.Second

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches a subcommand and returns the process exit code.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageText)
		return 2
	}
	switch args[0] {
	case "healthcheck":
		return healthcheck(os.Getenv("ETL_HTTP_ADDR"), stderr)
	case "init", "fetcher", "exporter", "transformer":
		return runService(args[0], stderr)
	case "publish":
		return runPublish(args[1:], stderr)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usageText)
		return 0
	default:
		fmt.Fprintf(stderr, "etl: unknown command %q\n%s", args[0], usageText)
		return 2
	}
}

// runService loads the configuration and logger, installs the SIGINT/SIGTERM
// handler and runs one subcommand until it returns or a signal arrives.
func runService(name string, stderr io.Writer) int {
	cfg, err := config.Load(name)
	if err != nil {
		fmt.Fprintf(stderr, "etl %s: %v\n", name, err)
		return 1
	}
	log, closer, err := obs.NewLogger(name, logDir(cfg.LogDir))
	if err != nil {
		fmt.Fprintf(stderr, "etl %s: %v\n", name, err)
		return 1
	}
	defer closer.Close()

	// SIGTERM is what `docker compose stop` sends; SIGINT is Ctrl-C.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if name == "init" {
		if err := bootstrap.Run(ctx, cfg, log); err != nil {
			log.Error("init failed", "error", err)
			return 1
		}
		return 0
	}
	if err := serveAndRun(ctx, cfg, log); err != nil {
		log.Error("service failed", "error", err)
		return 1
	}
	log.Info("service stopped")
	return 0
}

// logDir returns the directory handed to obs.NewLogger. Replicas of one
// service (the transformer runs with deploy.replicas: 2) share the ./logs
// bind mount, and two processes must never append to one file, so with
// ETL_LOG_PER_HOST=true each replica writes to <dir>/<hostname>/<service>/etl.log.
// In compose the hostname is the container id, as shown by `docker ps`.
func logDir(dir string) string {
	if os.Getenv("ETL_LOG_PER_HOST") != "true" {
		return dir
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		return dir
	}
	return filepath.Join(dir, host)
}

// serveAndRun builds the dependencies of one long-running service, serves
// /healthz /readyz /metrics on cfg.HTTPAddr and runs the service loop. When
// either the loop or the HTTP server stops, the other one is stopped too.
func serveAndRun(parent context.Context, cfg config.Config, log *slog.Logger) error {
	// One registry per process: the Go runtime and process collectors, the
	// etl_* catalogue.
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	m := obs.NewMetrics(reg)
	health := obs.NewHealth()

	startCtx, cancelStart := context.WithTimeout(parent, startupTimeout)
	svc, cleanup, err := buildService(startCtx, cfg, m, health, log)
	cancelStart()
	defer cleanup()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	srvDone := make(chan error, 1)
	go func() {
		err := obs.Serve(ctx, cfg.HTTPAddr, health, reg)
		if err != nil {
			cancel(fmt.Errorf("http server: %w", err)) // stop the service loop too
		}
		srvDone <- err
	}()
	log.Info("service started", "http_addr", cfg.HTTPAddr)

	runErr := svc(ctx)
	if runErr == nil {
		// The loop returns nil when ctx is cancelled. If the cancel came from
		// the HTTP server rather than a signal, report that cause.
		if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
			runErr = cause
		}
	}
	cancel(nil) // stop the HTTP server
	if srvErr := <-srvDone; srvErr != nil && runErr == nil {
		// The loop already stopped cleanly; a slow HTTP shutdown (e.g. a
		// scrape still in flight after 5 s) is not worth a failing exit code.
		log.Warn("http server shutdown", "error", srvErr)
	}
	return runErr
}

// serviceFunc is one service loop; it returns nil after ctx is cancelled.
type serviceFunc func(ctx context.Context) error

// buildService connects the dependencies of cfg.Service, registers its
// readiness checks and returns its loop. cleanup is always non-nil and
// releases whatever was opened, even when err != nil.
func buildService(ctx context.Context, cfg config.Config, m *obs.Metrics,
	health *obs.Health, log *slog.Logger) (svc serviceFunc, cleanup func(), err error) {
	var closers []func()
	cleanup = func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}

	pool, err := pg.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, cleanup, err
	}
	closers = append(closers, pool.Close)
	health.AddReadiness("postgres", pool.Ping)

	if cfg.Service == "fetcher" {
		lim := coingecko.NewLimiter(cfg.MinRequestSpacing)
		// No Timeout here: coingecko.Client bounds each request with its own
		// 10 s context deadline, the single source of the per-request limit.
		hc := &http.Client{}
		cg := coingecko.NewClient(cfg.CoinGeckoBaseURL, cfg.CoinGeckoAPIKey, hc, lim, time.Now)
		cg.SetLogger(log)
		return func(ctx context.Context) error {
			return fetcher.Run(ctx, cfg, cg, lim, pool, m, log)
		}, cleanup, nil
	}

	// exporter and transformer need object storage and schemas.
	s3c, err := s3.New(ctx, s3.Config{
		Endpoint: cfg.S3Endpoint, AccessKey: cfg.S3AccessKey, SecretKey: cfg.S3SecretKey, Region: cfg.S3Region,
	})
	if err != nil {
		return nil, cleanup, err
	}
	health.AddReadiness("s3", s3c.Ping)
	schemaReg, err := validation.Load(schemas.FS)
	if err != nil {
		return nil, cleanup, fmt.Errorf("load schemas: %w", err)
	}

	switch cfg.Service {
	case "exporter":
		return func(ctx context.Context) error {
			return exporter.Run(ctx, cfg, pool, s3c, schemaReg, m, log)
		}, cleanup, nil

	case "transformer":
		// transformer.Run builds its own consumer-group client; this small
		// client only answers readiness probes.
		pinger, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID("etl-transformer-health"))
		if err != nil {
			return nil, cleanup, fmt.Errorf("broker client: %w", err)
		}
		closers = append(closers, pinger.Close)
		health.AddReadiness("broker", pinger.Ping)
		deps := transformer.Deps{
			Pool: pool, S3: s3c, Reg: schemaReg, Metrics: m, Log: log,
			Rules: transform.Rules{
				StaleAfter:          cfg.StaleAfter,
				TickerStaleAfter:    cfg.TickerStaleAfter,
				TickerAnomalyPct:    cfg.TickerAnomalyPct,
				AnomalyTolerancePct: cfg.AnomalyTolerancePct,
			},
		}
		return func(ctx context.Context) error {
			return transformer.Run(ctx, cfg, deps)
		}, cleanup, nil
	}
	return nil, cleanup, fmt.Errorf("unknown service %q", cfg.Service)
}

// readyzURL turns a listen address such as ":8080" or "0.0.0.0:8080" into the
// loopback URL of /readyz. An empty addr means the default ":8080".
func readyzURL(addr string) (string, error) {
	if addr == "" {
		addr = ":8080"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("bad ETL_HTTP_ADDR %q: %w", addr, err)
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/readyz", nil
}

// healthcheck is used by Docker healthchecks: the distroless image has no curl.
// The 3 s client timeout exceeds the 2 s readiness timeout of /readyz.
func healthcheck(addr string, stderr io.Writer) int {
	url, err := readyzURL(addr)
	if err != nil {
		fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return 1
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(stderr, "healthcheck: %s returned %d: %s\n", url, resp.StatusCode, body)
		return 1
	}
	return 0
}
