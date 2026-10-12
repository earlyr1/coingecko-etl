package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/obs"
)

// jobFunc is the body of one short batch job. gauges are its job-specific
// metrics (obs.Metric* names); a body returns them when they describe what the
// run really did (etl publish: only on success; retention: also on error).
type jobFunc func(ctx context.Context, cfg config.Config, log *slog.Logger) (gauges map[string]float64, err error)

// runJob is the frame of every scheduled one-shot job: config, logger
// (<ETL_LOG_DIR>/<logService>/etl.log, appended, plus stdout = the Cronicle job
// log), SIGINT/SIGTERM, the body, one Pushgateway report, the exit code
// (0 ok, 1 failed). A config or logger error exits 1 before any push.
func runJob(logService, job string, stderr io.Writer, body jobFunc) int {
	cfg, err := config.Load(logService)
	if err != nil {
		fmt.Fprintf(stderr, "etl %s: %v\n", logService, err)
		return 1
	}
	log, closer, err := obs.NewLogger(logService, logDir(cfg.LogDir))
	if err != nil {
		fmt.Fprintf(stderr, "etl %s: %v\n", logService, err)
		return 1
	}
	defer closer.Close()
	// SIGTERM: Cronicle's abort and timeout, docker compose stop.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log = log.With("job", job)
	log.Info("job started")
	start := time.Now()
	gauges, err := body(ctx, cfg, log)
	fin := time.Now()
	obs.ReportJobRun(ctx, log, cfg.PushgatewayURL, obs.JobRun{Job: job, Started: start, Finished: fin, Err: err, Gauges: gauges})
	if err != nil {
		log.Error("job failed", "error", err, "duration_ms", fin.Sub(start).Milliseconds())
		return 1
	}
	log.Info("job finished", "duration_ms", fin.Sub(start).Milliseconds())
	return 0
}
