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
	"coingecko-etl/internal/maintenance"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/storage/pg"
)

// jobFunc is the body of one short batch job. gauges are its job-specific
// metrics (obs.Metric* names); a body returns them when they describe what the
// run really did (etl publish: only on success; retention: also on error).
type jobFunc func(ctx context.Context, cfg config.Config, log *slog.Logger) (gauges map[string]float64, err error)

// runJob is the frame of every scheduled one-shot job: config (validated for
// cmd, the command as typed: "publish", "maintenance retention", ...), logger
// (<ETL_LOG_DIR>/<logService>/etl.log, appended, plus stdout = the Cronicle job
// log), SIGINT/SIGTERM, the body, one Pushgateway report, the exit code
// (0 ok, 1 failed). A config or logger error exits 1 before any push.
func runJob(cmd, logService, job string, stderr io.Writer, body jobFunc) int {
	cfg, err := config.Load(cmd)
	if err != nil {
		fmt.Fprintf(stderr, "etl %s: %v\n", cmd, err)
		return 1
	}
	log, closer, err := obs.NewLogger(logService, logDir(cfg.LogDir))
	if err != nil {
		fmt.Fprintf(stderr, "etl %s: %v\n", cmd, err)
		return 1
	}
	defer closer.Close()
	// SIGTERM: Cronicle's abort and timeout, docker compose stop.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// ReportJobRun adds "job" itself: give it the logger without it, so its
	// record carries the attribute once.
	base := log
	log = log.With("job", job)
	log.Info("job started")
	start := time.Now()
	gauges, err := body(ctx, cfg, log)
	fin := time.Now()
	obs.ReportJobRun(ctx, base, cfg.PushgatewayURL, obs.JobRun{Job: job, Started: start, Finished: fin, Err: err, Gauges: gauges})
	if err != nil {
		log.Error("job failed", "error", err, "duration_ms", fin.Sub(start).Milliseconds())
		return 1
	}
	log.Info("job finished", "duration_ms", fin.Sub(start).Milliseconds())
	return 0
}

// runMaintenance runs one daily Postgres job: `etl maintenance retention` or
// `etl maintenance vacuum`. Both log to <ETL_LOG_DIR>/maintenance/etl.log
// (the job attribute tells them apart) and exit 0 ok, 1 failed, 2 usage.
func runMaintenance(args []string, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprint(stderr, usageText)
		return 2
	}
	switch args[0] {
	case "retention":
		return runJob(config.CmdRetention, "maintenance", obs.JobRetention, stderr, retentionJob)
	case "vacuum":
		return runJob(config.CmdVacuum, "maintenance", obs.JobVacuum, stderr, vacuumJob)
	}
	fmt.Fprintf(stderr, "etl: unknown maintenance job %q\n%s", args[0], usageText)
	return 2
}

// retentionJob returns its gauges also on error: they count what really was removed.
func retentionJob(ctx context.Context, cfg config.Config, log *slog.Logger) (map[string]float64, error) {
	gauges := func(r maintenance.RetentionResult) map[string]float64 {
		return map[string]float64{
			obs.MetricRetentionPartitionsDropped: float64(len(r.DroppedPartitions)),
			obs.MetricRetentionBatchesDeleted:    float64(r.BatchesDeleted),
		}
	}
	cctx, cancel := context.WithTimeout(ctx, startupTimeout)
	pool, err := pg.Connect(cctx, cfg.PostgresDSN)
	cancel()
	if err != nil {
		return gauges(maintenance.RetentionResult{}), err
	}
	defer pool.Close()
	res, err := maintenance.Retention(ctx, pool, time.Now(), maintenance.DefaultRetentionOptions(cfg.ClaimsRetentionDays), log)
	return gauges(res), err
}

func vacuumJob(ctx context.Context, cfg config.Config, log *slog.Logger) (map[string]float64, error) {
	cctx, cancel := context.WithTimeout(ctx, startupTimeout)
	pool, err := pg.Connect(cctx, cfg.PostgresDSN)
	cancel()
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	return nil, maintenance.Vacuum(ctx, pool, log)
}
