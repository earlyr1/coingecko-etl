package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/publisher"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/window"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"
)

// publishTimeout stays below Cronicle's 5-minute job timeout, so a stuck S3 or
// broker still produces a "job failed" line and a success=0 push before the kill.
const publishTimeout = 4 * time.Minute

// runPublish is `etl publish [--now <unix seconds|RFC3339>]`: it publishes the
// window ending at floor(now - 1m, ETL_WINDOW). Exit 2 is a usage error.
func runPublish(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	fs.SetOutput(stderr)
	nowArg := fs.String("now", "", "logical run time: unix seconds or RFC3339 (default: current time)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "etl publish: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	now := time.Now()
	set := false
	fs.Visit(func(f *flag.Flag) { set = set || f.Name == "now" })
	if set { // --now "" (JOB_NOW unset) is an error, not "use the clock"
		t, err := parseNow(*nowArg)
		if err != nil {
			fmt.Fprintf(stderr, "etl publish: %v\n", err)
			return 2
		}
		now = t
	}
	return runJob(config.CmdPublish, "publisher", obs.JobPublisher, stderr, func(ctx context.Context, cfg config.Config, log *slog.Logger) (map[string]float64, error) {
		ctx, cancel := context.WithTimeout(ctx, publishTimeout)
		defer cancel()
		w := window.ForPublish(now, cfg.Window)
		log = log.With("window_start", xtime.Format(w.Start), "window_end", xtime.Format(w.End))
		log.Info("publish started", "now", xtime.Format(now), "window", cfg.Window.String())
		res, err := publishWindow(ctx, cfg, w, log)
		if errors.Is(err, publisher.ErrNotReady) {
			log.Error("window not ready", "success_key", window.RawSuccessKey(w), "error", err)
		}
		if err != nil {
			return nil, err // gauges only on success
		}
		log.Info("window published", "parts", res.Parts, "lines", res.Lines, "records", res.Records, "dlq", res.DLQLines)
		return map[string]float64{obs.MetricPublishRecords: float64(res.Records), obs.MetricPublishDLQLines: float64(res.DLQLines)}, nil
	})
}

// publishWindow builds the clients of one run (S3, schemas, producer; no Postgres) and publishes w.
func publishWindow(ctx context.Context, cfg config.Config, w window.Window, log *slog.Logger) (publisher.Result, error) {
	s3c, err := s3.New(ctx, s3.Config{Endpoint: cfg.S3Endpoint, AccessKey: cfg.S3AccessKey, SecretKey: cfg.S3SecretKey, Region: cfg.S3Region})
	if err != nil {
		return publisher.Result{}, err
	}
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		return publisher.Result{}, fmt.Errorf("load schemas: %w", err)
	}
	prod, err := queue.NewProducer(cfg.Brokers, "etl-publisher")
	if err != nil {
		return publisher.Result{}, fmt.Errorf("producer: %w", err)
	}
	defer prod.Close()
	return publisher.PublishWindow(ctx, publisher.Deps{S3: s3c, Producer: prod, Reg: reg, Topic: cfg.Topic, Log: log}, w)
}

// parseNow accepts integer unix seconds (Cronicle's $JOB_NOW) or RFC 3339; the result is UTC.
func parseNow(s string) (time.Time, error) {
	if s != "" && strings.Trim(s, "0123456789") == "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return time.Unix(n, 0).UTC(), nil
		}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("--now %q: want unix seconds or RFC3339", s)
}
