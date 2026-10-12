package obs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/expfmt"
)

// Batch jobs (grouping key job=...) and the job-specific gauges they may push.
const (
	JobPublisher = "etl_publisher"
	JobRetention = "etl_retention"
	JobVacuum    = "etl_vacuum"

	MetricPublishRecords             = "etl_publish_records"
	MetricPublishDLQLines            = "etl_publish_dlq_lines"
	MetricRetentionPartitionsDropped = "etl_retention_partitions_dropped"
	MetricRetentionBatchesDeleted    = "etl_retention_batches_deleted"
)

// jobGaugeHelp is the only allowed set of job-specific gauges. HELP strings are
// constants: the Pushgateway rejects a push whose HELP or TYPE for a metric name
// differs from another group's.
var jobGaugeHelp = map[string]string{
	MetricPublishRecords:             "Records the last successful publish run sent to the broker.",
	MetricPublishDLQLines:            "Raw lines the last successful publish run wrote to the publish DLQ.",
	MetricRetentionPartitionsDropped: "dedup_claims partitions the last retention run dropped.",
	MetricRetentionBatchesDeleted:    "Written consumer_batches rows the last retention run deleted.",
}

// pushTimeout bounds one push, so a dead Pushgateway cannot hold a job.
const pushTimeout = 10 * time.Second

// JobRun is the outcome of one batch-job run. Gauges are the job-specific
// metrics the job body returned: etl publish returns them only on success,
// retention also on failure (what it really removed).
type JobRun struct {
	Job      string // JobPublisher | JobRetention | JobVacuum (grouping key job=...)
	Started  time.Time
	Finished time.Time
	Err      error              // nil = success
	Gauges   map[string]float64 // names above; nil = none
}

// PushJobRun sends run to the Pushgateway at baseURL with POST (push "Add"):
// only the metric names in this push are replaced in group {job=run.Job}, so a
// failed run, which omits etl_job_last_success_timestamp_seconds, keeps the
// last success. Unknown gauge names or an empty Job are rejected before any
// HTTP call. The metrics carry no labels: the grouping key is the job only.
func PushJobRun(ctx context.Context, baseURL string, run JobRun) error {
	if run.Job == "" {
		return errors.New("obs: push: empty job name")
	}
	for name := range run.Gauges {
		if _, ok := jobGaugeHelp[name]; !ok {
			return fmt.Errorf("obs: push: unknown job gauge %q", name)
		}
	}
	reg := prometheus.NewRegistry() // never the process registry
	gauge := func(name, help string, v float64) {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		g.Set(v)
		reg.MustRegister(g)
	}
	finished := float64(run.Finished.UnixMilli()) / 1000
	success := 0.0
	if run.Err == nil {
		success = 1
		gauge("etl_job_last_success_timestamp_seconds", "Unix time the last successful run of this job finished.", finished)
	}
	gauge("etl_job_last_run_timestamp_seconds", "Unix time the last run of this job finished.", finished)
	gauge("etl_job_duration_seconds", "Wall time of the last run of this job.", run.Finished.Sub(run.Started).Seconds())
	gauge("etl_job_success", "1 if the last run of this job succeeded, 0 if it failed.", success)
	for name, v := range run.Gauges {
		gauge(name, jobGaugeHelp[name], v)
	}
	err := push.New(baseURL, run.Job).Gatherer(reg).
		Client(&http.Client{Timeout: pushTimeout}).
		Format(expfmt.NewFormat(expfmt.TypeTextPlain)).
		AddContext(ctx)
	if err != nil {
		return fmt.Errorf("obs: push %s: %w", run.Job, err)
	}
	return nil
}

// ReportJobRun pushes run with a 10 s timeout on context.WithoutCancel(ctx), so
// a run stopped by SIGTERM still reports, and logs a failure at ERROR
// ("pushgateway push failed", job, url, error). It never changes the job's outcome.
func ReportJobRun(ctx context.Context, log *slog.Logger, baseURL string, run JobRun) {
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pushTimeout)
	defer cancel()
	if err := PushJobRun(pctx, baseURL, run); err != nil {
		log.Error("pushgateway push failed", "job", run.Job, "url", baseURL, "error", err)
	}
}
