package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics is the full etl_* catalogue (spec §8.2). Every service registers all of
// them; a service only moves the ones it owns, the others stay absent or at zero.
type Metrics struct {
	APIRequests        *prometheus.CounterVec   // etl_api_requests_total{endpoint,status}
	APIRequestDuration *prometheus.HistogramVec // etl_api_request_duration_seconds{endpoint}
	APIRateLimited     prometheus.Counter       // etl_api_rate_limited_total
	LastSuccess        *prometheus.GaugeVec     // etl_last_success_timestamp_seconds{endpoint}
	SourceDataAge      *prometheus.GaugeVec     // etl_source_data_age_seconds{endpoint}
	SchemaValidations  *prometheus.CounterVec   // etl_schema_validations_total{stage,schema,result}
	SchemaViolations   *prometheus.CounterVec   // etl_schema_violations_total{stage,schema,keyword}
	ExportLag          prometheus.Gauge         // etl_export_lag_seconds
	ExportBatches      *prometheus.CounterVec   // etl_export_batches_total{result}
	S3Puts             *prometheus.CounterVec   // etl_s3_put_total{result}  result=created|exists|error|mismatch
	Records            *prometheus.CounterVec   // etl_records_total{dataset,outcome}
	DedupDropped       *prometheus.CounterVec   // etl_dedup_dropped_total{reason}
	Rejected           *prometheus.CounterVec   // etl_rejected_total{reason}
	DLQ                *prometheus.CounterVec   // etl_dlq_total{stage,error_class}
	ConsumerLag        *prometheus.GaugeVec     // etl_consumer_lag_records{partition}
	BatchDuration      prometheus.Histogram     // etl_batch_duration_seconds
	E2ELatency         prometheus.Histogram     // etl_e2e_latency_seconds
}

// NewMetrics creates and registers every metric on reg (panics on duplicate
// registration, like promauto). Pass a fresh prometheus.NewRegistry() per process;
// Go runtime / process collectors are registered by the caller (cmd/etl).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		APIRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_api_requests_total",
			Help: "CoinGecko requests by endpoint and status: HTTP code, network_error, or skipped (tick skipped: limiter cooldown or a request still in flight).",
		}, []string{"endpoint", "status"}),
		APIRequestDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "etl_api_request_duration_seconds",
			Help:    "CoinGecko HTTP call latency, including body read.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
		}, []string{"endpoint"}),
		APIRateLimited: f.NewCounter(prometheus.CounterOpts{
			Name: "etl_api_rate_limited_total",
			Help: "CoinGecko responses with HTTP 429.",
		}),
		LastSuccess: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "etl_last_success_timestamp_seconds",
			Help: "Unix time of the last successful (outcome ok) call per endpoint.",
		}, []string{"endpoint"}),
		SourceDataAge: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "etl_source_data_age_seconds",
			Help: "fetched_at minus the newest last_updated in the response, per endpoint.",
		}, []string{"endpoint"}),
		SchemaValidations: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_schema_validations_total",
			Help: "JSON Schema validations by stage, schema and result (pass|fail).",
		}, []string{"stage", "schema", "result"}),
		SchemaViolations: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_schema_violations_total",
			Help: "JSON Schema violations by stage, schema and failing keyword.",
		}, []string{"stage", "schema", "keyword"}),
		ExportLag: f.NewGauge(prometheus.GaugeOpts{
			Name: "etl_export_lag_seconds",
			Help: "Seconds since the end of the oldest ETL_WINDOW window the exporter has not closed yet (0 while on schedule).",
		}),
		ExportBatches: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_export_batches_total",
			Help: "Exporter raw parts by result (exported, error); error also counts every failed window step of a tick (ledger reads, a refused ETL_WINDOW or lake, open, claim, seal, _SUCCESS).",
		}, []string{"result"}),
		S3Puts: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_s3_put_total",
			Help: "Conditional S3 PUTs by result: created, exists (412, same bytes), mismatch (412, different bytes), error.",
		}, []string{"result"}),
		Records: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_records_total",
			Help: "Transformer records by dataset and outcome (written, duplicate, rejected, dlq).",
		}, []string{"dataset", "outcome"}),
		DedupDropped: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_dedup_dropped_total",
			Help: "Records dropped by deduplication, by reason (replay, duplicate_response, duplicate_record).",
		}, []string{"reason"}),
		Rejected: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_rejected_total",
			Help: "Records written to the rejected zone, by reason.",
		}, []string{"reason"}),
		DLQ: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_dlq_total",
			Help: "Records written to the dead-letter zone, by stage and error class.",
		}, []string{"stage", "error_class"}),
		ConsumerLag: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "etl_consumer_lag_records",
			Help: "High watermark minus committed offset, per assigned partition.",
		}, []string{"partition"}),
		BatchDuration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "etl_batch_duration_seconds",
			Help:    "Transformer batch processing time (TX1, PUTs, TX2).",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms .. ~20s
		}),
		// The latency includes the wait for the raw window to close (up to
		// ETL_WINDOW) and the publish grace: the buckets are dense up to 10
		// minutes, so a p95 is not interpolated across one wide bucket.
		E2ELatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "etl_e2e_latency_seconds",
			Help:    "From fetched_at to the processed object being written.",
			Buckets: []float64{5, 15, 30, 60, 120, 180, 240, 300, 360, 420, 480, 600, 900, 1800, 3600},
		}),
	}
}
