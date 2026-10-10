package obs

import (
	"reflect"
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewMetricsRegistersCatalogue(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	// Vectors only appear in Gather once a child exists, so touch every metric.
	m.APIRequests.WithLabelValues("coins_markets", "200").Inc()
	m.APIRequestDuration.WithLabelValues("coins_markets").Observe(0.2)
	m.APIRateLimited.Inc()
	m.LastSuccess.WithLabelValues("coins_markets").SetToCurrentTime()
	m.SourceDataAge.WithLabelValues("coins_markets").Set(12)
	m.SchemaValidations.WithLabelValues("transformer", "coins_markets.v1", "pass").Inc()
	m.SchemaViolations.WithLabelValues("transformer", "coins_markets.v1", "required").Inc()
	m.ExportLag.Set(3)
	m.ExportBatches.WithLabelValues("ok").Inc()
	m.S3Puts.WithLabelValues("created").Inc()
	m.PublishedRecords.Inc()
	m.Records.WithLabelValues("market_snapshot", "written").Inc()
	m.DedupDropped.WithLabelValues("replay").Inc()
	m.Rejected.WithLabelValues("empty_record").Inc()
	m.DLQ.WithLabelValues("transform", "schema").Inc()
	m.ConsumerLag.WithLabelValues("0").Set(5)
	m.BatchDuration.Observe(0.1)
	m.E2ELatency.Observe(20)

	want := map[string][]string{
		"etl_api_requests_total":             {"endpoint", "status"},
		"etl_api_request_duration_seconds":   {"endpoint"},
		"etl_api_rate_limited_total":         nil,
		"etl_last_success_timestamp_seconds": {"endpoint"},
		"etl_source_data_age_seconds":        {"endpoint"},
		"etl_schema_validations_total":       {"result", "schema", "stage"},
		"etl_schema_violations_total":        {"keyword", "schema", "stage"},
		"etl_export_lag_seconds":             nil,
		"etl_export_batches_total":           {"result"},
		"etl_s3_put_total":                   {"result"},
		"etl_published_records_total":        nil,
		"etl_records_total":                  {"dataset", "outcome"},
		"etl_dedup_dropped_total":            {"reason"},
		"etl_rejected_total":                 {"reason"},
		"etl_dlq_total":                      {"error_class", "stage"},
		"etl_consumer_lag_records":           {"partition"},
		"etl_batch_duration_seconds":         nil,
		"etl_e2e_latency_seconds":            nil,
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := map[string][]string{}
	for _, f := range families {
		var labels []string
		for _, l := range f.GetMetric()[0].GetLabel() {
			labels = append(labels, l.GetName())
		}
		sort.Strings(labels)
		got[f.GetName()] = labels
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("metric catalogue mismatch:\n got %v\nwant %v", got, want)
	}
}

func TestNewMetricsTwiceOnSameRegistryPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewMetrics(reg)
	defer func() {
		if recover() == nil {
			t.Fatal("second NewMetrics on the same registry did not panic")
		}
	}()
	NewMetrics(reg)
}
