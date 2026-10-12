package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

type pushRequest struct {
	method, path, contentType string
	body                      []byte
}

// pushServer records every request and answers with status.
func pushServer(t *testing.T, status int) (*httptest.Server, func() []pushRequest) {
	t.Helper()
	var (
		mu   sync.Mutex
		reqs []pushRequest
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		reqs = append(reqs, pushRequest{r.Method, r.URL.Path, r.Header.Get("Content-Type"), b})
		mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []pushRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]pushRequest(nil), reqs...)
	}
}

func parseFamilies(t *testing.T, body []byte) map[string]*dto.MetricFamily {
	t.Helper()
	p := expfmt.NewTextParser(model.UTF8Validation)
	mfs, err := p.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("parse pushed body: %v\n%s", err, body)
	}
	return mfs
}

// gaugeValue returns the value of the single, label-free gauge name.
func gaugeValue(t *testing.T, mfs map[string]*dto.MetricFamily, name string) float64 {
	t.Helper()
	mf, ok := mfs[name]
	if !ok {
		t.Fatalf("metric %s not pushed", name)
	}
	if mf.GetType() != dto.MetricType_GAUGE || len(mf.GetMetric()) != 1 {
		t.Fatalf("metric %s: type %v with %d series, want one gauge", name, mf.GetType(), len(mf.GetMetric()))
	}
	m := mf.GetMetric()[0]
	if len(m.GetLabel()) != 0 {
		t.Fatalf("metric %s has labels %v, want none", name, m.GetLabel())
	}
	return m.GetGauge().GetValue()
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

var (
	runStarted  = time.Date(2026, 10, 12, 11, 0, 0, 0, time.UTC)
	runFinished = time.Date(2026, 10, 12, 11, 0, 2, 734_000_000, time.UTC)
)

func TestPushJobRunSuccess(t *testing.T) {
	srv, reqs := pushServer(t, http.StatusOK)
	err := PushJobRun(context.Background(), srv.URL, JobRun{
		Job: JobRetention, Started: runStarted, Finished: runFinished,
		Gauges: map[string]float64{MetricRetentionPartitionsDropped: 3},
	})
	if err != nil {
		t.Fatalf("PushJobRun: %v", err)
	}
	got := reqs()
	if len(got) != 1 {
		t.Fatalf("server saw %d requests, want 1", len(got))
	}
	r := got[0]
	if r.method != http.MethodPost || r.path != "/metrics/job/etl_retention" {
		t.Fatalf("request %s %s, want POST /metrics/job/etl_retention", r.method, r.path)
	}
	if !strings.HasPrefix(r.contentType, "text/plain") {
		t.Fatalf("Content-Type %q, want text/plain", r.contentType)
	}
	mfs := parseFamilies(t, r.body)
	finished := 1791802802.734 // 2026-10-12T11:00:02.734Z
	if v := gaugeValue(t, mfs, "etl_job_success"); v != 1 {
		t.Errorf("etl_job_success = %v, want 1", v)
	}
	if v := gaugeValue(t, mfs, "etl_job_last_run_timestamp_seconds"); !near(v, finished) {
		t.Errorf("etl_job_last_run_timestamp_seconds = %v, want %v", v, finished)
	}
	if v := gaugeValue(t, mfs, "etl_job_last_success_timestamp_seconds"); !near(v, finished) {
		t.Errorf("etl_job_last_success_timestamp_seconds = %v, want %v", v, finished)
	}
	if v := gaugeValue(t, mfs, "etl_job_duration_seconds"); !near(v, 2.734) {
		t.Errorf("etl_job_duration_seconds = %v, want 2.734", v)
	}
	if v := gaugeValue(t, mfs, "etl_retention_partitions_dropped"); v != 3 {
		t.Errorf("etl_retention_partitions_dropped = %v, want 3", v)
	}
	if len(mfs) != 5 {
		t.Errorf("pushed %d metric families, want 5", len(mfs))
	}
}

func TestPushJobRunFailureKeepsLastSuccess(t *testing.T) {
	srv, reqs := pushServer(t, http.StatusOK)
	err := PushJobRun(context.Background(), srv.URL, JobRun{
		Job: JobPublisher, Started: runStarted, Finished: runFinished, Err: errors.New("no _SUCCESS"),
	})
	if err != nil {
		t.Fatalf("PushJobRun: %v", err)
	}
	got := reqs()
	if len(got) != 1 {
		t.Fatalf("server saw %d requests, want 1", len(got))
	}
	if got[0].method != http.MethodPost {
		t.Fatalf("method %s, want POST: PUT would erase the last success", got[0].method)
	}
	if got[0].path != "/metrics/job/etl_publisher" {
		t.Fatalf("path %s, want /metrics/job/etl_publisher", got[0].path)
	}
	mfs := parseFamilies(t, got[0].body)
	if v := gaugeValue(t, mfs, "etl_job_success"); v != 0 {
		t.Errorf("etl_job_success = %v, want 0", v)
	}
	if _, ok := mfs["etl_job_last_success_timestamp_seconds"]; ok {
		t.Error("a failed run pushed etl_job_last_success_timestamp_seconds")
	}
	if v := gaugeValue(t, mfs, "etl_job_last_run_timestamp_seconds"); !near(v, 1791802802.734) {
		t.Errorf("etl_job_last_run_timestamp_seconds = %v", v)
	}
}

func TestPushJobRunRejectsUnknownGauge(t *testing.T) {
	srv, reqs := pushServer(t, http.StatusOK)
	runs := map[string]JobRun{
		"unknown gauge": {Job: JobVacuum, Started: runStarted, Finished: runFinished, Gauges: map[string]float64{"etl_bogus": 1}},
		"empty job":     {Started: runStarted, Finished: runFinished},
	}
	for name, run := range runs {
		if err := PushJobRun(context.Background(), srv.URL, run); err == nil {
			t.Errorf("%s: PushJobRun succeeded, want an error", name)
		}
	}
	if n := len(reqs()); n != 0 {
		t.Fatalf("server saw %d requests, want none", n)
	}
}

func TestPushJobRunHTTPError(t *testing.T) {
	srv, _ := pushServer(t, http.StatusInternalServerError)
	err := PushJobRun(context.Background(), srv.URL, JobRun{Job: JobVacuum, Started: runStarted, Finished: runFinished})
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("PushJobRun = %v, want an error naming 500", err)
	}
}

func TestReportJobRunLogsAndSurvivesCancelledContext(t *testing.T) {
	srv, reqs := pushServer(t, http.StatusOK)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the job was stopped by SIGTERM: its context is already done
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	ReportJobRun(ctx, log, srv.URL, JobRun{Job: JobVacuum, Started: runStarted, Finished: runFinished, Err: context.Canceled})
	if n := len(reqs()); n != 1 {
		t.Fatalf("server saw %d requests after a cancelled context, want 1", n)
	}
	if buf.Len() != 0 {
		t.Fatalf("a successful push logged: %s", buf.String())
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := "http://" + ln.Addr().String()
	ln.Close()
	ReportJobRun(context.Background(), log, closed, JobRun{Job: JobVacuum, Started: runStarted, Finished: runFinished})
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("logged %d records, want 1:\n%s", len(lines), buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("log record %q: %v", lines[0], err)
	}
	if rec["level"] != "ERROR" || rec["msg"] != "pushgateway push failed" || rec["job"] != "etl_vacuum" || rec["url"] != closed {
		t.Fatalf("log record %v, want ERROR pushgateway push failed job=etl_vacuum url=%s", rec, closed)
	}
	if e, _ := rec["error"].(string); e == "" {
		t.Fatalf("log record %v has no error", rec)
	}
}

// The Pushgateway refuses a push whose HELP or TYPE for a metric name differs
// from what another group already holds.
func TestPushHelpStringsStable(t *testing.T) {
	srv, reqs := pushServer(t, http.StatusOK)
	for _, run := range []JobRun{
		{Job: JobRetention, Started: runStarted, Finished: runFinished, Gauges: map[string]float64{MetricRetentionBatchesDeleted: 2}},
		{Job: JobVacuum, Started: runStarted, Finished: runFinished.Add(time.Hour), Err: errors.New("boom")},
	} {
		if err := PushJobRun(context.Background(), srv.URL, run); err != nil {
			t.Fatalf("PushJobRun(%s): %v", run.Job, err)
		}
	}
	got := reqs()
	if len(got) != 2 {
		t.Fatalf("server saw %d requests, want 2", len(got))
	}
	meta := func(body []byte) map[string]bool {
		out := map[string]bool{}
		for _, l := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(l, "# HELP etl_job_") || strings.HasPrefix(l, "# TYPE etl_job_") {
				out[l] = true
			}
		}
		return out
	}
	a, b := meta(got[0].body), meta(got[1].body)
	for _, name := range []string{"etl_job_success", "etl_job_last_run_timestamp_seconds", "etl_job_duration_seconds"} {
		var help string
		for l := range a {
			if strings.HasPrefix(l, "# HELP "+name+" ") {
				help = l
			}
		}
		if help == "" || !b[help] {
			t.Errorf("HELP of %s differs between jobs: %q", name, help)
		}
	}
}
