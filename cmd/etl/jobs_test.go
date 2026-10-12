package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestRunJobReportsFailureOnSIGTERM runs `etl publish` as a child process
// whose S3 never answers, stops it with SIGTERM (Cronicle's abort and
// timeout, docker compose stop) and expects the failure to be logged and
// pushed, and exit 1, well inside Cronicle's 10 s kill grace.
func TestRunJobReportsFailureOnSIGTERM(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "etl")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // S3 never answers: the run blocks in the _SUCCESS GET
	}))
	defer hang.Close()
	var mu sync.Mutex
	var pushes []string // "METHOD PATH\nBODY"
	pgw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		pushes = append(pushes, r.Method+" "+r.URL.Path+"\n"+string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer pgw.Close()

	logDir := t.TempDir()
	cmd := exec.Command(bin, "publish", "--now", "1791813960")
	cmd.Env = append(os.Environ(), "ETL_LOG_DIR="+logDir, "ETL_LOG_PER_HOST=", "ETL_S3_ENDPOINT="+hang.URL,
		"ETL_BROKERS=127.0.0.1:1", "ETL_PUSHGATEWAY_URL="+pgw.URL, "ETL_WINDOW=5m")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	reaped := false
	defer func() {
		if !reaped {
			_ = cmd.Process.Kill()
			<-exited
		}
	}()

	logFile := filepath.Join(logDir, "publisher", "etl.log")
	readLog := func() string {
		b, _ := os.ReadFile(logFile)
		return string(b)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(readLog(), `"msg":"publish started"`) {
		if time.Now().After(deadline) {
			t.Fatalf("no publish started line within 5 s:\n%s", readLog())
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The signal handler is installed before "job started", so SIGTERM is
	// caught by the job instead of killing the process.
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		reaped = true
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 {
			t.Fatalf("exit after SIGTERM: %v; want exit code 1", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("etl publish did not exit within 5 s of SIGTERM")
	}

	logged := readLog()
	for _, want := range []string{`"msg":"job failed"`, `"job":"etl_publisher"`} {
		if !strings.Contains(logged, want) {
			t.Errorf("log lacks %s:\n%s", want, logged)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(pushes) != 1 {
		t.Fatalf("got %d pushes, want 1: %q", len(pushes), pushes)
	}
	if !strings.HasPrefix(pushes[0], "POST /metrics/job/etl_publisher\n") ||
		!strings.Contains(pushes[0], "\netl_job_success 0\n") ||
		strings.Contains(pushes[0], "etl_job_last_success_timestamp_seconds") {
		t.Fatalf("push = %q; want a POST with etl_job_success 0 and no last-success timestamp", pushes[0])
	}
}
