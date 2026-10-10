package main

import (
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunDispatch(t *testing.T) {
	cases := []struct {
		args       []string
		wantCode   int
		wantStderr string
	}{
		{nil, 2, "usage: etl"},
		{[]string{"bogus"}, 2, `unknown command "bogus"`},
		{[]string{"init"}, 1, "etl init: not implemented"},
		{[]string{"fetcher"}, 1, "etl fetcher: not implemented"},
		{[]string{"exporter"}, 1, "etl exporter: not implemented"},
		{[]string{"publisher"}, 1, "etl publisher: not implemented"},
		{[]string{"transformer"}, 1, "etl transformer: not implemented"},
		{[]string{"help"}, 0, ""},
	}
	for _, c := range cases {
		t.Run(strings.Join(c.args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(c.args, &stdout, &stderr); code != c.wantCode {
				t.Fatalf("exit code %d, want %d (stderr %q)", code, c.wantCode, stderr.String())
			}
			if !strings.Contains(stderr.String(), c.wantStderr) {
				t.Fatalf("stderr %q does not contain %q", stderr.String(), c.wantStderr)
			}
		})
	}
}

func TestReadyzURL(t *testing.T) {
	cases := map[string]string{
		"":               "http://127.0.0.1:8080/readyz",
		":8080":          "http://127.0.0.1:8080/readyz",
		"0.0.0.0:9000":   "http://127.0.0.1:9000/readyz",
		"[::]:8080":      "http://127.0.0.1:8080/readyz",
		"10.0.0.5:8080":  "http://10.0.0.5:8080/readyz",
		"localhost:8081": "http://localhost:8081/readyz",
	}
	for in, want := range cases {
		got, err := readyzURL(in)
		if err != nil || got != want {
			t.Errorf("readyzURL(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := readyzURL("8080"); err == nil {
		t.Error(`readyzURL("8080") succeeded, want error`)
	}
}

func TestHealthcheckCommand(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()
	t.Setenv("ETL_HTTP_ADDR", srv.Listener.Addr().String())

	var stderr bytes.Buffer
	if code := run([]string{"healthcheck"}, &bytes.Buffer{}, &stderr); code != 0 {
		t.Fatalf("ready server: exit %d (%s), want 0", code, stderr.String())
	}
	status = http.StatusServiceUnavailable
	if code := run([]string{"healthcheck"}, &bytes.Buffer{}, &stderr); code != 1 {
		t.Fatalf("not-ready server: exit %d, want 1", code)
	}
}

func TestHealthcheckUnreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there any more
	t.Setenv("ETL_HTTP_ADDR", addr)
	if code := run([]string{"healthcheck"}, &bytes.Buffer{}, &bytes.Buffer{}); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
}
