package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestIsPreconditionFailed(t *testing.T) {
	byCode := fmt.Errorf("wrapped: %w", &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "At least one of the pre-conditions you specified did not hold"})
	byStatus := &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 412}},
		Err:      errors.New("boom"),
	}}
	other := &smithy.GenericAPIError{Code: "AccessDenied"}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"error code", byCode, true},
		{"http status", byStatus, true},
		{"other api error", other, false},
		{"plain error", errors.New("dial tcp: refused"), false},
	}
	for _, c := range cases {
		if got := isPreconditionFailed(c.err); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestIsBucketExists(t *testing.T) {
	if !isBucketExists(&smithy.GenericAPIError{Code: "BucketAlreadyOwnedByYou"}) {
		t.Error("BucketAlreadyOwnedByYou not recognised")
	}
	if !isBucketExists(&smithy.GenericAPIError{Code: "BucketAlreadyExists"}) {
		t.Error("BucketAlreadyExists not recognised")
	}
	if isBucketExists(&smithy.GenericAPIError{Code: "AccessDenied"}) {
		t.Error("AccessDenied treated as exists")
	}
}

func TestMetaValueCaseInsensitive(t *testing.T) {
	for _, k := range []string{"sha256", "Sha256", "SHA256"} {
		v, ok := metaValue(map[string]string{k: "abc"}, "sha256")
		if !ok || v != "abc" {
			t.Errorf("key %q: got %q %v", k, v, ok)
		}
	}
	if _, ok := metaValue(map[string]string{"other": "x"}, "sha256"); ok {
		t.Error("missing key reported present")
	}
}

// hangingListener accepts TCP connections, reads whatever arrives and never
// answers: a gateway that hangs rather than being down.
func hangingListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			go io.Copy(io.Discard, c)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	return "http://" + ln.Addr().String()
}

// stallingBodyServer sends 200 headers that promise a body and then stalls.
func stallingBodyServer(t *testing.T) string {
	t.Helper()
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(stop) }) // runs first: lets the handler return
	return srv.URL
}

// TestRequestsTimeOutWhenTheGatewayHangs: no caller sets a deadline, so the
// client's own timeouts are what stops a hung gateway.
func TestRequestsTimeOutWhenTheGatewayHangs(t *testing.T) {
	newHungClient := func(endpoint string) *Client {
		o := options(Config{Endpoint: endpoint, AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1",
			Timeout: 200 * time.Millisecond, HeaderTimeout: 200 * time.Millisecond})
		o.RetryMaxAttempts = 1 // measure the timeout, not the SDK's retry backoff
		return &Client{api: s3.New(o)}
	}
	put := func(c *Client) error {
		_, err := c.PutIfAbsent(context.Background(), BucketRaw, "k.jsonl", []byte("{}\n"))
		return err
	}
	get := func(c *Client) error {
		_, err := c.Get(context.Background(), BucketRaw, "k.jsonl")
		return err
	}

	cases := []struct {
		name     string
		endpoint string
		calls    map[string]func(*Client) error
	}{
		{"no response", hangingListener(t), map[string]func(*Client) error{"PutIfAbsent": put, "Get": get}},
		{"stalled body", stallingBodyServer(t), map[string]func(*Client) error{"Get": get}},
	}
	for _, tc := range cases {
		c := newHungClient(tc.endpoint)
		for name, call := range tc.calls {
			start := time.Now()
			err := call(c)
			if err == nil {
				t.Fatalf("%s, %s: no error", tc.name, name)
			}
			if d := time.Since(start); d > 2*time.Second {
				t.Fatalf("%s, %s: returned after %v, want well within 2s", tc.name, name, d)
			}
		}
	}
}

func TestNewSetsDefaultTimeouts(t *testing.T) {
	c, err := New(context.Background(), Config{Endpoint: "http://127.0.0.1:7070", AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	hc, ok := c.api.Options().HTTPClient.(*awshttp.BuildableClient)
	if !ok {
		t.Fatalf("HTTP client is %T, want *awshttp.BuildableClient", c.api.Options().HTTPClient)
	}
	if got := hc.GetTimeout(); got != DefaultTimeout {
		t.Errorf("timeout = %v, want %v", got, DefaultTimeout)
	}
	if got := hc.GetTransport().ResponseHeaderTimeout; got != DefaultHeaderTimeout {
		t.Errorf("response header timeout = %v, want %v", got, DefaultHeaderTimeout)
	}
}

// TestNewIgnoresHostAWSEnvironment: the client is configured from Config
// alone, so a developer's AWS profile or endpoint variables cannot break or
// redirect it.
func TestNewIgnoresHostAWSEnvironment(t *testing.T) {
	t.Setenv("AWS_PROFILE", "does-not-exist")
	t.Setenv("AWS_REGION", "eu-west-3")
	t.Setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:1")
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:2")
	c, err := New(context.Background(), Config{Endpoint: "http://versitygw:7070", AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1"})
	if err != nil {
		t.Fatalf("New with AWS_PROFILE=does-not-exist: %v", err)
	}
	o := c.api.Options()
	if o.BaseEndpoint == nil || *o.BaseEndpoint != "http://versitygw:7070" || o.Region != "us-east-1" || !o.UsePathStyle {
		t.Fatalf("options: endpoint=%v region=%q path-style=%v, want http://versitygw:7070, us-east-1, true", o.BaseEndpoint, o.Region, o.UsePathStyle)
	}
}
