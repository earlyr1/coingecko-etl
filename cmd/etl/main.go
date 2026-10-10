// Command etl is the single binary of the pipeline. The first argument selects
// the subcommand; see usage below.
package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

const usageText = `usage: etl <command>

commands:
  init         run migrations, create partitions, topic and buckets (one-shot)
  fetcher      poll CoinGecko into raw.api_calls
  exporter     export raw.api_calls to s3://raw as JSONL
  publisher    publish exported batches to the broker
  transformer  consume, clean, dedup and write s3://processed
  healthcheck  exit 0 if the local /readyz returns 200 (for container healthchecks)
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches a subcommand and returns the process exit code.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageText)
		return 2
	}
	switch args[0] {
	case "healthcheck":
		return healthcheck(os.Getenv("ETL_HTTP_ADDR"), stderr)
	case "init", "fetcher", "exporter", "publisher", "transformer":
		fmt.Fprintf(stderr, "etl %s: not implemented\n", args[0])
		return 1
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usageText)
		return 0
	default:
		fmt.Fprintf(stderr, "etl: unknown command %q\n%s", args[0], usageText)
		return 2
	}
}

// readyzURL turns a listen address such as ":8080" or "0.0.0.0:8080" into the
// loopback URL of /readyz. An empty addr means the default ":8080".
func readyzURL(addr string) (string, error) {
	if addr == "" {
		addr = ":8080"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("bad ETL_HTTP_ADDR %q: %w", addr, err)
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/readyz", nil
}

// healthcheck is used by Docker healthchecks: the distroless image has no curl.
// The 3 s client timeout exceeds the 2 s readiness timeout of /readyz.
func healthcheck(addr string, stderr io.Writer) int {
	url, err := readyzURL(addr)
	if err != nil {
		fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return 1
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(stderr, "healthcheck: %s returned %d: %s\n", url, resp.StatusCode, body)
		return 1
	}
	return 0
}
