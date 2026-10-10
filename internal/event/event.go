// Package event defines the raw event envelope (schema S1, pipeline/raw_event.v1):
// one RawEvent per CoinGecko HTTP call. The same bytes travel Postgres -> raw JSONL
// -> broker, so serialization is deterministic and the payload is never re-encoded.
package event

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	SchemaVersion       = 1
	SourceCoinGecko     = "coingecko"
	EndpointMarkets     = "coins_markets"
	EndpointCoinDetail  = "coin_detail"
	OutcomeOK           = "ok"
	OutcomeHTTPError    = "http_error"
	OutcomeNetworkError = "network_error"
)

type Request struct {
	Path   string            `json:"path"`
	Params map[string]string `json:"params"`
}

type HTTPMeta struct {
	Status        int     `json:"status"`
	LatencyMS     int64   `json:"latency_ms"`
	Date          *string `json:"date"`
	AgeSeconds    *int    `json:"age_s"`
	CFCacheStatus *string `json:"cf_cache_status"`
	ETag          *string `json:"etag"`
	RetryAfterS   *int    `json:"retry_after_s"`
}

type RawEvent struct {
	SchemaVersion  int             `json:"schema_version"`
	EventID        string          `json:"event_id"`        // UUIDv7
	IdempotencyKey *string         `json:"idempotency_key"` // nil unless outcome ok
	Source         string          `json:"source"`
	Endpoint       string          `json:"endpoint"`
	Request        Request         `json:"request"`
	FetchedAt      string          `json:"fetched_at"` // xtime.Format
	Outcome        string          `json:"outcome"`
	HTTP           *HTTPMeta       `json:"http"`
	Error          *string         `json:"error"`
	PayloadSHA256  *string         `json:"payload_sha256"` // hex
	Payload        json.RawMessage `json:"payload"`        // verbatim bytes; JSON null when absent
}

// SHA256Hex returns the lowercase hex SHA-256 of b.
func SHA256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// IdempotencyKey = sha256hex(source "|" endpoint "|" canonical(params) "|" payloadSHA256Hex),
// where canonical(params) is "k=v" pairs sorted by key and joined by "&" (no URL
// escaping). Two calls with the same request and byte-identical body get the same key.
func IdempotencyKey(source, endpoint string, params map[string]string, payloadSHA256Hex string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]string, len(keys))
	for i, k := range keys {
		pairs[i] = k + "=" + params[k]
	}
	return SHA256Hex([]byte(source + "|" + endpoint + "|" + strings.Join(pairs, "&") + "|" + payloadSHA256Hex))
}

// envelope is RawEvent without the payload. encoding/json writes struct fields
// in declaration order and map keys sorted, which makes the output deterministic.
// The payload is appended by hand because json.Marshal would compact (re-encode)
// a json.RawMessage and change its bytes.
type envelope struct {
	SchemaVersion  int       `json:"schema_version"`
	EventID        string    `json:"event_id"`
	IdempotencyKey *string   `json:"idempotency_key"`
	Source         string    `json:"source"`
	Endpoint       string    `json:"endpoint"`
	Request        Request   `json:"request"`
	FetchedAt      string    `json:"fetched_at"`
	Outcome        string    `json:"outcome"`
	HTTP           *HTTPMeta `json:"http"`
	Error          *string   `json:"error"`
	PayloadSHA256  *string   `json:"payload_sha256"`
}

// Marshal serializes ev as one JSON object with fields in declaration order and
// no trailing newline. Payload is embedded byte-for-byte (an empty Payload is
// written as null); it must be valid JSON without CR or LF, so that the event
// stays one JSONL line and one message (the fetcher compacts bodies with line
// breaks). A nil Request.Params is written as {}.
func Marshal(ev RawEvent) ([]byte, error) {
	env := envelope{
		SchemaVersion:  ev.SchemaVersion,
		EventID:        ev.EventID,
		IdempotencyKey: ev.IdempotencyKey,
		Source:         ev.Source,
		Endpoint:       ev.Endpoint,
		Request:        ev.Request,
		FetchedAt:      ev.FetchedAt,
		Outcome:        ev.Outcome,
		HTTP:           ev.HTTP,
		Error:          ev.Error,
		PayloadSHA256:  ev.PayloadSHA256,
	}
	if env.Request.Params == nil {
		env.Request.Params = map[string]string{}
	}
	payload := []byte("null")
	if len(ev.Payload) > 0 {
		if !json.Valid(ev.Payload) {
			return nil, errors.New("event: payload is not valid JSON")
		}
		if bytes.ContainsAny(ev.Payload, "\r\n") {
			return nil, errors.New("event: payload contains a line break (CR or LF); an event must stay on one line")
		}
		payload = ev.Payload
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // keep "<", ">", "&" readable in error texts and URLs
	if err := enc.Encode(env); err != nil {
		return nil, fmt.Errorf("event: marshal envelope: %w", err)
	}
	head := bytes.TrimSuffix(buf.Bytes(), []byte("\n")) // Encode appends a newline
	head = bytes.TrimSuffix(head, []byte("}"))          // reopen the object

	out := make([]byte, 0, len(head)+len(`,"payload":}`)+len(payload))
	out = append(out, head...)
	out = append(out, `,"payload":`...)
	out = append(out, payload...)
	out = append(out, '}')
	return out, nil
}

// Unmarshal parses one serialized RawEvent. Payload keeps the exact bytes of the
// "payload" value (encoding/json copies a json.RawMessage verbatim); a JSON null
// payload becomes a nil Payload.
func Unmarshal(b []byte) (RawEvent, error) {
	var ev RawEvent
	if err := json.Unmarshal(b, &ev); err != nil {
		return RawEvent{}, fmt.Errorf("event: unmarshal: %w", err)
	}
	if bytes.Equal(ev.Payload, []byte("null")) {
		ev.Payload = nil
	}
	return ev, nil
}
