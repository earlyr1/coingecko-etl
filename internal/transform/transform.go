package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/validation"
)

// Transform turns one raw event into dataset records. It is pure: the result
// depends only on ev and rules. An error means the payload is not decodable
// (the caller sends the message to dlq/).
func Transform(ev event.RawEvent, rules Rules) ([]Record, error) {
	switch ev.Endpoint {
	case event.EndpointMarkets:
		return transformMarkets(ev, rules)
	case event.EndpointCoinDetail:
		return nil, fmt.Errorf("transform: endpoint %q not implemented", ev.Endpoint)
	default:
		return nil, fmt.Errorf("transform: unknown endpoint %q", ev.Endpoint)
	}
}

// SnapshotAt is the event time of a raw event: max last_updated of a markets
// response, the document's last_updated for /coins/{id}.
func SnapshotAt(ev event.RawEvent) (time.Time, error) {
	switch ev.Endpoint {
	case event.EndpointMarkets:
		rows, err := decodeMarkets(ev.Payload)
		if err != nil {
			return time.Time{}, err
		}
		return marketsSnapshotAt(rows)
	case event.EndpointCoinDetail:
		var doc struct {
			LastUpdated *string `json:"last_updated"`
		}
		if err := decodeJSON(ev.Payload, &doc); err != nil {
			return time.Time{}, fmt.Errorf("transform: decode coin_detail payload: %w", err)
		}
		t, ok, err := parseTime(doc.LastUpdated)
		if err != nil {
			return time.Time{}, err
		}
		if !ok {
			return time.Time{}, fmt.Errorf("transform: coin_detail payload has no last_updated")
		}
		return t, nil
	default:
		return time.Time{}, fmt.Errorf("transform: unknown endpoint %q", ev.Endpoint)
	}
}

// RowJSON serializes one row: the Business fields in struct order followed by
// the lineage fields (_event_id … _processed_at), as a single line without a
// trailing newline.
func RowJSON(r Record, lin Lineage) ([]byte, error) {
	biz, err := json.Marshal(r.Business)
	if err != nil {
		return nil, fmt.Errorf("transform: marshal %s: %w", r.Dataset, err)
	}
	tech, err := json.Marshal(lin)
	if err != nil {
		return nil, fmt.Errorf("transform: marshal lineage: %w", err)
	}
	if len(biz) < 2 || biz[0] != '{' || biz[len(biz)-1] != '}' {
		return nil, fmt.Errorf("transform: business of %s is not a JSON object", r.Dataset)
	}
	var buf bytes.Buffer
	buf.Grow(len(biz) + len(tech))
	buf.Write(biz[:len(biz)-1]) // drop the closing brace
	if len(biz) > 2 {
		buf.WriteByte(',')
	}
	buf.Write(tech[1:]) // drop the opening brace
	return buf.Bytes(), nil
}

// SchemaIDFor maps a processed dataset to its S2 schema $id ("" if unknown).
func SchemaIDFor(dataset string) string {
	switch dataset {
	case DatasetMarketSnapshot:
		return validation.IDMarketSnapshot
	case DatasetCoinSnapshot:
		return validation.IDCoinSnapshot
	case DatasetCoinMarketByCurrency:
		return validation.IDCoinMarketByCurrency
	case DatasetTicker:
		return validation.IDTicker
	case DatasetCoinContract:
		return validation.IDCoinContract
	default:
		return ""
	}
}
