// Package transform holds the pure CoinGecko → processed-record transforms.
// Nothing in this package performs I/O or reads the clock: the same RawEvent
// and Rules always yield byte-identical records.
package transform

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// Zone is where a record is written.
type Zone string

const (
	ZoneProcessed Zone = "processed"
	ZoneRejected  Zone = "rejected"
)

// Dataset names (also the dataset=<name> object-key segment).
const (
	DatasetMarketSnapshot       = "market_snapshot"
	DatasetCoinSnapshot         = "coin_snapshot"
	DatasetCoinMarketByCurrency = "coin_market_by_currency"
	DatasetTicker               = "ticker"
	DatasetCoinContract         = "coin_contract"
)

// Reject reasons produced by the transforms (snapshot_conflict is decided by dedup).
const (
	ReasonMissingID        = "missing_id"
	ReasonNullPrice        = "null_price"
	ReasonNonpositivePrice = "nonpositive_price"
	ReasonEmptyPlatform    = "empty_platform"
	ReasonEmptyRecord      = "empty_record"
)

// Rules are the configurable cleaning thresholds (config.StaleAfter etc.).
type Rules struct {
	StaleAfter, TickerStaleAfter time.Duration
	TickerAnomalyPct             float64
}

// Lineage holds the technical columns appended to every written row.
// They are never part of Business, so they never enter the content hash.
type Lineage struct {
	EventID     string `json:"_event_id"`
	RawObject   string `json:"_raw_object"`
	Partition   int32  `json:"_kafka_partition"`
	Offset      int64  `json:"_kafka_offset"`
	SchemaVer   int    `json:"_schema_version"`
	ProcessedAt string `json:"_processed_at"`
}

// Record is one dataset row produced from one message.
type Record struct {
	Dataset       string // "market_snapshot" | "coin_snapshot" | "coin_market_by_currency" | "ticker" | "coin_contract"
	Key           string // natural key, fields joined by "|"
	EventDate     string // FormatDate(snapshot_at)
	Ordinal       int    // index within the message, assignment order
	Zone          Zone
	Reason        string   // reject reason when Zone == ZoneRejected
	ContentSHA256 [32]byte // over business fields only
	Business      any      // business-field struct (hashed)
}

// Dec is a decimal number serialized as a JSON string in plain notation
// ("0.00004", never "4e-05"). The empty Dec is JSON null.
type Dec string

// MarshalJSON writes null for the empty Dec and a JSON string otherwise.
func (d Dec) MarshalJSON() ([]byte, error) {
	if d == "" {
		return []byte("null"), nil
	}
	return json.Marshal(string(d))
}

// UnmarshalJSON accepts null or a JSON string (used by tests and readers).
func (d *Dec) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*d = ""
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("transform: Dec: %w", err)
	}
	*d = Dec(s)
	return nil
}

// Decimal converts a non-empty Dec to a shopspring decimal.
func (d Dec) Decimal() (decimal.Decimal, bool) {
	if d == "" {
		return decimal.Decimal{}, false
	}
	v, err := decimal.NewFromString(string(d))
	if err != nil {
		return decimal.Decimal{}, false
	}
	return v, true
}

// DecFromNumber converts a JSON number literal (exact text, e.g. from
// json.Decoder.UseNumber) into a plain-notation decimal string without ever
// going through float64. An empty json.Number (JSON null) yields "".
func DecFromNumber(n json.Number) (Dec, error) {
	if n == "" {
		return "", nil
	}
	v, err := decimal.NewFromString(string(n))
	if err != nil {
		return "", fmt.Errorf("transform: not a decimal number %q: %w", string(n), err)
	}
	// String() prints plain notation and drops trailing zeros: "20096053.0" → "20096053".
	return Dec(v.String()), nil
}

// MarketSnapshot is one market_snapshot row (one coin from /coins/markets).
// Field order is the serialized column order. Only business fields: no
// lineage, no fetched_at.
type MarketSnapshot struct {
	CoinID                string   `json:"coin_id"`
	SnapshotAt            string   `json:"snapshot_at"`
	Symbol                *string  `json:"symbol"`
	Name                  *string  `json:"name"`
	VsCurrency            string   `json:"vs_currency"`
	MarketCapRank         *int64   `json:"market_cap_rank"`
	Price                 Dec      `json:"price"`
	MarketCap             Dec      `json:"market_cap"`
	FDV                   Dec      `json:"fdv"`
	TotalVolume           Dec      `json:"total_volume"`
	High24h               Dec      `json:"high_24h"`
	Low24h                Dec      `json:"low_24h"`
	PriceChange24h        Dec      `json:"price_change_24h"`
	PriceChangePct1h      *float64 `json:"price_change_pct_1h"`
	PriceChangePct24h     *float64 `json:"price_change_pct_24h"`
	PriceChangePct7d      *float64 `json:"price_change_pct_7d"`
	MarketCapChange24h    Dec      `json:"market_cap_change_24h"`
	MarketCapChangePct24h *float64 `json:"market_cap_change_pct_24h"`
	CirculatingSupply     Dec      `json:"circulating_supply"`
	TotalSupply           Dec      `json:"total_supply"`
	MaxSupply             Dec      `json:"max_supply"`
	ATH                   Dec      `json:"ath"`
	ATHChangePct          *float64 `json:"ath_change_pct"`
	ATHAt                 *string  `json:"ath_at"`
	ATL                   Dec      `json:"atl"`
	ATLChangePct          *float64 `json:"atl_change_pct"`
	ATLAt                 *string  `json:"atl_at"`
	ROITimes              *float64 `json:"roi_times"`
	ROICurrency           *string  `json:"roi_currency"`
	ROIPct                *float64 `json:"roi_pct"`
	SourceUpdatedAt       *string  `json:"source_updated_at"`
	IsStale               bool     `json:"is_stale"`
	IsInactive            bool     `json:"is_inactive"`
	IsAnomaly             bool     `json:"is_anomaly"`
}
