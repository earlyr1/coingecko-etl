package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/xtime"

	"github.com/shopspring/decimal"
)

// marketsRow is one element of the /coins/markets array. Numbers are
// json.Number so money keeps its exact text; JSON null leaves "" / nil.
// Fields not listed (image, …) are dropped by decoding.
type marketsRow struct {
	ID                           *string     `json:"id"`
	Symbol                       *string     `json:"symbol"`
	Name                         *string     `json:"name"`
	CurrentPrice                 json.Number `json:"current_price"`
	MarketCap                    json.Number `json:"market_cap"`
	MarketCapRank                json.Number `json:"market_cap_rank"`
	FullyDilutedValuation        json.Number `json:"fully_diluted_valuation"`
	TotalVolume                  json.Number `json:"total_volume"`
	High24h                      json.Number `json:"high_24h"`
	Low24h                       json.Number `json:"low_24h"`
	PriceChange24h               json.Number `json:"price_change_24h"`
	PriceChangePercentage24h     json.Number `json:"price_change_percentage_24h"`
	MarketCapChange24h           json.Number `json:"market_cap_change_24h"`
	MarketCapChangePercentage24h json.Number `json:"market_cap_change_percentage_24h"`
	CirculatingSupply            json.Number `json:"circulating_supply"`
	TotalSupply                  json.Number `json:"total_supply"`
	MaxSupply                    json.Number `json:"max_supply"`
	ATH                          json.Number `json:"ath"`
	ATHChangePercentage          json.Number `json:"ath_change_percentage"`
	ATHDate                      *string     `json:"ath_date"`
	ATL                          json.Number `json:"atl"`
	ATLChangePercentage          json.Number `json:"atl_change_percentage"`
	ATLDate                      *string     `json:"atl_date"`
	ROI                          *struct {
		Times      json.Number `json:"times"`
		Currency   *string     `json:"currency"`
		Percentage json.Number `json:"percentage"`
	} `json:"roi"`
	LastUpdated         *string     `json:"last_updated"`
	PriceChangePct1hIn  json.Number `json:"price_change_percentage_1h_in_currency"`
	PriceChangePct24hIn json.Number `json:"price_change_percentage_24h_in_currency"`
	PriceChangePct7dIn  json.Number `json:"price_change_percentage_7d_in_currency"`
}

// decodeJSON decodes b into v with UseNumber (numbers never pass through
// float64) and rejects trailing data.
func decodeJSON(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return fmt.Errorf("trailing data after JSON value")
	}
	return nil
}

func decodeMarkets(payload []byte) ([]marketsRow, error) {
	var rows []marketsRow
	if err := decodeJSON(payload, &rows); err != nil {
		return nil, fmt.Errorf("transform: decode coins_markets payload: %w", err)
	}
	return rows, nil
}

// marketsSnapshotAt is the max last_updated over all rows: CoinGecko stamps
// every coin recomputed in one pass with the same time, stale coins lag.
func marketsSnapshotAt(rows []marketsRow) (time.Time, error) {
	var max time.Time
	found := false
	for _, r := range rows {
		t, ok, err := parseTime(r.LastUpdated)
		if err != nil {
			return time.Time{}, err
		}
		if ok && (!found || t.After(max)) {
			max, found = t, true
		}
	}
	if !found {
		return time.Time{}, fmt.Errorf("transform: coins_markets payload has no last_updated")
	}
	return max, nil
}

func transformMarkets(ev event.RawEvent, rules Rules) ([]Record, error) {
	rows, err := decodeMarkets(ev.Payload)
	if err != nil {
		return nil, err
	}
	snap, err := marketsSnapshotAt(rows)
	if err != nil {
		return nil, err
	}
	snapAt := xtime.Format(snap)
	eventDate := xtime.FormatDate(snap)
	vs := strings.ToLower(strings.TrimSpace(ev.Request.Params["vs_currency"]))
	if vs == "" {
		vs = "usd"
	}

	out := make([]Record, 0, len(rows))
	for i, r := range rows {
		b, err := marketSnapshotFrom(r, snap, snapAt, vs, rules)
		if err != nil {
			return nil, fmt.Errorf("transform: coins_markets row %d: %w", i, err)
		}
		rec := Record{
			Dataset:   DatasetMarketSnapshot,
			Key:       b.CoinID + "|" + snapAt,
			EventDate: eventDate,
			Ordinal:   i,
			Zone:      ZoneProcessed,
			Business:  b,
		}
		switch {
		case b.CoinID == "":
			rec.Zone, rec.Reason = ZoneRejected, ReasonMissingID
			// No coin id to key on: the row's position in the payload keeps the key unique.
			rec.Key = "#" + strconv.Itoa(i) + "|" + snapAt
		case b.Price == "":
			rec.Zone, rec.Reason = ZoneRejected, ReasonNullPrice
		case !positive(b.Price):
			rec.Zone, rec.Reason = ZoneRejected, ReasonNonpositivePrice
		}
		if rec.ContentSHA256, err = ContentSHA256(b); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

func marketSnapshotFrom(r marketsRow, snap time.Time, snapAt, vs string, rules Rules) (MarketSnapshot, error) {
	var e decErr
	m := MarketSnapshot{
		CoinID:                strings.TrimSpace(strOrEmpty(r.ID)),
		SnapshotAt:            snapAt,
		Symbol:                upperSymbol(r.Symbol),
		Name:                  nilIfEmpty(r.Name),
		VsCurrency:            vs,
		Price:                 e.dec(r.CurrentPrice),
		MarketCap:             e.dec(r.MarketCap),
		FDV:                   e.dec(r.FullyDilutedValuation),
		TotalVolume:           e.dec(r.TotalVolume),
		High24h:               e.dec(r.High24h),
		Low24h:                e.dec(r.Low24h),
		PriceChange24h:        e.dec(r.PriceChange24h),
		PriceChangePct1h:      e.pct(r.PriceChangePct1hIn),
		PriceChangePct24h:     e.pct(r.PriceChangePercentage24h),
		PriceChangePct7d:      e.pct(r.PriceChangePct7dIn),
		MarketCapChange24h:    e.dec(r.MarketCapChange24h),
		MarketCapChangePct24h: e.pct(r.MarketCapChangePercentage24h),
		CirculatingSupply:     e.dec(r.CirculatingSupply),
		TotalSupply:           e.dec(r.TotalSupply),
		MaxSupply:             e.dec(r.MaxSupply),
		ATH:                   e.dec(r.ATH),
		ATHChangePct:          e.pct(r.ATHChangePercentage),
		ATHAt:                 e.ts(r.ATHDate),
		ATL:                   e.dec(r.ATL),
		ATLChangePct:          e.pct(r.ATLChangePercentage),
		ATLAt:                 e.ts(r.ATLDate),
		SourceUpdatedAt:       e.ts(r.LastUpdated),
	}
	if m.PriceChangePct24h == nil {
		// The *_in_currency duplicate is the fallback when the plain field is null.
		m.PriceChangePct24h = e.pct(r.PriceChangePct24hIn)
	}
	if r.ROI != nil {
		m.ROITimes = e.pct(r.ROI.Times)
		m.ROICurrency = lowerCode(r.ROI.Currency) // a currency code: joins on vs_currency
		m.ROIPct = e.pct(r.ROI.Percentage)
	}
	rank, err := intOrNil(r.MarketCapRank)
	if err != nil {
		return MarketSnapshot{}, err
	}
	m.MarketCapRank = rank
	if e.err != nil {
		return MarketSnapshot{}, e.err
	}

	if t, ok, _ := parseTime(r.LastUpdated); ok {
		m.IsStale = snap.Sub(t) > rules.StaleAfter
	}
	m.IsInactive = isZero(m.TotalVolume)
	// precision=full prices carry more digits than the rounded 24h bounds
	// (USYC 1.140213 against [1.14, 1.14]): allow the configured tolerance.
	m.IsAnomaly = outsideRange(m.Price, m.Low24h, m.High24h, rules.AnomalyTolerancePct)
	return m, nil
}

// positive reports whether d is a decimal > 0.
func positive(d Dec) bool {
	v, ok := d.Decimal()
	return ok && v.IsPositive()
}

// isZero reports whether d is present and equal to 0.
func isZero(d Dec) bool {
	v, ok := d.Decimal()
	return ok && v.IsZero()
}

// outsideRange reports whether price lies outside [low, high] widened by
// tolPct percent on each side: price < low*(1-tolPct/100) or price >
// high*(1+tolPct/100), computed in decimal. tolPct 0 is the strict check.
// It is false unless all three values are present.
func outsideRange(price, low, high Dec, tolPct float64) bool {
	p, okP := price.Decimal()
	l, okL := low.Decimal()
	h, okH := high.Decimal()
	if !okP || !okL || !okH {
		return false
	}
	t := decimal.NewFromFloat(tolPct).Div(decimal.NewFromInt(100))
	one := decimal.NewFromInt(1)
	return p.LessThan(l.Mul(one.Sub(t))) || p.GreaterThan(h.Mul(one.Add(t)))
}
