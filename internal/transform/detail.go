package transform

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/xtime"
)

// CoinSnapshot is the coin_snapshot parent row (scalars of /coins/{id}).
type CoinSnapshot struct {
	CoinID            string   `json:"coin_id"`
	SnapshotAt        string   `json:"snapshot_at"`
	Symbol            *string  `json:"symbol"`
	Name              *string  `json:"name"`
	Categories        []string `json:"categories"`
	HashingAlgorithm  *string  `json:"hashing_algorithm"`
	GenesisDate       *string  `json:"genesis_date"`
	MarketCapRank     *int64   `json:"market_cap_rank"`
	CirculatingSupply Dec      `json:"circulating_supply"`
	TotalSupply       Dec      `json:"total_supply"`
	MaxSupply         Dec      `json:"max_supply"`
	MaxSupplyInfinite *bool    `json:"max_supply_infinite"`
	SourceUpdatedAt   *string  `json:"source_updated_at"`
	IsStale           bool     `json:"is_stale"`
}

// CoinMarketByCurrency is one currency of the 23 market_data maps zipped together.
type CoinMarketByCurrency struct {
	CoinID                string   `json:"coin_id"`
	Currency              string   `json:"currency"`
	SnapshotAt            string   `json:"snapshot_at"`
	Price                 Dec      `json:"price"`
	MarketCap             Dec      `json:"market_cap"`
	FDV                   Dec      `json:"fdv"`
	TotalVolume           Dec      `json:"total_volume"`
	High24h               Dec      `json:"high_24h"`
	Low24h                Dec      `json:"low_24h"`
	ATH                   Dec      `json:"ath"`
	ATHChangePct          *float64 `json:"ath_change_pct"`
	ATHAt                 *string  `json:"ath_at"`
	ATL                   Dec      `json:"atl"`
	ATLChangePct          *float64 `json:"atl_change_pct"`
	ATLAt                 *string  `json:"atl_at"`
	PriceChange24h        Dec      `json:"price_change_24h"`
	PriceChangePct1h      *float64 `json:"price_change_pct_1h"`
	PriceChangePct24h     *float64 `json:"price_change_pct_24h"`
	PriceChangePct7d      *float64 `json:"price_change_pct_7d"`
	PriceChangePct14d     *float64 `json:"price_change_pct_14d"`
	PriceChangePct30d     *float64 `json:"price_change_pct_30d"`
	PriceChangePct60d     *float64 `json:"price_change_pct_60d"`
	PriceChangePct200d    *float64 `json:"price_change_pct_200d"`
	PriceChangePct1y      *float64 `json:"price_change_pct_1y"`
	MarketCapChange24h    Dec      `json:"market_cap_change_24h"`
	MarketCapChangePct24h *float64 `json:"market_cap_change_pct_24h"`
	IsInactive            bool     `json:"is_inactive"`
	IsAnomaly             bool     `json:"is_anomaly"`
}

// Ticker is one element of tickers[] of /coins/{id}.
type Ticker struct {
	CoinID              string   `json:"coin_id"` // the /coins/{id} document's coin (parent key)
	ExchangeID          string   `json:"exchange_id"`
	Base                string   `json:"base"`
	Target              string   `json:"target"`
	SnapshotAt          string   `json:"snapshot_at"`
	ExchangeName        *string  `json:"exchange_name"`
	HasTradingIncentive *bool    `json:"has_trading_incentive"`
	BaseCoinID          *string  `json:"base_coin_id"` // tickers[].coin_id (e.g. "ethereum" for ETH/BTC)
	TargetCoinID        *string  `json:"target_coin_id"`
	Last                Dec      `json:"last"`
	Volume              Dec      `json:"volume"`
	LastUSD             Dec      `json:"last_usd"`
	LastBTC             Dec      `json:"last_btc"`
	LastETH             Dec      `json:"last_eth"`
	VolumeUSD           Dec      `json:"volume_usd"`
	VolumeBTC           Dec      `json:"volume_btc"`
	VolumeETH           Dec      `json:"volume_eth"`
	BidAskSpreadPct     *float64 `json:"bid_ask_spread_pct"`
	TrustScore          *string  `json:"trust_score"`
	TickerTimestamp     *string  `json:"ticker_timestamp"`
	LastTradedAt        *string  `json:"last_traded_at"`
	LastFetchAt         *string  `json:"last_fetch_at"`
	UpstreamIsStale     *bool    `json:"upstream_is_stale"`
	UpstreamIsAnomaly   *bool    `json:"upstream_is_anomaly"`
	IsStale             bool     `json:"is_stale"`
	IsAnomaly           bool     `json:"is_anomaly"`
}

// CoinContract is one platform of platforms + detail_platforms.
type CoinContract struct {
	CoinID          string `json:"coin_id"`
	PlatformID      string `json:"platform_id"`
	SnapshotAt      string `json:"snapshot_at"`
	ContractAddress string `json:"contract_address"`
	DecimalPlace    *int64 `json:"decimal_place"`
}

// coinDoc is the subset of /coins/{id} we use; everything else is dropped.
type coinDoc struct {
	ID               *string            `json:"id"`
	Symbol           *string            `json:"symbol"`
	Name             *string            `json:"name"`
	Categories       []string           `json:"categories"`
	HashingAlgorithm *string            `json:"hashing_algorithm"`
	GenesisDate      *string            `json:"genesis_date"`
	MarketCapRank    json.Number        `json:"market_cap_rank"`
	Platforms        map[string]*string `json:"platforms"`
	DetailPlatforms  map[string]*struct {
		DecimalPlace    json.Number `json:"decimal_place"`
		ContractAddress *string     `json:"contract_address"`
	} `json:"detail_platforms"`
	LastUpdated *string        `json:"last_updated"`
	MarketData  *coinMarketDoc `json:"market_data"`
	Tickers     []tickerDoc    `json:"tickers"`
}

// numMap is a CoinGecko currency map ("usd": 82776, …); null values decode to "".
type numMap map[string]json.Number

type coinMarketDoc struct {
	CurrentPrice                    numMap             `json:"current_price"`
	MarketCap                       numMap             `json:"market_cap"`
	FullyDilutedValuation           numMap             `json:"fully_diluted_valuation"`
	TotalVolume                     numMap             `json:"total_volume"`
	High24h                         numMap             `json:"high_24h"`
	Low24h                          numMap             `json:"low_24h"`
	ATH                             numMap             `json:"ath"`
	ATHChangePercentage             numMap             `json:"ath_change_percentage"`
	ATHDate                         map[string]*string `json:"ath_date"`
	ATL                             numMap             `json:"atl"`
	ATLChangePercentage             numMap             `json:"atl_change_percentage"`
	ATLDate                         map[string]*string `json:"atl_date"`
	PriceChange24hInCurrency        numMap             `json:"price_change_24h_in_currency"`
	PriceChangePct1hInCurrency      numMap             `json:"price_change_percentage_1h_in_currency"`
	PriceChangePct24hInCurrency     numMap             `json:"price_change_percentage_24h_in_currency"`
	PriceChangePct7dInCurrency      numMap             `json:"price_change_percentage_7d_in_currency"`
	PriceChangePct14dInCurrency     numMap             `json:"price_change_percentage_14d_in_currency"`
	PriceChangePct30dInCurrency     numMap             `json:"price_change_percentage_30d_in_currency"`
	PriceChangePct60dInCurrency     numMap             `json:"price_change_percentage_60d_in_currency"`
	PriceChangePct200dInCurrency    numMap             `json:"price_change_percentage_200d_in_currency"`
	PriceChangePct1yInCurrency      numMap             `json:"price_change_percentage_1y_in_currency"`
	MarketCapChange24hInCurrency    numMap             `json:"market_cap_change_24h_in_currency"`
	MarketCapChangePct24hInCurrency numMap             `json:"market_cap_change_percentage_24h_in_currency"`
	TotalSupply                     json.Number        `json:"total_supply"`
	MaxSupply                       json.Number        `json:"max_supply"`
	MaxSupplyInfinite               *bool              `json:"max_supply_infinite"`
	CirculatingSupply               json.Number        `json:"circulating_supply"`
	LastUpdated                     *string            `json:"last_updated"`
}

type tickerDoc struct {
	Base   *string `json:"base"`
	Target *string `json:"target"`
	Market *struct {
		Name                *string `json:"name"`
		Identifier          *string `json:"identifier"`
		HasTradingIncentive *bool   `json:"has_trading_incentive"`
	} `json:"market"`
	Last                   json.Number `json:"last"`
	Volume                 json.Number `json:"volume"`
	ConvertedLast          numMap      `json:"converted_last"`
	ConvertedVolume        numMap      `json:"converted_volume"`
	TrustScore             *string     `json:"trust_score"`
	BidAskSpreadPercentage json.Number `json:"bid_ask_spread_percentage"`
	Timestamp              *string     `json:"timestamp"`
	LastTradedAt           *string     `json:"last_traded_at"`
	LastFetchAt            *string     `json:"last_fetch_at"`
	IsAnomaly              *bool       `json:"is_anomaly"`
	IsStale                *bool       `json:"is_stale"`
	CoinID                 *string     `json:"coin_id"`
	TargetCoinID           *string     `json:"target_coin_id"`
}

// detailBuilder accumulates records of one /coins/{id} message; ordinals
// are assigned in append order across all datasets of the message.
type detailBuilder struct {
	coinID    string
	respKey   string // responseID(ev): keys records that have no id
	snap      time.Time
	snapAt    string
	eventDate string
	rules     Rules
	out       []Record
}

func (b *detailBuilder) add(dataset, key string, zone Zone, reason string, business any) error {
	sum, err := ContentSHA256(business)
	if err != nil {
		return err
	}
	b.out = append(b.out, Record{
		Dataset:       dataset,
		Key:           key,
		EventDate:     b.eventDate,
		Ordinal:       len(b.out),
		Zone:          zone,
		Reason:        reason,
		ContentSHA256: sum,
		Business:      business,
	})
	return nil
}

func transformDetail(ev event.RawEvent, rules Rules) ([]Record, error) {
	var doc coinDoc
	if err := decodeJSON(ev.Payload, &doc); err != nil {
		return nil, fmt.Errorf("transform: decode coin_detail payload: %w", err)
	}
	snap, ok, err := parseTime(doc.LastUpdated)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("transform: coin_detail payload has no last_updated")
	}
	b := &detailBuilder{
		coinID:    strings.TrimSpace(strOrEmpty(doc.ID)),
		respKey:   responseID(ev),
		snap:      snap,
		snapAt:    xtime.Format(snap),
		eventDate: xtime.FormatDate(snap),
		rules:     rules,
	}

	parent, err := coinSnapshotFrom(&doc, b)
	if err != nil {
		return nil, err
	}
	if b.coinID == "" {
		// Children cannot reference a parent without an id: only the reject is emitted.
		if err := b.add(DatasetCoinSnapshot, fallbackKey(b.respKey, 0, b.snapAt), ZoneRejected, ReasonMissingID, parent); err != nil {
			return nil, err
		}
		return b.out, nil
	}
	if err := b.add(DatasetCoinSnapshot, b.coinID+"|"+b.snapAt, ZoneProcessed, "", parent); err != nil {
		return nil, err
	}
	if err := b.addCurrencies(doc.MarketData); err != nil {
		return nil, err
	}
	if err := b.addTickers(doc.Tickers); err != nil {
		return nil, err
	}
	if err := b.addContracts(&doc); err != nil {
		return nil, err
	}
	return b.out, nil
}

func coinSnapshotFrom(doc *coinDoc, b *detailBuilder) (CoinSnapshot, error) {
	var e decErr
	c := CoinSnapshot{
		CoinID:           b.coinID,
		SnapshotAt:       b.snapAt,
		Symbol:           upperSymbol(doc.Symbol),
		Name:             nilIfEmpty(doc.Name),
		Categories:       nonEmptyStrings(doc.Categories),
		HashingAlgorithm: nilIfEmpty(doc.HashingAlgorithm),
	}
	if g := nilIfEmpty(doc.GenesisDate); g != nil {
		t, err := time.Parse("2006-01-02", *g)
		if err != nil {
			return CoinSnapshot{}, fmt.Errorf("transform: bad genesis_date %q: %w", *g, err)
		}
		v := t.Format("2006-01-02")
		c.GenesisDate = &v
	}
	rank, err := intOrNil(doc.MarketCapRank)
	if err != nil {
		return CoinSnapshot{}, err
	}
	c.MarketCapRank = rank
	updated := doc.LastUpdated
	if md := doc.MarketData; md != nil {
		c.CirculatingSupply = e.dec(md.CirculatingSupply)
		c.TotalSupply = e.dec(md.TotalSupply)
		c.MaxSupply = e.dec(md.MaxSupply)
		c.MaxSupplyInfinite = md.MaxSupplyInfinite
		if nilIfEmpty(md.LastUpdated) != nil {
			updated = md.LastUpdated
		}
	}
	c.SourceUpdatedAt = e.ts(updated)
	if e.err != nil {
		return CoinSnapshot{}, e.err
	}
	if t, ok, _ := parseTime(updated); ok {
		c.IsStale = b.snap.Sub(t) > b.rules.StaleAfter
	}
	return c, nil
}

// addCurrencies zips the 23 currency maps on the currency code, sorted.
func (b *detailBuilder) addCurrencies(md *coinMarketDoc) error {
	if md == nil {
		return nil
	}
	numMaps := []numMap{
		md.CurrentPrice, md.MarketCap, md.FullyDilutedValuation, md.TotalVolume, md.High24h, md.Low24h,
		md.ATH, md.ATHChangePercentage, md.ATL, md.ATLChangePercentage,
		md.PriceChange24hInCurrency, md.PriceChangePct1hInCurrency, md.PriceChangePct24hInCurrency,
		md.PriceChangePct7dInCurrency, md.PriceChangePct14dInCurrency, md.PriceChangePct30dInCurrency,
		md.PriceChangePct60dInCurrency, md.PriceChangePct200dInCurrency, md.PriceChangePct1yInCurrency,
		md.MarketCapChange24hInCurrency, md.MarketCapChangePct24hInCurrency,
	}
	set := map[string]bool{}
	for _, m := range numMaps {
		for k := range m {
			set[k] = true
		}
	}
	for _, m := range []map[string]*string{md.ATHDate, md.ATLDate} {
		for k := range m {
			set[k] = true
		}
	}
	currencies := make([]string, 0, len(set))
	for k := range set {
		currencies = append(currencies, k)
	}
	sort.Strings(currencies)

	for _, cur := range currencies {
		var e decErr
		r := CoinMarketByCurrency{
			CoinID:                b.coinID,
			Currency:              strings.ToLower(strings.TrimSpace(cur)),
			SnapshotAt:            b.snapAt,
			Price:                 e.dec(md.CurrentPrice[cur]),
			MarketCap:             e.dec(md.MarketCap[cur]),
			FDV:                   e.dec(md.FullyDilutedValuation[cur]),
			TotalVolume:           e.dec(md.TotalVolume[cur]),
			High24h:               e.dec(md.High24h[cur]),
			Low24h:                e.dec(md.Low24h[cur]),
			ATH:                   e.dec(md.ATH[cur]),
			ATHChangePct:          e.pct(md.ATHChangePercentage[cur]),
			ATHAt:                 e.ts(md.ATHDate[cur]),
			ATL:                   e.dec(md.ATL[cur]),
			ATLChangePct:          e.pct(md.ATLChangePercentage[cur]),
			ATLAt:                 e.ts(md.ATLDate[cur]),
			PriceChange24h:        e.dec(md.PriceChange24hInCurrency[cur]),
			PriceChangePct1h:      e.pct(md.PriceChangePct1hInCurrency[cur]),
			PriceChangePct24h:     e.pct(md.PriceChangePct24hInCurrency[cur]),
			PriceChangePct7d:      e.pct(md.PriceChangePct7dInCurrency[cur]),
			PriceChangePct14d:     e.pct(md.PriceChangePct14dInCurrency[cur]),
			PriceChangePct30d:     e.pct(md.PriceChangePct30dInCurrency[cur]),
			PriceChangePct60d:     e.pct(md.PriceChangePct60dInCurrency[cur]),
			PriceChangePct200d:    e.pct(md.PriceChangePct200dInCurrency[cur]),
			PriceChangePct1y:      e.pct(md.PriceChangePct1yInCurrency[cur]),
			MarketCapChange24h:    e.dec(md.MarketCapChange24hInCurrency[cur]),
			MarketCapChangePct24h: e.pct(md.MarketCapChangePct24hInCurrency[cur]),
		}
		if e.err != nil {
			return fmt.Errorf("transform: market_data currency %q: %w", cur, e.err)
		}
		r.IsInactive = isZero(r.TotalVolume)
		// Strict: /coins/{id} sends price and 24h bounds at one precision.
		r.IsAnomaly = outsideRange(r.Price, r.Low24h, r.High24h, 0)

		zone, reason := ZoneProcessed, ""
		switch {
		case allMetricsNull(r):
			zone, reason = ZoneRejected, ReasonEmptyRecord
		case r.Price == "":
			zone, reason = ZoneRejected, ReasonNullPrice
		case !positive(r.Price):
			zone, reason = ZoneRejected, ReasonNonpositivePrice
		}
		key := b.coinID + "|" + r.Currency + "|" + b.snapAt
		if err := b.add(DatasetCoinMarketByCurrency, key, zone, reason, r); err != nil {
			return err
		}
	}
	return nil
}

func allMetricsNull(r CoinMarketByCurrency) bool {
	for _, d := range []Dec{r.Price, r.MarketCap, r.FDV, r.TotalVolume, r.High24h, r.Low24h, r.ATH, r.ATL, r.PriceChange24h, r.MarketCapChange24h} {
		if d != "" {
			return false
		}
	}
	for _, f := range []*float64{r.ATHChangePct, r.ATLChangePct, r.PriceChangePct1h, r.PriceChangePct24h, r.PriceChangePct7d,
		r.PriceChangePct14d, r.PriceChangePct30d, r.PriceChangePct60d, r.PriceChangePct200d, r.PriceChangePct1y, r.MarketCapChangePct24h} {
		if f != nil {
			return false
		}
	}
	return r.ATHAt == nil && r.ATLAt == nil
}

// addTickers emits one record per tickers[] element, in payload order.
// Duplicate keys inside one message are not resolved here: each keeps its
// own ordinal and dedup decides (first ordinal wins, equal content drops).
func (b *detailBuilder) addTickers(docs []tickerDoc) error {
	rows := make([]Ticker, 0, len(docs))
	for i, d := range docs {
		var e decErr
		t := Ticker{
			CoinID:            b.coinID,
			Base:              normAsset(strOrEmpty(d.Base)),
			Target:            normAsset(strOrEmpty(d.Target)),
			SnapshotAt:        b.snapAt,
			BaseCoinID:        nilIfEmpty(d.CoinID),
			TargetCoinID:      nilIfEmpty(d.TargetCoinID),
			Last:              e.dec(d.Last),
			Volume:            e.dec(d.Volume),
			LastUSD:           e.dec(d.ConvertedLast["usd"]),
			LastBTC:           e.dec(d.ConvertedLast["btc"]),
			LastETH:           e.dec(d.ConvertedLast["eth"]),
			VolumeUSD:         e.dec(d.ConvertedVolume["usd"]),
			VolumeBTC:         e.dec(d.ConvertedVolume["btc"]),
			VolumeETH:         e.dec(d.ConvertedVolume["eth"]),
			BidAskSpreadPct:   e.pct(d.BidAskSpreadPercentage),
			TrustScore:        nilIfEmpty(d.TrustScore),
			TickerTimestamp:   e.ts(d.Timestamp),
			LastTradedAt:      e.ts(d.LastTradedAt),
			LastFetchAt:       e.ts(d.LastFetchAt),
			UpstreamIsStale:   d.IsStale,
			UpstreamIsAnomaly: d.IsAnomaly,
		}
		if d.Market != nil {
			t.ExchangeID = strings.TrimSpace(strOrEmpty(d.Market.Identifier))
			t.ExchangeName = nilIfEmpty(d.Market.Name)
			t.HasTradingIncentive = d.Market.HasTradingIncentive
		}
		if e.err != nil {
			return fmt.Errorf("transform: ticker %d: %w", i, e.err)
		}
		stale := d.IsStale != nil && *d.IsStale
		if lt, ok, _ := parseTime(d.LastTradedAt); ok && b.snap.Sub(lt) > b.rules.TickerStaleAfter {
			stale = true
		}
		t.IsStale = stale
		rows = append(rows, t)
	}

	median, hasMedian := medianLastUSD(rows)
	limit := decimal.NewFromFloat(b.rules.TickerAnomalyPct)
	hundred := decimal.NewFromInt(100)
	for i := range rows {
		t := &rows[i]
		t.IsAnomaly = t.UpstreamIsAnomaly != nil && *t.UpstreamIsAnomaly
		if v, ok := t.LastUSD.Decimal(); ok && hasMedian {
			dev := v.Sub(median).Abs().Div(median).Mul(hundred)
			if dev.GreaterThan(limit) {
				t.IsAnomaly = true
			}
		}
		zone, reason := ZoneProcessed, ""
		key := b.coinID + "|" + t.ExchangeID + "|" + t.Base + "|" + t.Target + "|" + b.snapAt
		if t.ExchangeID == "" || t.Base == "" || t.Target == "" {
			zone, reason = ZoneRejected, ReasonMissingID
			key = b.coinID + "|" + fallbackKey(b.respKey, i, b.snapAt)
		}
		if err := b.add(DatasetTicker, key, zone, reason, *t); err != nil {
			return err
		}
	}
	return nil
}

// medianLastUSD is the median of the present, positive last_usd values.
func medianLastUSD(rows []Ticker) (decimal.Decimal, bool) {
	var vals []decimal.Decimal
	for _, t := range rows {
		if v, ok := t.LastUSD.Decimal(); ok && v.IsPositive() {
			vals = append(vals, v)
		}
	}
	if len(vals) == 0 {
		return decimal.Decimal{}, false
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i].LessThan(vals[j]) })
	n := len(vals)
	if n%2 == 1 {
		return vals[n/2], true
	}
	return vals[n/2-1].Add(vals[n/2]).Div(decimal.NewFromInt(2)), true
}

// addContracts zips platforms and detail_platforms on the platform key, sorted.
func (b *detailBuilder) addContracts(doc *coinDoc) error {
	set := map[string]bool{}
	for k := range doc.Platforms {
		set[k] = true
	}
	for k := range doc.DetailPlatforms {
		set[k] = true
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		addr := strOrEmpty(doc.Platforms[k])
		var places json.Number
		if dp := doc.DetailPlatforms[k]; dp != nil {
			if strings.TrimSpace(addr) == "" {
				addr = strOrEmpty(dp.ContractAddress)
			}
			places = dp.DecimalPlace
		}
		dpl, err := intOrNil(places)
		if err != nil {
			return fmt.Errorf("transform: platform %q decimal_place: %w", k, err)
		}
		c := CoinContract{
			CoinID:          b.coinID,
			PlatformID:      strings.TrimSpace(k),
			SnapshotAt:      b.snapAt,
			ContractAddress: normAddress(addr),
			DecimalPlace:    dpl,
		}
		zone, reason := ZoneProcessed, ""
		if c.PlatformID == "" || c.ContractAddress == "" {
			zone, reason = ZoneRejected, ReasonEmptyPlatform
		}
		key := b.coinID + "|" + c.PlatformID + "|" + b.snapAt
		if err := b.add(DatasetCoinContract, key, zone, reason, c); err != nil {
			return err
		}
	}
	return nil
}
