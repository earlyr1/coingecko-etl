package transform

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"coingecko-etl/internal/xtime"
)

// evmAddress matches an EVM address in any letter case ("0x…" or "0X…").
var evmAddress = regexp.MustCompile(`(?i)^0x[0-9a-f]{40}$`)

// nilIfEmpty maps a missing or "" string to nil (CoinGecko encodes "absent"
// as null, "" or a missing key).
func nilIfEmpty(s *string) *string {
	if s == nil || strings.TrimSpace(*s) == "" {
		return nil
	}
	v := *s
	return &v
}

// strOrEmpty dereferences s, mapping nil to "".
func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// nonEmptyStrings drops "" elements and maps an empty result to nil
// (serialized as JSON null, never []).
func nonEmptyStrings(in []string) []string {
	var out []string
	for _, s := range in {
		if strings.TrimSpace(s) != "" {
			out = append(out, s)
		}
	}
	return out
}

// upperSymbol upper-cases a ticker symbol; nil/"" → nil.
func upperSymbol(s *string) *string {
	s = nilIfEmpty(s)
	if s == nil {
		return nil
	}
	v := strings.ToUpper(strings.TrimSpace(*s))
	return &v
}

// lowerCode lower-cases a currency code (like vs_currency and
// coin_market_by_currency.currency); nil/"" → nil.
func lowerCode(s *string) *string {
	s = nilIfEmpty(s)
	if s == nil {
		return nil
	}
	v := strings.ToLower(strings.TrimSpace(*s))
	return &v
}

// normAddress lower-cases EVM addresses and leaves every other address
// format untouched (Solana/Tron/Stellar addresses are case-sensitive).
func normAddress(s string) string {
	s = strings.TrimSpace(s)
	if evmAddress.MatchString(s) {
		return strings.ToLower(s)
	}
	return s
}

// normAsset normalizes a ticker base/target: EVM address → lower case,
// anything else is a symbol → upper case.
func normAsset(s string) string {
	s = strings.TrimSpace(s)
	if evmAddress.MatchString(s) {
		return strings.ToLower(s)
	}
	return strings.ToUpper(s)
}

// normTime parses any CoinGecko timestamp and re-formats it as RFC 3339 UTC
// with milliseconds; nil/"" → nil.
func normTime(s *string) (*string, error) {
	t, ok, err := parseTime(s)
	if err != nil || !ok {
		return nil, err
	}
	v := xtime.Format(t)
	return &v, nil
}

// parseTime parses a non-empty timestamp; nil/"" → zero time and ok=false.
func parseTime(s *string) (time.Time, bool, error) {
	s = nilIfEmpty(s)
	if s == nil {
		return time.Time{}, false, nil
	}
	t, err := xtime.Parse(*s)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("transform: bad timestamp %q: %w", *s, err)
	}
	return t, true, nil
}

// pct converts a JSON number to a float64 (percentages and ratios are JSON
// numbers in the processed zone); "" (JSON null) → nil.
func pct(n json.Number) (*float64, error) {
	if n == "" {
		return nil, nil
	}
	f, err := strconv.ParseFloat(string(n), 64)
	if err != nil {
		return nil, fmt.Errorf("transform: not a number %q: %w", string(n), err)
	}
	return &f, nil
}

// intOrNil converts a JSON integer to *int64; "" → nil. "5.0" is accepted.
func intOrNil(n json.Number) (*int64, error) {
	if n == "" {
		return nil, nil
	}
	if i, err := strconv.ParseInt(string(n), 10, 64); err == nil {
		return &i, nil
	}
	f, err := strconv.ParseFloat(string(n), 64)
	if err != nil || f != float64(int64(f)) {
		return nil, fmt.Errorf("transform: not an integer %q", string(n))
	}
	i := int64(f)
	return &i, nil
}

// decErr collects the first DecFromNumber/pct error so long field lists stay readable.
type decErr struct{ err error }

func (e *decErr) dec(n json.Number) Dec {
	d, err := DecFromNumber(n)
	if err != nil && e.err == nil {
		e.err = err
	}
	return d
}

func (e *decErr) pct(n json.Number) *float64 {
	f, err := pct(n)
	if err != nil && e.err == nil {
		e.err = err
	}
	return f
}

func (e *decErr) ts(s *string) *string {
	t, err := normTime(s)
	if err != nil && e.err == nil {
		e.err = err
	}
	return t
}
