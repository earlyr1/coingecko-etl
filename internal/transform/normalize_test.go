package transform

import (
	"encoding/json"
	"testing"
)

func TestDecFromNumber(t *testing.T) {
	cases := []struct {
		in      json.Number
		want    Dec
		wantErr bool
	}{
		{"", "", false},
		{"82776", "82776", false},
		{"20096053.0", "20096053", false},
		{"4e-05", "0.00004", false},
		{"1.5E+3", "1500", false},
		{"-34.3464", "-34.3464", false},
		{"43083803721443304123", "43083803721443304123", false},
		{"0", "0", false},
		{"abc", "", true},
	}
	for _, c := range cases {
		got, err := DecFromNumber(c.in)
		if (err != nil) != c.wantErr {
			t.Fatalf("DecFromNumber(%q) err = %v, wantErr %v", c.in, err, c.wantErr)
		}
		if got != c.want {
			t.Errorf("DecFromNumber(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestDecJSON(t *testing.T) {
	b, err := json.Marshal(struct {
		A Dec `json:"a"`
		B Dec `json:"b"`
	}{A: "1.5"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"a":"1.5","b":null}` {
		t.Fatalf("got %s", b)
	}
}

func TestNormalizeHelpers(t *testing.T) {
	s := func(v string) *string { return &v }
	if nilIfEmpty(s("")) != nil || nilIfEmpty(nil) != nil || *nilIfEmpty(s("x")) != "x" {
		t.Error("nilIfEmpty")
	}
	if got := *upperSymbol(s("btc")); got != "BTC" {
		t.Errorf("upperSymbol = %q", got)
	}
	if got := nonEmptyStrings([]string{}); got != nil {
		t.Errorf("nonEmptyStrings([]) = %#v, want nil", got)
	}
	if got := nonEmptyStrings([]string{"", "Layer 1"}); len(got) != 1 || got[0] != "Layer 1" {
		t.Errorf("nonEmptyStrings = %#v", got)
	}
	if got := normAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"); got != "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" {
		t.Errorf("normAddress EVM = %q", got)
	}
	if got := normAddress("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"); got != "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" {
		t.Errorf("normAddress non-EVM changed: %q", got)
	}
	if got := normAsset("0X0B7C6C138DC75622F4200D157B13FB93E6BD94FA"); got != "0x0b7c6c138dc75622f4200d157b13fb93e6bd94fa" {
		t.Errorf("normAsset EVM = %q", got)
	}
	if got := normAsset("thor.rune"); got != "THOR.RUNE" {
		t.Errorf("normAsset symbol = %q", got)
	}
	got, err := normTime(s("2026-10-10T10:00:45+00:00"))
	if err != nil || *got != "2026-10-10T10:00:45.000Z" {
		t.Errorf("normTime = %v, %v", got, err)
	}
	if got, err := normTime(s("")); got != nil || err != nil {
		t.Errorf("normTime(\"\") = %v, %v", got, err)
	}
	if _, err := normTime(s("yesterday")); err == nil {
		t.Error("normTime accepted garbage")
	}
}
