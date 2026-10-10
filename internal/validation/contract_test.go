package validation

import (
	"encoding/json"
	"io/fs"
	"os"
	"path"
	"strings"
	"testing"

	"coingecko-etl/schemas"
)

// Fixture layout (schemas/testdata):
//
//	<schema path without .json>/valid/*.json    must produce zero violations
//	<schema path without .json>/invalid/*.json  {"expect":{"instance_location":..,"keyword":..},"doc":<document>}
//
// e.g. pipeline/raw_event.v1/valid/ok_markets.json is checked against
// https://schemas.cg-etl.internal/pipeline/raw_event.v1.json. Directories
// with any other shape (testdata/coingecko/ recorded responses) are ignored
// here and have their own tests.
//
// This test is the schema compatibility gate (spec §5.2). Valid fixtures are
// append-only: never edit or delete one. Together they form the corpus that
// every in-place change to <name>.vN.json (a compatible change) must keep
// accepting. A change that would reject an existing valid fixture is
// breaking and goes into a new <name>.v(N+1).json side by side instead.
const testdataDir = "../../schemas/testdata"

type invalidFixture struct {
	Expect struct {
		InstanceLocation string `json:"instance_location"`
		Keyword          string `json:"keyword"`
	} `json:"expect"`
	Doc json.RawMessage `json:"doc"` // kept byte-for-byte
}

func TestSchemaFixtures(t *testing.T) {
	r, err := Load(schemas.FS)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	seen := map[string]map[string]int{} // schema id -> "valid"/"invalid" -> count
	fsys := os.DirFS(testdataDir)
	err = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || path.Ext(p) != ".json" {
			return err
		}
		kindDir := path.Dir(p)
		kind := path.Base(kindDir)
		if kind != "valid" && kind != "invalid" {
			return nil
		}
		id := baseURL + path.Dir(kindDir) + ".json"
		if seen[id] == nil {
			seen[id] = map[string]int{}
		}
		seen[id][kind]++
		b, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		t.Run(p, func(t *testing.T) {
			if kind == "valid" {
				vs, err := r.Validate(id, b)
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				if len(vs) != 0 {
					t.Fatalf("valid fixture has violations: %+v", vs)
				}
				return
			}
			var f invalidFixture
			if err := json.Unmarshal(b, &f); err != nil {
				t.Fatalf("fixture wrapper: %v", err)
			}
			if f.Expect.Keyword == "" || len(f.Doc) == 0 {
				t.Fatal("fixture wrapper needs expect.keyword and doc")
			}
			vs, err := r.Validate(id, f.Doc)
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			for _, v := range vs {
				if v.InstanceLocation == f.Expect.InstanceLocation && Keyword(v) == f.Expect.Keyword {
					if !strings.HasPrefix(v.KeywordLocation, "https://schemas.cg-etl.internal/") || v.Message == "" {
						t.Fatalf("malformed violation %+v", v)
					}
					return
				}
			}
			t.Fatalf("want violation (%q, %s), got %+v", f.Expect.InstanceLocation, f.Expect.Keyword, vs)
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Every strict pipeline schema must have both kinds of fixtures.
	for _, id := range []string{IDRawEvent, IDRejectedRecord, IDDLQRecord} {
		if seen[id]["valid"] == 0 || seen[id]["invalid"] == 0 {
			t.Errorf("%s: need valid and invalid fixtures, have %v", id, seen[id])
		}
	}
}
