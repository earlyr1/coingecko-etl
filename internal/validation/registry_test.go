package validation

import (
	"fmt"
	"strings"
	"testing"
	"testing/fstest"

	"coingecko-etl/schemas"
)

func TestLoadEmbeddedSchemas(t *testing.T) {
	r, err := Load(schemas.FS)
	if err != nil {
		t.Fatalf("Load(schemas.FS): %v", err)
	}
	for _, id := range []string{IDRawEvent, IDRejectedRecord, IDDLQRecord} {
		if _, ok := r.schemas[id]; !ok {
			t.Errorf("schema %s not registered", id)
		}
	}
}

// mapFS builds an in-memory schema tree; each file's $id is derived from
// its path so Load's $id check passes.
func mapFS(files map[string]string) fstest.MapFS {
	m := fstest.MapFS{}
	for p, body := range files {
		m[p] = &fstest.MapFile{Data: []byte(fmt.Sprintf(body, baseURL+p))}
	}
	return m
}

func TestLoadRejectsExternalRef(t *testing.T) {
	fsys := mapFS(map[string]string{
		"t/a.v1.json": `{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":%q,
			"$ref":"https://example.com/not-embedded.json"}`,
	})
	if _, err := Load(fsys); err == nil {
		t.Fatal("Load succeeded with a $ref to a non-embedded URL; want error")
	}
}

func TestLoadRejectsRelativeRefToMissingFile(t *testing.T) {
	fsys := mapFS(map[string]string{
		"t/a.v1.json": `{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":%q,
			"$ref":"../common/missing.v1.json#/$defs/x"}`,
	})
	if _, err := Load(fsys); err == nil {
		t.Fatal("Load succeeded with a $ref to a missing file; want error")
	}
}

func TestLoadRejectsIDNotMatchingPath(t *testing.T) {
	fsys := fstest.MapFS{"t/a.v1.json": &fstest.MapFile{Data: []byte(
		`{"$id":"https://schemas.cg-etl.internal/t/other.v1.json","type":"object"}`)}}
	_, err := Load(fsys)
	if err == nil || !strings.Contains(err.Error(), "$id") {
		t.Fatalf("Load err = %v, want $id mismatch error", err)
	}
}

func TestLoadRejectsDuplicateKeysInSchema(t *testing.T) {
	fsys := mapFS(map[string]string{
		"t/a.v1.json": `{"$id":%q,"type":"object","type":"string"}`,
	})
	if _, err := Load(fsys); err == nil {
		t.Fatal("Load accepted a schema file with duplicate keys")
	}
}

func TestLoadRejectsEmptyFS(t *testing.T) {
	if _, err := Load(fstest.MapFS{}); err == nil {
		t.Fatal("Load(empty) succeeded; want error")
	}
}

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	r, err := Load(mapFS(map[string]string{
		"t/obj.v1.json": `{"$id":%q,"type":"object","required":["id"],
			"properties":{"id":{"type":"string"},"ts":{"$ref":"defs.v1.json#/$defs/ts"}}}`,
		"t/defs.v1.json": `{"$id":%q,"$defs":{"ts":{"type":"string","format":"date-time"}}}`,
		"t/list.v1.json": `{"$id":%q,"type":"array","items":{"type":"string"}}`,
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return r
}

func TestValidateRejectsInvalidJSON(t *testing.T) {
	r := testRegistry(t)
	id := baseURL + "t/obj.v1.json"
	cases := map[string][]byte{
		"syntax":         []byte(`{"id":`),
		"duplicate name": []byte(`{"id":"a","id":"b"}`),
		"invalid utf-8":  []byte("{\"id\":\"\xff\"}"),
		"trailing data":  []byte(`{"id":"a"} {}`),
		"empty":          []byte(``),
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := r.Validate(id, doc); err == nil {
				t.Fatalf("Validate(%q) err = nil, want invalid JSON error", doc)
			}
		})
	}
}

func TestValidateUnknownSchema(t *testing.T) {
	r := testRegistry(t)
	if _, err := r.Validate(baseURL+"t/nope.v1.json", []byte(`{}`)); err == nil {
		t.Fatal("Validate with unknown id: err = nil")
	}
}

func TestValidateValidDocument(t *testing.T) {
	r := testRegistry(t)
	vs, err := r.Validate(baseURL+"t/obj.v1.json", []byte(`{"id":"a","ts":"2026-10-10T10:07:55.000Z"}`))
	if err != nil || vs != nil {
		t.Fatalf("Validate = %v, %v; want nil, nil", vs, err)
	}
}

func TestViolationShapeThroughRef(t *testing.T) {
	r := testRegistry(t)
	vs, err := r.Validate(baseURL+"t/obj.v1.json", []byte(`{"id":"a","ts":"yesterday"}`))
	if err != nil {
		t.Fatal(err)
	}
	want := Violation{
		InstanceLocation: "/ts",
		KeywordLocation:  baseURL + "t/defs.v1.json#/$defs/ts/format",
	}
	if len(vs) != 1 || vs[0].InstanceLocation != want.InstanceLocation || vs[0].KeywordLocation != want.KeywordLocation {
		t.Fatalf("violations = %+v, want one at %+v", vs, want)
	}
	// The message must survive the $ref hop (BasicOutput loses it).
	if !strings.Contains(vs[0].Message, "date-time") {
		t.Errorf("message = %q, want it to mention date-time", vs[0].Message)
	}
	if got := Keyword(vs[0]); got != "format" {
		t.Errorf("Keyword = %q, want format", got)
	}
}

func TestViolationAtRoot(t *testing.T) {
	r := testRegistry(t)
	vs, err := r.Validate(baseURL+"t/obj.v1.json", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 1 || vs[0].InstanceLocation != "" ||
		vs[0].KeywordLocation != baseURL+"t/obj.v1.json#/required" || Keyword(vs[0]) != "required" {
		t.Fatalf("violations = %+v", vs)
	}
}

func TestViolationsCappedAndSorted(t *testing.T) {
	r := testRegistry(t)
	var items []string
	for i := 0; i < 30; i++ {
		items = append(items, fmt.Sprint(i))
	}
	doc := []byte("[" + strings.Join(items, ",") + "]") // 30 numbers, each violates items.type
	vs, err := r.Validate(baseURL+"t/list.v1.json", doc)
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 20 {
		t.Fatalf("got %d violations, want 20", len(vs))
	}
	for i := 1; i < len(vs); i++ {
		if vs[i-1].InstanceLocation > vs[i].InstanceLocation {
			t.Fatalf("violations not sorted: %q before %q", vs[i-1].InstanceLocation, vs[i].InstanceLocation)
		}
	}
	again, _ := r.Validate(baseURL+"t/list.v1.json", doc)
	if fmt.Sprint(again) != fmt.Sprint(vs) {
		t.Fatal("Validate is not deterministic")
	}
}

func TestKeyword(t *testing.T) {
	cases := map[string]string{
		"https://x/a.json#/properties/a/type": "type",
		"https://x/a.json#/required":          "required",
		"https://x/a.json#/$defs/a~1b":        "a/b",
		"":                                    "",
	}
	for loc, want := range cases {
		if got := Keyword(Violation{KeywordLocation: loc}); got != want {
			t.Errorf("Keyword(%q) = %q, want %q", loc, got, want)
		}
	}
}
