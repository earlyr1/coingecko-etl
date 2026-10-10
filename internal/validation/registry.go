// Package validation compiles the embedded JSON Schemas once and validates
// documents against them, returning flat, loggable violations.
package validation

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// maxViolations caps how many violations Validate returns (and a DLQ record
// carries).
const maxViolations = 20

// Violation is one leaf validation error.
type Violation struct {
	InstanceLocation string `json:"instance_location"` // JSON pointer into the document, "" = root
	KeywordLocation  string `json:"keyword_location"`  // absolute schema URL + "#" + JSON pointer to the failing keyword
	Message          string `json:"message"`
}

// Registry holds every compiled schema keyed by $id. Compiled schemas are
// immutable, so a Registry is safe for concurrent use.
type Registry struct {
	schemas map[string]*jsonschema.Schema
}

var printer = message.NewPrinter(language.English)

// Load compiles every *.json file under fsys. Each file must be valid JSON
// (no duplicate keys) and its $id must equal baseURL + its path in fsys.
// Any $ref that does not point at a file in fsys fails compilation:
// the loader has no URL schemes registered, so nothing is fetched from
// disk or the network.
func Load(fsys fs.FS) (*Registry, error) {
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()                          // "format" is an assertion, not an annotation
	c.UseLoader(jsonschema.SchemeURLLoader{}) // empty: every non-embedded $ref is an error

	var ids []string
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || path.Ext(p) != ".json" {
			return nil
		}
		b, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		if err := checkJSON(b); err != nil {
			return fmt.Errorf("validation: schema %s: %w", p, err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
		if err != nil {
			return fmt.Errorf("validation: schema %s: %w", p, err)
		}
		obj, ok := doc.(map[string]any)
		if !ok {
			return fmt.Errorf("validation: schema %s: not a JSON object", p)
		}
		id, _ := obj["$id"].(string)
		if want := baseURL + p; id != want {
			return fmt.Errorf("validation: schema %s: $id %q, want %q", p, id, want)
		}
		if err := c.AddResource(id, doc); err != nil {
			return fmt.Errorf("validation: schema %s: %w", p, err)
		}
		ids = append(ids, id)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, errors.New("validation: no *.json schemas found")
	}
	sort.Strings(ids)
	r := &Registry{schemas: make(map[string]*jsonschema.Schema, len(ids))}
	for _, id := range ids {
		s, err := c.Compile(id)
		if err != nil {
			return nil, fmt.Errorf("validation: compile %s: %w", id, err)
		}
		r.schemas[id] = s
	}
	return r, nil
}

// Validate checks doc against the schema with the given $id. It returns a
// non-nil error when the id is unknown or doc is not valid JSON (syntax
// error, duplicate object member names, invalid UTF-8, trailing data).
// Otherwise it returns the leaf violations (nil when valid), sorted and
// capped at 20.
func (r *Registry) Validate(schemaID string, doc []byte) ([]Violation, error) {
	s, ok := r.schemas[schemaID]
	if !ok {
		return nil, fmt.Errorf("validation: unknown schema %q", schemaID)
	}
	if err := checkJSON(doc); err != nil {
		return nil, err
	}
	// UnmarshalJSON decodes numbers as json.Number, so values above 2^53
	// keep their exact digits.
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	if err != nil {
		return nil, fmt.Errorf("validation: invalid JSON: %w", err)
	}
	err = s.Validate(inst)
	if err == nil {
		return nil, nil
	}
	var ve *jsonschema.ValidationError
	if !errors.As(err, &ve) {
		return nil, fmt.Errorf("validation: %w", err)
	}
	return leaves(ve), nil
}

// Keyword returns the last token of v.KeywordLocation (e.g. "required",
// "type", "format"), for use as a low-cardinality metric label.
func Keyword(v Violation) string {
	loc := v.KeywordLocation
	if i := strings.LastIndexByte(loc, '#'); i >= 0 {
		loc = loc[i+1:]
	}
	if i := strings.LastIndexByte(loc, '/'); i >= 0 {
		loc = loc[i+1:]
	}
	return strings.NewReplacer("~1", "/", "~0", "~").Replace(loc)
}

// checkJSON rejects bytes that are not exactly one valid JSON value.
// encoding/json/jsontext (standard library in Go 1.27) reports duplicate
// object member names and invalid UTF-8 as errors by default, which
// encoding/json v1 and the schema validator silently accept.
func checkJSON(b []byte) error {
	dec := jsontext.NewDecoder(bytes.NewReader(b))
	if err := dec.SkipValue(); err != nil {
		return fmt.Errorf("validation: invalid JSON: %w", err)
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return errors.New("validation: invalid JSON: trailing data after top-level value")
	}
	return nil
}

// leaves walks the ValidationError tree down to the leaf causes. It does
// not use BasicOutput(): in v6.0.3 BasicOutput replaces the message of
// errors reached through $ref with "validation failed".
func leaves(root *jsonschema.ValidationError) []Violation {
	var out []Violation
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) == 0 {
			out = append(out, Violation{
				InstanceLocation: pointer(e.InstanceLocation),
				KeywordLocation:  keywordLocation(e),
				Message:          e.ErrorKind.LocalizedString(printer),
			})
			return
		}
		for _, c := range e.Causes {
			walk(c)
		}
	}
	walk(root)
	// Sort for deterministic output: DLQ objects must be byte-identical
	// when a batch is reprocessed.
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.InstanceLocation != b.InstanceLocation {
			return a.InstanceLocation < b.InstanceLocation
		}
		if a.KeywordLocation != b.KeywordLocation {
			return a.KeywordLocation < b.KeywordLocation
		}
		return a.Message < b.Message
	})
	if len(out) > maxViolations {
		out = out[:maxViolations]
	}
	return out
}

// keywordLocation is the absolute location of the failing keyword:
// e.SchemaURL is "<$id>#<pointer to the subschema>"; the keyword's own
// path (ErrorKind.KeywordPath) is appended to it. v6.0.3 returns a nil
// KeywordPath for "not", so that keyword is appended by hand.
func keywordLocation(e *jsonschema.ValidationError) string {
	u := e.SchemaURL
	if !strings.Contains(u, "#") {
		u += "#"
	}
	kp := e.ErrorKind.KeywordPath()
	if _, ok := e.ErrorKind.(*kind.Not); ok && len(kp) == 0 {
		kp = []string{"not"}
	}
	return u + pointer(kp)
}

// pointer renders tokens as an RFC 6901 JSON pointer ("" for the root).
func pointer(tokens []string) string {
	var sb strings.Builder
	for _, t := range tokens {
		sb.WriteByte('/')
		sb.WriteString(strings.NewReplacer("~", "~0", "/", "~1").Replace(t))
	}
	return sb.String()
}
