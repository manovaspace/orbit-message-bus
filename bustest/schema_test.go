package bustest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type schemaAddress struct {
	City string `json:"city"`
	Zip  int    `json:"zip"`
}

type schemaSample struct {
	Name    string          `json:"name"`
	Count   int             `json:"count"`
	Note    string          `json:"note,omitempty"`
	Secret  string          `json:"-"`
	Ignored int             `json:"-"`
	Home    schemaAddress   `json:"home"`
	Raw     json.RawMessage `json:"raw"`
	At      time.Time       `json:"at"`
}

func TestCheckEventSchemaIdentical(t *testing.T) {
	path := filepath.Join(t.TempDir(), "event.schema.json")
	writeCommitted(t, path, mustFields(t, schemaSample{}))

	CheckEventSchema(t, schemaSample{}, path)
	CheckEventSchema(t, &schemaSample{}, path)
}

func TestCheckEventSchemaRemovedField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "event.schema.json")
	fields := mustFields(t, schemaSample{})
	fields["legacy"] = "string"
	writeCommitted(t, path, fields)

	msg := expectSchemaFailure(t, schemaSample{}, path)
	for _, want := range []string{`"legacy"`, `"string"`, `"missing"`, "breaking change"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("failure message %q does not contain %q", msg, want)
		}
	}
}

func TestCheckEventSchemaRetypedField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "event.schema.json")
	fields := mustFields(t, schemaSample{})
	fields["name"] = "number"
	writeCommitted(t, path, fields)

	msg := expectSchemaFailure(t, schemaSample{}, path)
	for _, want := range []string{`"name"`, `"number"`, `"string"`, "breaking change"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("failure message %q does not contain %q", msg, want)
		}
	}
}

func TestCheckEventSchemaAddedField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "event.schema.json")
	fields := mustFields(t, schemaSample{})
	delete(fields, "name")
	writeCommitted(t, path, fields)

	msg := expectSchemaFailure(t, schemaSample{}, path)
	for _, want := range []string{`"name"`, "additive", "refresh", "UPDATE_EVENT_SCHEMAS=1"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("failure message %q does not contain %q", msg, want)
		}
	}
}

func TestCheckEventSchemaUpdateWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "event.schema.json")
	t.Setenv(UpdateEventSchemasEnv, "1")
	CheckEventSchema(t, schemaSample{}, path)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read refreshed schema: %v", err)
	}
	if !strings.HasPrefix(string(data), "{\n  \"fields\": {\n") {
		t.Fatalf("schema is not indented JSON:\n%s", data)
	}
	if !strings.HasSuffix(string(data), "}\n") {
		t.Fatalf("schema does not end with a newline:\n%s", data)
	}
	want, _ := json.MarshalIndent(schemaFile{Fields: mustFields(t, schemaSample{})}, "", "  ")
	if strings.TrimSpace(string(data)) != strings.TrimSpace(string(want)) {
		t.Fatalf("refreshed schema =\n%s\nwant\n%s", data, want)
	}

	t.Setenv(UpdateEventSchemasEnv, "")
	CheckEventSchema(t, schemaSample{}, path)
}

func TestCheckEventSchemaMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "event.schema.json")
	msg := expectSchemaFailure(t, schemaSample{}, path)
	if !strings.Contains(msg, "UPDATE_EVENT_SCHEMAS=1") {
		t.Fatalf("missing-file message %q does not mention the refresh variable", msg)
	}
}

func TestCheckEventSchemaOmitempty(t *testing.T) {
	fields := mustFields(t, schemaSample{})
	if got := fields["note"]; got != "string?" {
		t.Fatalf("note kind = %q, want %q", got, "string?")
	}
}

func TestCheckEventSchemaNestedStruct(t *testing.T) {
	fields := mustFields(t, schemaSample{})
	if got := fields["home.city"]; got != "string" {
		t.Fatalf("home.city kind = %q, want string", got)
	}
	if got := fields["home.zip"]; got != "number" {
		t.Fatalf("home.zip kind = %q, want number", got)
	}
	if _, ok := fields["home"]; ok {
		t.Fatal("nested struct was not expanded: home has its own entry")
	}
	if got := fields["raw"]; got != "object" {
		t.Fatalf("json.RawMessage kind = %q, want object", got)
	}
	if got := fields["at"]; got != "string" {
		t.Fatalf("time.Time kind = %q, want string", got)
	}
}

func TestCheckEventSchemaSkipsIgnoredFields(t *testing.T) {
	fields := mustFields(t, schemaSample{})
	for _, name := range []string{"Secret", "Ignored", "-"} {
		if _, ok := fields[name]; ok {
			t.Fatalf("field %q tagged json:\"-\" appears in the schema", name)
		}
	}
}

func TestCheckEventSchemaRejectsNonStruct(t *testing.T) {
	path := filepath.Join(t.TempDir(), "event.schema.json")
	msg := expectSchemaFailure(t, 42, path)
	if !strings.Contains(msg, "struct") {
		t.Fatalf("non-struct message %q does not name the expected input", msg)
	}
}

// --- helpers ---------------------------------------------------------------

func mustFields(t *testing.T, event any) map[string]string {
	t.Helper()
	fields, err := eventFields(event)
	if err != nil {
		t.Fatalf("event fields: %v", err)
	}
	return fields
}

func writeCommitted(t *testing.T, path string, fields map[string]string) {
	t.Helper()
	if err := writeSchema(path, fields); err != nil {
		t.Fatalf("write schema: %v", err)
	}
}

// fatalRecord is the panic value fakeTB raises from Fatalf so a test can
// assert a failure without failing itself.
type fatalRecord struct{}

// fakeTB implements testing.TB by embedding the interface; CheckEventSchema
// only calls Helper and Fatalf, which are shadowed here.
type fakeTB struct {
	testing.TB
	messages []string
}

func (f *fakeTB) Helper() {}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.messages = append(f.messages, fmt.Sprintf(format, args...))
	panic(fatalRecord{})
}

func expectSchemaFailure(t *testing.T, event any, path string) string {
	t.Helper()
	fake := &fakeTB{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(fatalRecord); !ok {
					panic(r)
				}
			}
		}()
		CheckEventSchema(fake, event, path)
	}()
	if len(fake.messages) == 0 {
		t.Fatal("CheckEventSchema passed, want a failure")
	}
	return strings.Join(fake.messages, "\n")
}
