package bustest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// UpdateEventSchemasEnv is the environment variable that makes
// CheckEventSchema rewrite the committed file instead of comparing.
const UpdateEventSchemasEnv = "UPDATE_EVENT_SCHEMAS"

var (
	timeType       = reflect.TypeOf(time.Time{})
	rawMessageType = reflect.TypeOf(json.RawMessage(nil))
	numberType     = reflect.TypeOf(json.Number(""))
)

// schemaFile is the committed on-disk shape.
type schemaFile struct {
	Fields map[string]string `json:"fields"`
}

// CheckEventSchema compares the JSON shape of event (a struct or pointer to
// struct) with the committed file at schemaPath.
//
// The shape is an object {"fields": {"<json name>": "<kind>"}} where kind is
// string, number, bool, object or array. time.Time is string, json.RawMessage
// is object, nested structs are expanded as "<json name>.<child>", fields
// tagged json:"-" are skipped, and omitempty fields carry a trailing "?" (for
// example "string?").
//
// A field removed from the code, or a field whose kind changed, is a breaking
// change and fails. A new field is additive and fails until the committed file
// is refreshed with UPDATE_EVENT_SCHEMAS=1 go test ./... — with that variable
// set CheckEventSchema rewrites the file (and creates it when missing) and
// passes.
func CheckEventSchema(t testing.TB, event any, schemaPath string) {
	t.Helper()

	fields, err := eventFields(event)
	if err != nil {
		t.Fatalf("bustest: event schema for %s: %v", schemaPath, err)
	}

	if os.Getenv(UpdateEventSchemasEnv) == "1" {
		if err := writeSchema(schemaPath, fields); err != nil {
			t.Fatalf("bustest: event schema: write %s: %v", schemaPath, err)
		}
		return
	}

	committed, err := readSchema(schemaPath)
	if err != nil {
		t.Fatalf("bustest: event schema: read %s: %v (refresh with %s=1 go test ./...)",
			schemaPath, err, UpdateEventSchemasEnv)
	}

	var problems []string
	for _, name := range sortedKeys(committed) {
		want := committed[name]
		got, ok := fields[name]
		if !ok {
			problems = append(problems, fmt.Sprintf(
				"field %q: committed kind %q, code kind %q — breaking change", name, want, "missing"))
			continue
		}
		if got != want {
			problems = append(problems, fmt.Sprintf(
				"field %q: committed kind %q, code kind %q — breaking change", name, want, got))
		}
	}
	for _, name := range sortedKeys(fields) {
		if _, ok := committed[name]; ok {
			continue
		}
		problems = append(problems, fmt.Sprintf(
			"field %q (kind %q) is new and additive; refresh the committed file with %s=1 go test ./...",
			name, fields[name], UpdateEventSchemasEnv))
	}
	if len(problems) > 0 {
		t.Fatalf("bustest: event schema %s does not match the event's JSON shape:\n%s",
			schemaPath, strings.Join(problems, "\n"))
	}
}

// eventFields derives the shape of event from its struct's JSON tags.
func eventFields(event any) (map[string]string, error) {
	if event == nil {
		return nil, errors.New("event is nil")
	}
	t := reflect.TypeOf(event)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("event must be a struct or pointer to struct, got %s", t)
	}
	fields := make(map[string]string)
	addFields(fields, "", t, false)
	return fields, nil
}

// addFields records every JSON field of t, prefixing nested struct fields with
// their parent's JSON name. optional marks children of an omitempty field.
func addFields(fields map[string]string, prefix string, t reflect.Type, optional bool) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" { // unexported
			continue
		}
		name, omitempty, skip := jsonFieldName(f)
		if skip {
			continue
		}
		if prefix != "" {
			name = prefix + "." + name
		}
		kind, nested := describeType(f.Type)
		if nested != nil {
			addFields(fields, name, nested, optional || omitempty)
			continue
		}
		if optional || omitempty {
			kind += "?"
		}
		fields[name] = kind
	}
}

// jsonFieldName reads the encoding/json tag of f.
func jsonFieldName(f reflect.StructField) (name string, omitempty, skip bool) {
	tag := f.Tag.Get("json")
	if tag == "-" {
		return "", false, true
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	if name == "-" {
		return "", false, true
	}
	for _, opt := range parts[1:] {
		if opt == "omitempty" {
			omitempty = true
		}
	}
	if name == "" {
		name = f.Name
	}
	return name, omitempty, false
}

// describeType returns the JSON kind of t, or a struct type whose fields are
// expanded as dotted keys.
func describeType(t reflect.Type) (kind string, nested reflect.Type) {
	switch t {
	case timeType:
		return "string", nil
	case rawMessageType:
		return "object", nil
	case numberType:
		return "number", nil
	}
	switch t.Kind() {
	case reflect.Pointer:
		return describeType(t.Elem())
	case reflect.Struct:
		return "object", t
	case reflect.Bool:
		return "bool", nil
	case reflect.String:
		return "string", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return "number", nil
	case reflect.Slice, reflect.Array:
		return "array", nil
	case reflect.Map, reflect.Interface:
		return "object", nil
	default:
		return "object", nil
	}
}

func writeSchema(path string, fields map[string]string) error {
	data, err := json.MarshalIndent(schemaFile{Fields: fields}, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func readSchema(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc schemaFile
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if doc.Fields == nil {
		return nil, errors.New(`missing "fields" object`)
	}
	return doc.Fields, nil
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
