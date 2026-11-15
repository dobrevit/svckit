package outbox

import (
	"os"
	"testing"
)

// schema.sql is what Schema(DefaultTable) returns, so the documented DDL and
// the one the code knows cannot drift.
func TestSchemaSQLIsTheDefaultSchema(t *testing.T) {
	raw, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != schemaHeader+Schema(DefaultTable) {
		t.Errorf("schema.sql differs from Schema(DefaultTable); regenerate it:\n%s%s", schemaHeader, Schema(DefaultTable))
	}
}
