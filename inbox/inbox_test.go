package inbox

import (
	"context"
	"os"
	"testing"
)

func TestSchemaSQLIsTheDefaultSchema(t *testing.T) {
	raw, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != schemaHeader+Schema(DefaultTable) {
		t.Errorf("schema.sql differs from Schema(DefaultTable):\n%s%s", schemaHeader, Schema(DefaultTable))
	}
}

func TestTableNamesAreIdentifiers(t *testing.T) {
	for _, bad := range []string{"x; DROP TABLE y", "a.b.c", "1x"} {
		if _, err := New(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if _, err := New("messaging.inbox"); err != nil {
		t.Error(err)
	}
}

func TestAClaimNeedsAnIDAndAConsumer(t *testing.T) {
	if _, err := Claim(context.Background(), nil, "", "billing"); err == nil {
		t.Error("claimed an empty id")
	}
	if _, err := Claim(context.Background(), nil, "m-1", ""); err == nil {
		t.Error("claimed for no consumer")
	}
}
