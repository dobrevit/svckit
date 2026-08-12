package httpx_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dobrevit/svckit/httpx"
)

func TestWriteError(t *testing.T) {
	w := httptest.NewRecorder()
	httpx.WriteError(w, http.StatusForbidden, "Insufficient permissions")

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q", ct)
	}
	var body httpx.ErrorBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body.Error != "Insufficient permissions" || body.Code != "" {
		t.Errorf("body = %+v", body)
	}
}

func TestWriteErrorCode(t *testing.T) {
	w := httptest.NewRecorder()
	httpx.WriteErrorCode(w, http.StatusUnauthorized, "Authentication required", "NO_AUTH_TOKEN")

	var body httpx.ErrorBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body.Code != "NO_AUTH_TOKEN" {
		t.Errorf("code = %q, want NO_AUTH_TOKEN", body.Code)
	}
}

func TestWriteInternalErrorNeverLeaks(t *testing.T) {
	w := httptest.NewRecorder()
	httpx.WriteInternalError(w)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	if got := w.Body.String(); !strings.Contains(got, "Internal server error") {
		t.Errorf("body = %q", got)
	}
}

func TestDecodeJSON(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}

	t.Run("valid", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/", strings.NewReader(`{"name":"ok"}`))
		var p payload
		if err := httpx.DecodeJSON(httptest.NewRecorder(), r, &p); err != nil {
			t.Fatalf("DecodeJSON = %v", err)
		}
		if p.Name != "ok" {
			t.Errorf("name = %q", p.Name)
		}
	})

	t.Run("malformed", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/", strings.NewReader(`{"name":`))
		var p payload
		if err := httpx.DecodeJSON(httptest.NewRecorder(), r, &p); err == nil {
			t.Fatal("expected error for malformed JSON")
		}
	})

	t.Run("trailing garbage", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/", strings.NewReader(`{"name":"a"}{"x":1}`))
		var p payload
		if err := httpx.DecodeJSON(httptest.NewRecorder(), r, &p); err == nil {
			t.Fatal("expected error for trailing garbage")
		}
	})

	t.Run("oversized body", func(t *testing.T) {
		big := `{"name":"` + strings.Repeat("a", httpx.MaxJSONBody) + `"}`
		r := httptest.NewRequest("POST", "/", strings.NewReader(big))
		var p payload
		err := httpx.DecodeJSON(httptest.NewRecorder(), r, &p)
		if err == nil {
			t.Fatal("expected error for oversized body")
		}
		if !strings.Contains(err.Error(), "exceeds") {
			t.Errorf("error = %v, want body-size message", err)
		}
	})
}

func TestPagination(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		wantLimit  int
		wantOffset int
	}{
		{"defaults", "", httpx.DefaultPageLimit, 0},
		{"explicit", "limit=50&offset=10", 50, 10},
		{"clamped to max", "limit=100000", httpx.MaxPageLimit, 0},
		{"zero limit falls back", "limit=0", httpx.DefaultPageLimit, 0},
		{"negative offset clamped", "offset=-5", httpx.DefaultPageLimit, 0},
		{"unparsable falls back", "limit=abc&offset=xyz", httpx.DefaultPageLimit, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/?"+tc.query, nil)
			page := httpx.Pagination(r)
			if page.Limit != tc.wantLimit || page.Offset != tc.wantOffset {
				t.Errorf("Pagination(%q) = %+v, want limit=%d offset=%d",
					tc.query, page, tc.wantLimit, tc.wantOffset)
			}
		})
	}

	t.Run("custom bounds", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/?limit=500", nil)
		page := httpx.PaginationWith(r, 25, 1000)
		if page.Limit != 500 {
			t.Errorf("limit = %d, want 500", page.Limit)
		}
	})
}
