package ginx_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/dobrevit/svckit/ginx"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestInternalErrorNeverLeaks(t *testing.T) {
	router := gin.New()
	router.GET("/boom", func(c *gin.Context) {
		ginx.InternalError(c, errors.New("pq: duplicate key value violates unique constraint"))
	})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/boom", nil))

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "duplicate key") || strings.Contains(body, "pq:") {
		t.Errorf("internal error leaked to client: %s", body)
	}
	if !strings.Contains(body, "Internal server error") {
		t.Errorf("body = %s, want canonical message", body)
	}
}

func TestBind(t *testing.T) {
	type req struct {
		Name string `json:"name" binding:"required"`
	}

	router := gin.New()
	router.POST("/things", func(c *gin.Context) {
		var in req
		if !ginx.Bind(c, &in) {
			return
		}
		ginx.Created(c, gin.H{"name": in.Name})
	})

	t.Run("valid body", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/things", strings.NewReader(`{"name":"x"}`))
		router.ServeHTTP(w, r)
		if w.Code != http.StatusCreated {
			t.Errorf("status = %d, want 201 (body %s)", w.Code, w.Body.String())
		}
	})

	t.Run("invalid body", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/things", strings.NewReader(`{`))
		router.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", w.Code)
		}
		if !strings.Contains(w.Body.String(), "Invalid request body") {
			t.Errorf("body = %s", w.Body.String())
		}
	})

	t.Run("missing required field", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/things", strings.NewReader(`{}`))
		router.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", w.Code)
		}
	})
}

func TestPagination(t *testing.T) {
	router := gin.New()
	router.GET("/list", func(c *gin.Context) {
		page := ginx.Pagination(c)
		ginx.OK(c, gin.H{"limit": page.Limit, "offset": page.Offset})
	})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/list?limit=99999&offset=-3", nil))
	body := w.Body.String()
	if !strings.Contains(body, `"limit":100`) || !strings.Contains(body, `"offset":0`) {
		t.Errorf("body = %s, want clamped limit=100 offset=0", body)
	}
}
