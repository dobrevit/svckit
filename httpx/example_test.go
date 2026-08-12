package httpx_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/dobrevit/svckit/httpx"
)

// Pagination clamps the caller's request into a range the handler can serve,
// so an unbounded ?limit= cannot ask for the whole table.
func ExamplePagination() {
	r := httptest.NewRequest(http.MethodGet, "/orders?limit=100000&offset=40", nil)

	page := httpx.Pagination(r)

	fmt.Printf("limit=%d offset=%d\n", page.Limit, page.Offset)
	// Output: limit=100 offset=40
}

// WriteError sends a client-facing message. Internal error text never reaches
// the response body: use WriteInternalError for that, which logs the cause and
// tells the client nothing beyond the status.
func ExampleWriteError() {
	w := httptest.NewRecorder()

	httpx.WriteError(w, http.StatusNotFound, "order not found")

	fmt.Println(w.Code)
	fmt.Println(w.Body.String())
	// Output:
	// 404
	// {"error":"order not found"}
}
