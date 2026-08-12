package middleware_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/dobrevit/svckit/middleware"
)

// Middleware is func(http.Handler) http.Handler, so it composes with the
// standard library and with any router that speaks the same shape.
func ExampleCORS() {
	routes := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})

	handler := middleware.CORSWithOrigins([]string{"https://app.example.com"})(routes)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/orders", nil)
	r.Header.Set("Origin", "https://app.example.com")
	handler.ServeHTTP(w, r)

	fmt.Println(w.Header().Get("Access-Control-Allow-Origin"))
	// Output: https://app.example.com
}

// A preflight request is answered by the middleware and never reaches the
// handler behind it.
func ExampleCORS_preflight() {
	reached := false
	routes := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true })

	handler := middleware.CORSWithOrigins([]string{"https://app.example.com"})(routes)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodOptions, "/orders", nil)
	r.Header.Set("Origin", "https://app.example.com")
	handler.ServeHTTP(w, r)

	fmt.Println(w.Code, reached)
	// Output: 204 false
}

// Routers know their own patterns and the standard request does not carry
// one, so a router adapter records it. Metrics and logging then label by
// template instead of by concrete path, which keeps the series count bounded.
func ExampleWithRoute() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(middleware.WithRoute(r.Context(), "/orders/{id}"))
		fmt.Println(middleware.Route(r))
	})

	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/orders/42", nil))
	// Output: /orders/{id}
}
