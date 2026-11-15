package app

import (
	"context"
	"net"
	"net/http"
	"testing"
)

// The server Run starts on the operations listener serves OpsHandler.
func TestTheOpsServerServesMetrics(t *testing.T) {
	a := &App{Name: "test-service", OpsAddr: "127.0.0.1:0"}
	srv := a.opsServer()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown(context.Background()) }()

	resp, err := http.Get("http://" + ln.Addr().String() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /metrics on the ops server = %d", resp.StatusCode)
	}
}
