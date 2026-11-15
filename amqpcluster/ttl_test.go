package amqpcluster

import (
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
)

func TestSubscriptionTTLArguments(t *testing.T) {
	cases := []struct {
		name string
		ttl  time.Duration
		want any // nil means no x-message-ttl
	}{
		// Exactly what every existing queue was declared with.
		{"default", 0, int64(300000)},
		{"chosen", 90 * time.Second, int64(90000)},
		{"none", eventbus.NoQueueTTL, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ca, nodes, _ := clusterOf(t, 1, "rabbit-1")
			cs := &ClusterSubscriber{cluster: ca, exchange: "events"}

			if err := cs.setupQueue("orders", "order.*", SubscriptionOptions{Durable: true, TTL: tc.ttl}); err != nil {
				t.Fatalf("setupQueue: %v", err)
			}

			args := subscribeChannel(nodes[0]).queueArgs["orders"]
			got, ok := args["x-message-ttl"]
			if tc.want == nil {
				if ok {
					t.Errorf("x-message-ttl = %#v, want none", got)
				}
				return
			}
			if got != tc.want {
				t.Errorf("x-message-ttl = %#v, want %#v", got, tc.want)
			}
		})
	}
}
