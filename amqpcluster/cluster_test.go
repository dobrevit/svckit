package amqpcluster

import (
	"strings"
	"testing"
	"time"
)

// newTestAdapter builds an adapter over the given nodes without connecting to
// anything: node selection, health tracking and load balancing are all
// decisions made before a connection is used.
func newTestAdapter(config ClusterConfig, nodes ...*Node) *ClusterAdapter {
	return &ClusterAdapter{config: config, nodes: nodes}
}

func node(name string, state NodeState) *Node {
	return &Node{Name: name, URL: "amqp://guest:guest@" + name + ":5672/", state: state}
}

func TestOnlyHealthyNodesAreSelected(t *testing.T) {
	healthy := node("rabbit-2", NodeHealthy)
	ca := newTestAdapter(
		ClusterConfig{LoadBalanceStrategy: "round_robin"},
		node("rabbit-1", NodeFailed),
		healthy,
		node("rabbit-3", NodeFailed),
	)

	for range 5 {
		selected := ca.selectNodeForPublish("")
		if selected != healthy {
			t.Fatalf("selected %v, want the only healthy node", selected)
		}
	}
}

// A degraded node is not healthy: it is still failing often enough that
// sending it traffic is a choice, not a default.
func TestDegradedNodesAreNotSelected(t *testing.T) {
	ca := newTestAdapter(
		ClusterConfig{LoadBalanceStrategy: "round_robin"},
		node("rabbit-1", NodeDegraded),
		node("rabbit-2", NodeFailed),
	)

	if selected := ca.selectNodeForPublish(""); selected != nil {
		t.Errorf("selected %q, want nothing when no node is healthy", selected.Name)
	}
}

func TestNoHealthyNodeSelectsNothing(t *testing.T) {
	ca := newTestAdapter(ClusterConfig{LoadBalanceStrategy: "round_robin"})

	if selected := ca.selectNodeForPublish(""); selected != nil {
		t.Errorf("selected %v from an empty cluster", selected)
	}
}

func TestRoundRobinSpreadsAcrossHealthyNodes(t *testing.T) {
	ca := newTestAdapter(
		ClusterConfig{LoadBalanceStrategy: "round_robin"},
		node("rabbit-1", NodeHealthy),
		node("rabbit-2", NodeHealthy),
		node("rabbit-3", NodeHealthy),
	)

	counts := map[string]int{}
	for range 9 {
		counts[ca.selectNodeForPublish("").Name]++
	}

	for _, name := range []string{"rabbit-1", "rabbit-2", "rabbit-3"} {
		if counts[name] != 3 {
			t.Errorf("distribution = %v, want each node three times", counts)
			break
		}
	}
}

// Hashing exists so that all messages for one routing key land on one node;
// if it did not, ordering per key would be lost.
func TestHashStrategyIsStableForARoutingKey(t *testing.T) {
	ca := newTestAdapter(
		ClusterConfig{LoadBalanceStrategy: "hash"},
		node("rabbit-1", NodeHealthy),
		node("rabbit-2", NodeHealthy),
		node("rabbit-3", NodeHealthy),
	)

	first := ca.selectNodeForPublish("order.created").Name
	for range 10 {
		if got := ca.selectNodeForPublish("order.created").Name; got != first {
			t.Fatalf("the same routing key selected %q then %q", first, got)
		}
	}
}

// Different keys should not all pile onto one node.
func TestHashStrategySpreadsDifferentKeys(t *testing.T) {
	ca := newTestAdapter(
		ClusterConfig{LoadBalanceStrategy: "hash"},
		node("rabbit-1", NodeHealthy),
		node("rabbit-2", NodeHealthy),
		node("rabbit-3", NodeHealthy),
	)

	seen := map[string]bool{}
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		seen[ca.selectNodeForPublish(key).Name] = true
	}

	if len(seen) < 2 {
		t.Errorf("every routing key hashed to the same node: %v", seen)
	}
}

// With no routing key there is nothing to hash, so selection has to fall back
// rather than always pick the same node.
func TestHashStrategyFallsBackWithoutARoutingKey(t *testing.T) {
	ca := newTestAdapter(
		ClusterConfig{LoadBalanceStrategy: "hash"},
		node("rabbit-1", NodeHealthy),
		node("rabbit-2", NodeHealthy),
	)

	seen := map[string]bool{}
	for range 6 {
		seen[ca.selectNodeForPublish("").Name] = true
	}

	if len(seen) < 2 {
		t.Errorf("selection without a routing key always chose %v", seen)
	}
}

func TestNodeStateStringsAreDistinct(t *testing.T) {
	states := map[NodeState]bool{NodeHealthy: true, NodeDegraded: true, NodeFailed: true}
	if len(states) != 3 {
		t.Error("the node states are not distinct values")
	}
}

func TestConfigValidation(t *testing.T) {
	valid := func() *Config {
		return &Config{
			Nodes:               []string{"amqp://guest:guest@localhost:5672/"},
			ServiceName:         "orders",
			HealthCheckInterval: 30 * time.Second,
			RetryAttempts:       3,
			LoadBalanceStrategy: "round_robin",
		}
	}

	if err := valid().Validate(); err != nil {
		t.Fatalf("a complete config was rejected: %v", err)
	}

	cases := map[string]func(*Config){
		"no nodes":               func(c *Config) { c.Nodes = nil },
		"no service name":        func(c *Config) { c.ServiceName = "" },
		"no health interval":     func(c *Config) { c.HealthCheckInterval = 0 },
		"no retries":             func(c *Config) { c.RetryAttempts = 0 },
		"unknown load balancing": func(c *Config) { c.LoadBalanceStrategy = "sometimes" },
	}
	for name, break_ := range cases {
		t.Run(name, func(t *testing.T) {
			config := valid()
			break_(config)
			if err := config.Validate(); err == nil {
				t.Errorf("a config with %s was accepted", name)
			}
		})
	}
}

func TestLoadConfigFromEnvReadsClusterNodes(t *testing.T) {
	t.Setenv("RABBITMQ_CLUSTER_NODES", " amqp://a:5672/ , amqp://b:5672/ ")

	config, err := LoadConfigFromEnv("orders")
	if err != nil {
		t.Fatalf("LoadConfigFromEnv: %v", err)
	}

	if len(config.Nodes) != 2 {
		t.Fatalf("Nodes = %v, want two", config.Nodes)
	}
	// Surrounding whitespace in the list must not become part of the URL.
	for _, n := range config.Nodes {
		if strings.TrimSpace(n) != n {
			t.Errorf("node %q carries surrounding whitespace", n)
		}
	}
	if config.ServiceName != "orders" {
		t.Errorf("ServiceName = %q", config.ServiceName)
	}
	if err := config.Validate(); err != nil {
		t.Errorf("a config loaded from the environment does not validate: %v", err)
	}
}

func TestLoadConfigFromEnvFallsBackToASingleURL(t *testing.T) {
	t.Setenv("RABBITMQ_CLUSTER_NODES", "")
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	config, err := LoadConfigFromEnv("orders")
	if err != nil {
		t.Fatalf("LoadConfigFromEnv: %v", err)
	}
	if len(config.Nodes) != 1 || config.Nodes[0] != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("Nodes = %v", config.Nodes)
	}
}

// Credentials must not reach a log line: String is what gets printed at boot.
func TestStringMasksCredentials(t *testing.T) {
	config := &Config{
		Nodes:       []string{"amqp://svc:sup3rs3cret@rabbit-1:5672/"},
		ServiceName: "orders",
	}

	rendered := config.String()

	if strings.Contains(rendered, "sup3rs3cret") {
		t.Errorf("the password appears in %q", rendered)
	}
	// The username and host stay, or the line is useless for diagnosis.
	if !strings.Contains(rendered, "svc") || !strings.Contains(rendered, "rabbit-1") {
		t.Errorf("masking removed too much: %q", rendered)
	}
}

func TestMaskCredentialsLeavesURLsWithoutCredentialsAlone(t *testing.T) {
	plain := "amqp://rabbit-1:5672/"
	if got := maskCredentials(plain); got != plain {
		t.Errorf("maskCredentials(%q) = %q, want it unchanged", plain, got)
	}
}
