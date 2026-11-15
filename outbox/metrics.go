package outbox

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	relayed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "svckit_outbox_relayed_total",
		Help: "Outbox messages the relay handled, by outcome: published, returned, nacked, timeout, failed or expired.",
	}, []string{"table", "outcome"})

	backlog = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "svckit_outbox_backlog",
		Help: "Outbox messages neither published nor expired.",
	}, []string{"table"})

	oldestAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "svckit_outbox_oldest_unpublished_age_seconds",
		Help: "Age of the oldest outbox message neither published nor expired.",
	}, []string{"table"})
)
