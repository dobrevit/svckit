package eventbus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// RabbitMQ event metrics
var (
	// EventsPublished tracks total events published
	EventsPublished = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rabbitmq_events_published_total",
			Help: "Total number of events published to RabbitMQ",
		},
		[]string{"event_type", "service", "status"},
	)

	// EventsReceived tracks total events received
	EventsReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rabbitmq_events_received_total",
			Help: "Total number of events received from RabbitMQ",
		},
		[]string{"event_type", "service", "queue"},
	)

	// EventsProcessed tracks total events processed
	EventsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rabbitmq_events_processed_total",
			Help: "Total number of events successfully processed",
		},
		[]string{"event_type", "service", "queue", "status"},
	)

	// EventProcessingDuration tracks event processing time
	EventProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "rabbitmq_event_processing_duration_seconds",
			Help:    "Duration of event processing in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"event_type", "service", "queue"},
	)

	// EventsInDLQ tracks events sent to dead letter queue
	EventsInDLQ = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rabbitmq_events_dlq_total",
			Help: "Total number of events sent to dead letter queue",
		},
		[]string{"event_type", "service", "queue", "reason"},
	)

	// EventsRequeued tracks events requeued for retry
	EventsRequeued = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rabbitmq_events_requeued_total",
			Help: "Total number of events requeued for retry",
		},
		[]string{"event_type", "service", "queue"},
	)

	// EventsAcknowledged tracks events acknowledged
	EventsAcknowledged = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rabbitmq_events_acknowledged_total",
			Help: "Total number of events acknowledged",
		},
		[]string{"event_type", "service", "queue"},
	)

	// QueueDepth tracks current queue depth (gauge)
	QueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rabbitmq_queue_depth",
			Help: "Current depth of RabbitMQ queues",
		},
		[]string{"queue", "service"},
	)

	// ConnectionStatus tracks connection status
	ConnectionStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rabbitmq_connection_status",
			Help: "RabbitMQ connection status (1 = connected, 0 = disconnected)",
		},
		[]string{"service", "connection_type"},
	)
)

// RecordEventPublished records an event publication
func RecordEventPublished(eventType, service, status string) {
	EventsPublished.WithLabelValues(eventType, service, status).Inc()
}

// RecordEventReceived records an event reception
func RecordEventReceived(eventType, service, queue string) {
	EventsReceived.WithLabelValues(eventType, service, queue).Inc()
}

// RecordEventProcessed records an event processing result
func RecordEventProcessed(eventType, service, queue, status string) {
	EventsProcessed.WithLabelValues(eventType, service, queue, status).Inc()
}

// RecordEventDLQ records an event sent to DLQ
func RecordEventDLQ(eventType, service, queue, reason string) {
	EventsInDLQ.WithLabelValues(eventType, service, queue, reason).Inc()
}

// RecordEventRequeued records an event requeue
func RecordEventRequeued(eventType, service, queue string) {
	EventsRequeued.WithLabelValues(eventType, service, queue).Inc()
}

// RecordEventAcknowledged records an event acknowledgment
func RecordEventAcknowledged(eventType, service, queue string) {
	EventsAcknowledged.WithLabelValues(eventType, service, queue).Inc()
}

// UpdateQueueDepth updates the current queue depth
func UpdateQueueDepth(queue, service string, depth float64) {
	QueueDepth.WithLabelValues(queue, service).Set(depth)
}

// UpdateConnectionStatus updates the connection status
func UpdateConnectionStatus(service, connectionType string, connected bool) {
	status := float64(0)
	if connected {
		status = 1
	}
	ConnectionStatus.WithLabelValues(service, connectionType).Set(status)
}
