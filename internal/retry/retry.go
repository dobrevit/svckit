// Package retry is the delayed, bounded retry both subscribers use when a
// subscription has a RetryPolicy (eventbus.RetryPolicy is an alias of
// Policy): the queues it needs, and where a failed message goes next.
//
// A message whose handler fails is not put back at the head of its queue.
// A retryable failure is republished to a retry queue whose TTL is the delay
// for that attempt and which dead-letters back into the main queue; a
// permanent failure, or the last attempt, goes to the dead-letter queue. The
// attempt number travels in the AttemptHeader header.
//
// Everything is routed through the default exchange, by queue name: retries
// and dead letters never pass through the subscription's topic exchange, so
// another binding on it (a "#" audit tap) does not receive them.
package retry

import (
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// AttemptHeader carries the attempt that will handle the message: absent
	// on the first delivery, 2 on the first retry, and so on. On a dead
	// letter it is the attempt that failed last.
	AttemptHeader = "x-svckit-attempt"

	// ReasonHeader on a dead letter says why it was dead-lettered:
	// "permanent" (the handler said so) or "attempts_exhausted".
	ReasonHeader = "x-svckit-reason"

	ReasonPermanent         = "permanent"
	ReasonAttemptsExhausted = "attempts_exhausted"
)

// Policy is how a subscription retries a message whose handler failed with
// a retryable error.
type Policy struct {
	// Delays are the waits before each retry: the retry after attempt n
	// waits Delays[min(n-1, len(Delays)-1)], so the last delay repeats. One
	// retry queue is declared per distinct delay.
	Delays []time.Duration

	// MaxAttempts is how many times the handler is called at most, the
	// first delivery included. After the last, the message is dead-lettered.
	MaxAttempts int
}

// Validate reports a policy that cannot work.
func (p Policy) Validate() error {
	if p.MaxAttempts < 1 {
		return errors.New("retry policy: MaxAttempts must be at least 1")
	}
	if p.MaxAttempts > 1 && len(p.Delays) == 0 {
		return errors.New("retry policy: Delays must not be empty when MaxAttempts allows a retry")
	}
	for _, d := range p.Delays {
		if d < time.Millisecond {
			return fmt.Errorf("retry policy: delay %v is shorter than a millisecond", d)
		}
	}
	return nil
}

// DelayAfter is the wait before the retry that follows attempt n.
func (p Policy) DelayAfter(attempt int) time.Duration {
	i := attempt - 1
	if i >= len(p.Delays) {
		i = len(p.Delays) - 1
	}
	if i < 0 {
		i = 0
	}
	return p.Delays[i]
}

// DLQName is the dead-letter queue of queue.
func DLQName(queue string) string { return queue + ".dlq" }

// QueueName is the retry queue of queue for one delay.
func QueueName(queue string, delay time.Duration) string {
	return fmt.Sprintf("%s.retry.%s", queue, delay)
}

// MainQueueArgs sets args (the main queue's other arguments) to dead-letter
// into the DLQ through the default exchange, so a message the handler never
// sees (expired, or rejected before it) also lands in the DLQ and not on the
// topic exchange.
func MainQueueArgs(queue string, args amqp.Table) amqp.Table {
	out := amqp.Table{}
	for k, v := range args {
		out[k] = v
	}
	out["x-dead-letter-exchange"] = ""
	out["x-dead-letter-routing-key"] = DLQName(queue)
	return out
}

// RetryQueues are the retry queues of queue, by name, with their arguments:
// the delay as their TTL, and dead-lettering back into queue.
func RetryQueues(queue string, p Policy) map[string]amqp.Table {
	queues := make(map[string]amqp.Table)
	for _, d := range p.Delays {
		queues[QueueName(queue, d)] = amqp.Table{
			"x-message-ttl":             int64(d / time.Millisecond),
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": queue,
		}
	}
	return queues
}

// Attempt is the attempt a delivery is: its AttemptHeader, or 1.
func Attempt(d amqp.Delivery) int {
	switch v := d.Headers[AttemptHeader].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	}
	return 1
}

// Next says where a message whose handler failed goes, as a republish
// through the default exchange: to the retry queue for its attempt, with
// the attempt incremented, or to the DLQ with the attempt and the reason.
func (p Policy) Next(d amqp.Delivery, queue string, retryable bool) (routingKey string, msg amqp.Publishing) {
	attempt := Attempt(d)
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}

	if retryable && attempt < p.MaxAttempts {
		headers[AttemptHeader] = int32(attempt + 1)
		return QueueName(queue, p.DelayAfter(attempt)), republish(d, headers)
	}

	headers[AttemptHeader] = int32(attempt)
	headers[ReasonHeader] = ReasonAttemptsExhausted
	if !retryable {
		headers[ReasonHeader] = ReasonPermanent
	}
	return DLQName(queue), republish(d, headers)
}

func republish(d amqp.Delivery, headers amqp.Table) amqp.Publishing {
	return amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    amqp.Persistent,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		UserId:          d.UserId,
		AppId:           d.AppId,
		Body:            d.Body,
	}
}
