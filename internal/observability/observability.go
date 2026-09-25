// Package observability provides the JSON logger and Prometheus metrics.
package observability

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
)

// NewLogger returns a JSON slog logger tagged with the instance id.
func NewLogger(cfg config.Config) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})).
		With(slog.String("instanceId", cfg.InstanceID))
}

// Metrics groups every Prometheus collector of the service. Each instance
// owns its registry, so tests can build several apps in one process.
type Metrics struct {
	Registry *prometheus.Registry

	transactions         *prometheus.CounterVec
	duplicates           *prometheus.CounterVec
	idempotencyConflicts prometheus.Counter
	concurrencyConflicts prometheus.Counter
	latency              *prometheus.HistogramVec
	referenceRetries     prometheus.Counter
	reconciliations      *prometheus.CounterVec
	reconDivergences     prometheus.Counter
	sqsMessages          *prometheus.CounterVec
	sqsRetries           prometheus.Counter
	sqsDLQ               *prometheus.CounterVec
	outboxPublished      prometheus.Counter
	outboxFailures       prometheus.Counter
	outboxPending        prometheus.Gauge
	outboxLag            prometheus.Gauge
	outboxDelay          prometheus.Histogram
	httpRequests         *prometheus.CounterVec
	breakerState         *prometheus.GaugeVec
	breakerRejections    *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		transactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_transactions_total", Help: "Wager operations by source, kind, resulting status and replay flag.",
		}, []string{"source", "kind", "status", "replay"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_duplicates_total", Help: "Repeated deliveries answered from persisted state (idempotent replay or inbox).",
		}, []string{"source"}),
		idempotencyConflicts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wager_idempotency_conflicts_total", Help: "Idempotency key or external id reused with a different payload.",
		}),
		concurrencyConflicts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_concurrency_conflicts_total", Help: "Transactions retried after a serialization/version/deadlock conflict.",
		}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "wager_processing_seconds", Help: "Processing latency of wager operations.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}, []string{"source"}),
		referenceRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wager_reference_retries_total", Help: "Rescheduled attempts of PENDING_REFERENCE operations.",
		}),
		reconciliations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_reconciliations_total", Help: "Wallet reconciliations by result.",
		}, []string{"consistent"}),
		reconDivergences: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_reconciliation_divergences_total", Help: "Reconciliations where stored and ledger balances differ.",
		}),
		sqsMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sqs_messages_total", Help: "Consumed SQS messages by outcome.",
		}, []string{"outcome"}),
		sqsRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sqs_message_retries_total", Help: "Messages returned to the queue with backoff after a transient failure.",
		}),
		sqsDLQ: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sqs_dlq_messages_total", Help: "Messages moved to the dead-letter queue by reason.",
		}, []string{"reason"}),
		outboxPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_published_total", Help: "Outbox events published.",
		}),
		outboxFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_publish_failures_total", Help: "Failed outbox publication attempts (retried with backoff).",
		}),
		outboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_pending_events", Help: "Unpublished outbox events.",
		}),
		outboxLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_lag_seconds", Help: "Age of the oldest unpublished outbox event.",
		}),
		outboxDelay: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "outbox_publish_delay_seconds", Help: "Delay between event occurrence and publication.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
		}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total", Help: "HTTP requests by route and status code.",
		}, []string{"route", "code"}),
		breakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "circuit_breaker_state", Help: "Circuit breaker state per dependency: 0 closed, 1 half-open, 2 open.",
		}, []string{"name"}),
		breakerRejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "circuit_breaker_rejections_total", Help: "Calls rejected without touching the dependency because its breaker was open.",
		}, []string{"name"}),
	}
	m.Registry.MustRegister(
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.transactions, m.duplicates, m.idempotencyConflicts, m.concurrencyConflicts, m.latency,
		m.referenceRetries, m.reconciliations, m.reconDivergences, m.sqsMessages, m.sqsRetries, m.sqsDLQ,
		m.outboxPublished, m.outboxFailures, m.outboxPending, m.outboxLag, m.outboxDelay, m.httpRequests,
		m.breakerState, m.breakerRejections,
	)
	return m
}

func (m *Metrics) TransactionResult(source, kind, status string, replay bool) {
	m.transactions.WithLabelValues(source, kind, status, strconv.FormatBool(replay)).Inc()
}
func (m *Metrics) Duplicate(source string) { m.duplicates.WithLabelValues(source).Inc() }
func (m *Metrics) IdempotencyConflict()    { m.idempotencyConflicts.Inc() }
func (m *Metrics) ConcurrencyConflict()    { m.concurrencyConflicts.Inc() }
func (m *Metrics) ProcessingLatency(source string, d time.Duration) {
	m.latency.WithLabelValues(source).Observe(d.Seconds())
}
func (m *Metrics) ReferenceRetry() { m.referenceRetries.Inc() }
func (m *Metrics) ReconciliationChecked(consistent bool) {
	m.reconciliations.WithLabelValues(strconv.FormatBool(consistent)).Inc()
	if !consistent {
		m.reconDivergences.Inc()
	}
}
func (m *Metrics) SQSMessage(outcome string)   { m.sqsMessages.WithLabelValues(outcome).Inc() }
func (m *Metrics) SQSRetry()                   { m.sqsRetries.Inc() }
func (m *Metrics) SQSDeadLetter(reason string) { m.sqsDLQ.WithLabelValues(reason).Inc() }
func (m *Metrics) OutboxPublished(occurredAt time.Time) {
	m.outboxPublished.Inc()
	m.outboxDelay.Observe(time.Since(occurredAt).Seconds())
}
func (m *Metrics) OutboxFailure() { m.outboxFailures.Inc() }
func (m *Metrics) OutboxBacklog(pending int64, lag time.Duration) {
	m.outboxPending.Set(float64(pending))
	m.outboxLag.Set(lag.Seconds())
}
func (m *Metrics) HTTPRequest(route string, code int) {
	m.httpRequests.WithLabelValues(route, strconv.Itoa(code)).Inc()
}

// BreakerState implements resilience.Observer.
func (m *Metrics) BreakerState(name string, state int) {
	m.breakerState.WithLabelValues(name).Set(float64(state))
}

// BreakerRejected implements resilience.Observer.
func (m *Metrics) BreakerRejected(name string) { m.breakerRejections.WithLabelValues(name).Inc() }
