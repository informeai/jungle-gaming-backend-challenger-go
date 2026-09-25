// Package config loads and validates the service configuration from the
// environment. Invalid configuration aborts startup.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	InstanceID string
	LogLevel   string
	// FaultInjection enables crash simulation for recovery tests:
	// "consumer-crash-after-commit" or "outbox-crash-after-publish".
	FaultInjection string

	HTTP     HTTP
	Database Database
	AWS      AWS
	SQS      SQS
	Outbox   Outbox
	Pending  Pending
	OIDC     OIDC
}

type HTTP struct {
	// APIEnabled serves the business routes. When false the process only
	// exposes /health/* and /metrics (worker-only components).
	APIEnabled      bool
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration
}

type Database struct {
	URL            string
	MigrationURL   string
	MaxConns       int32
	ConnectTimeout time.Duration
}

type AWS struct {
	Region      string
	EndpointURL string
}

type SQS struct {
	ConsumerEnabled   bool
	ConsumerName      string
	IngressQueue      string
	IngressDLQ        string
	EventsQueue       string
	MaxMessages       int32
	WaitTime          time.Duration
	VisibilityTimeout time.Duration
	MaxReceives       int
	Concurrency       int
	HandlerTimeout    time.Duration
	RetryBaseDelay    time.Duration
	RetryMaxDelay     time.Duration
	AllowedProviders  []string
}

type Outbox struct {
	Enabled bool
	// DatabaseURL is the relay's own connection (role wallet_relay, which can
	// only read outbox_events and update its delivery columns).
	DatabaseURL  string
	PollInterval time.Duration
	BatchSize    int
	Lease        time.Duration
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
}

type Pending struct {
	Enabled      bool
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
	BaseDelay    time.Duration
	MaxDelay     time.Duration
	TTL          time.Duration
}

type OIDC struct {
	Issuer        string
	JWKSURL       string
	Audience      string
	ProviderClaim string
	ProviderRole  string
	InternalRole  string
}

// Load reads the configuration from the environment and validates it.
func Load() (Config, error) {
	e := &env{}
	host, _ := os.Hostname()
	c := Config{
		InstanceID: e.str("INSTANCE_ID", host),
		LogLevel:   e.str("LOG_LEVEL", "info"),

		FaultInjection: e.str("FAULT_INJECTION", ""),
		HTTP: HTTP{
			APIEnabled:      e.bool("API_ENABLED", true),
			Addr:            e.str("HTTP_ADDR", ":8080"),
			ReadTimeout:     e.dur("HTTP_READ_TIMEOUT", 10*time.Second),
			WriteTimeout:    e.dur("HTTP_WRITE_TIMEOUT", 15*time.Second),
			RequestTimeout:  e.dur("HTTP_REQUEST_TIMEOUT", 10*time.Second),
			ShutdownTimeout: e.dur("HTTP_SHUTDOWN_TIMEOUT", 15*time.Second),
		},
		Database: Database{
			URL:            e.str("DATABASE_URL", ""),
			MigrationURL:   e.str("MIGRATIONS_DATABASE_URL", ""),
			MaxConns:       int32(e.int("DATABASE_MAX_CONNS", 20)),
			ConnectTimeout: e.dur("DATABASE_CONNECT_TIMEOUT", 5*time.Second),
		},
		AWS: AWS{
			Region:      e.str("AWS_REGION", "us-east-1"),
			EndpointURL: e.str("AWS_ENDPOINT_URL", ""),
		},
		SQS: SQS{
			ConsumerEnabled:   e.bool("SQS_CONSUMER_ENABLED", true),
			ConsumerName:      e.str("SQS_CONSUMER_NAME", "wager-transactions-consumer"),
			IngressQueue:      e.str("SQS_INGRESS_QUEUE", "wager-transactions.fifo"),
			IngressDLQ:        e.str("SQS_INGRESS_DLQ", "wager-transactions-dlq.fifo"),
			EventsQueue:       e.str("SQS_EVENTS_QUEUE", "wallet-events.fifo"),
			MaxMessages:       int32(e.int("SQS_MAX_MESSAGES", 10)),
			WaitTime:          e.dur("SQS_WAIT_TIME", 10*time.Second),
			VisibilityTimeout: e.dur("SQS_VISIBILITY_TIMEOUT", 30*time.Second),
			MaxReceives:       e.int("SQS_MAX_RECEIVES", 5),
			Concurrency:       e.int("SQS_CONCURRENCY", 4),
			HandlerTimeout:    e.dur("SQS_HANDLER_TIMEOUT", 10*time.Second),
			RetryBaseDelay:    e.dur("SQS_RETRY_BASE_DELAY", 2*time.Second),
			RetryMaxDelay:     e.dur("SQS_RETRY_MAX_DELAY", 60*time.Second),
			AllowedProviders:  e.list("SQS_ALLOWED_PROVIDERS", "provider-a,provider-b"),
		},
		Outbox: Outbox{
			Enabled:      e.bool("OUTBOX_ENABLED", true),
			DatabaseURL:  e.str("OUTBOX_DATABASE_URL", ""),
			PollInterval: e.dur("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
			BatchSize:    e.int("OUTBOX_BATCH_SIZE", 50),
			Lease:        e.dur("OUTBOX_LEASE", 30*time.Second),
			BaseBackoff:  e.dur("OUTBOX_BASE_BACKOFF", time.Second),
			MaxBackoff:   e.dur("OUTBOX_MAX_BACKOFF", 5*time.Minute),
		},
		Pending: Pending{
			Enabled:      e.bool("PENDING_WORKER_ENABLED", true),
			PollInterval: e.dur("PENDING_POLL_INTERVAL", time.Second),
			BatchSize:    e.int("PENDING_BATCH_SIZE", 50),
			MaxAttempts:  e.int("REFERENCE_MAX_ATTEMPTS", 12),
			BaseDelay:    e.dur("REFERENCE_BASE_DELAY", time.Second),
			MaxDelay:     e.dur("REFERENCE_MAX_DELAY", time.Minute),
			TTL:          e.dur("REFERENCE_TTL", 30*time.Minute),
		},
		OIDC: OIDC{
			Issuer:        e.str("OIDC_ISSUER", ""),
			JWKSURL:       e.str("OIDC_JWKS_URL", ""),
			Audience:      e.str("OIDC_AUDIENCE", "wallet-api"),
			ProviderClaim: e.str("OIDC_PROVIDER_CLAIM", "provider_id"),
			ProviderRole:  e.str("OIDC_PROVIDER_ROLE", "wagering-provider"),
			InternalRole:  e.str("OIDC_INTERNAL_ROLE", "wallet-internal"),
		},
	}
	if len(e.errs) > 0 {
		return Config{}, errors.Join(e.errs...)
	}
	return c, c.Validate()
}

// Validate checks required values and ranges.
func (c Config) Validate() error {
	var errs []error
	req := func(name, v string) {
		if strings.TrimSpace(v) == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	// Each component only requires the settings (and credentials) it uses.
	if c.NeedsAppDatabase() {
		req("DATABASE_URL", c.Database.URL)
	}
	if c.HTTP.APIEnabled {
		req("OIDC_ISSUER", c.OIDC.Issuer)
		req("OIDC_AUDIENCE", c.OIDC.Audience)
	}
	if c.SQS.ConsumerEnabled {
		req("SQS_INGRESS_QUEUE", c.SQS.IngressQueue)
		req("SQS_INGRESS_DLQ", c.SQS.IngressDLQ)
	}
	if c.Outbox.Enabled {
		req("SQS_EVENTS_QUEUE", c.SQS.EventsQueue)
		req("OUTBOX_DATABASE_URL", c.Outbox.DatabaseURL)
	}
	if !c.HTTP.APIEnabled && !c.SQS.ConsumerEnabled && !c.Outbox.Enabled && !c.Pending.Enabled {
		errs = append(errs, errors.New("at least one component must be enabled (API, SQS consumer, outbox relay or pending worker)"))
	}
	req("INSTANCE_ID", c.InstanceID)
	if c.SQS.MaxMessages < 1 || c.SQS.MaxMessages > 10 {
		errs = append(errs, errors.New("SQS_MAX_MESSAGES must be between 1 and 10"))
	}
	if c.SQS.WaitTime > 20*time.Second {
		errs = append(errs, errors.New("SQS_WAIT_TIME must be <= 20s"))
	}
	if c.SQS.HandlerTimeout >= c.SQS.VisibilityTimeout {
		errs = append(errs, errors.New("SQS_HANDLER_TIMEOUT must be lower than SQS_VISIBILITY_TIMEOUT"))
	}
	if c.SQS.Concurrency < 1 || c.Outbox.BatchSize < 1 || c.Pending.BatchSize < 1 {
		errs = append(errs, errors.New("concurrency and batch sizes must be positive"))
	}
	if c.Pending.MaxAttempts < 1 || c.Pending.BaseDelay <= 0 || c.Pending.TTL <= 0 {
		errs = append(errs, errors.New("reference retry policy must be positive"))
	}
	if c.Outbox.Lease <= 0 || c.Outbox.PollInterval <= 0 {
		errs = append(errs, errors.New("outbox lease and poll interval must be positive"))
	}
	if c.Database.MaxConns < 2 {
		errs = append(errs, errors.New("DATABASE_MAX_CONNS must be >= 2"))
	}
	return errors.Join(errs...)
}

// NeedsAppDatabase reports whether the process moves money (API, consumer or
// pending worker) and therefore needs the wallet_app connection. A relay-only
// process never receives those credentials.
func (c Config) NeedsAppDatabase() bool {
	return c.HTTP.APIEnabled || c.SQS.ConsumerEnabled || c.Pending.Enabled
}

// NeedsSQS reports whether the process talks to the broker.
func (c Config) NeedsSQS() bool { return c.SQS.ConsumerEnabled || c.Outbox.Enabled }

type env struct{ errs []error }

func (e *env) str(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func (e *env) int(k string, def int) int {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", k, err))
	}
	return n
}

func (e *env) bool(k string, def bool) bool {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", k, err))
	}
	return b
}

func (e *env) dur(k string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", k, err))
	}
	return d
}

func (e *env) list(k, def string) []string {
	var out []string
	for p := range strings.SplitSeq(e.str(k, def), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
