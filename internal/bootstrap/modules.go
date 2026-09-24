// Package bootstrap is the composition root: it wires configuration,
// connections, repositories, use cases, handlers and workers with Uber Fx.
// It is the only package (besides cmd) that knows about Fx.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/app"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/auth"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wagering"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/httpapi"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/infra/postgres"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/infra/sqs"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/observability"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/worker"
)

// ObservabilityModule provides the JSON logger and metrics registry.
var ObservabilityModule = fx.Module("observability",
	fx.Provide(observability.NewLogger, observability.NewMetrics),
	fx.Provide(func(m *observability.Metrics) app.Metrics { return m }),
)

// PostgresModule provides the pool (validated on start, closed last) and the
// repositories bound to the application ports.
var PostgresModule = fx.Module("postgres",
	fx.Provide(
		newPool,
		postgres.NewTxManager,
		postgres.NewWalletRepo,
		postgres.NewTransactionRepo,
		postgres.NewOutboxRepo,
		postgres.NewInboxRepo,
		func(m *postgres.TxManager) app.TxManager { return m },
		func(r *postgres.WalletRepo) app.WalletRepository { return r },
		func(r *postgres.TransactionRepo) app.TransactionRepository { return r },
		func(r *postgres.OutboxRepo) app.OutboxRepository { return r },
		func(r *postgres.InboxRepo) app.InboxRepository { return r },
	),
)

func newPool(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := postgres.NewPool(cfg.Database)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := postgres.Ping(ctx, pool); err != nil {
				return fmt.Errorf("postgres unavailable: %w", err)
			}
			log.Info("postgres connected")
			return nil
		},
		OnStop: func(context.Context) error {
			pool.Close()
			log.Info("postgres pool closed")
			return nil
		},
	})
	return pool, nil
}

// SQSModule provides the SQS client and the resolved queue URLs.
var SQSModule = fx.Module("sqs",
	fx.Provide(newSQS),
)

type sqsResult struct {
	fx.Out
	Client *awssqs.Client
	Queues *sqs.Queues
}

func newSQS(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (sqsResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := sqs.NewClient(ctx, cfg.AWS)
	if err != nil {
		return sqsResult{}, err
	}
	queues := &sqs.Queues{}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		q, err := sqs.ResolveQueues(ctx, client, cfg.SQS)
		if err != nil {
			return fmt.Errorf("sqs unavailable: %w", err)
		}
		*queues = q
		log.Info("sqs queues resolved")
		return nil
	}})
	return sqsResult{Client: client, Queues: queues}, nil
}

// AuthModule provides the OIDC token verifier.
var AuthModule = fx.Module("auth",
	fx.Provide(func(cfg config.Config) (*auth.Verifier, error) {
		return auth.NewVerifier(cfg.OIDC, auth.Options{})
	}),
)

// AppModule provides the use cases.
var AppModule = fx.Module("app",
	fx.Provide(
		func() app.Clock { return func() time.Time { return time.Now().UTC() } },
		func() app.IDGenerator { return app.NewUUIDv7 },
		func(cfg config.Config) wagering.RetryPolicy {
			return wagering.RetryPolicy{MaxAttempts: cfg.Pending.MaxAttempts, BaseDelay: cfg.Pending.BaseDelay,
				MaxDelay: cfg.Pending.MaxDelay, TTL: cfg.Pending.TTL}
		},
		newWageringService,
		newWalletService,
	),
)

type serviceParams struct {
	fx.In
	Tx      app.TxManager
	Wallets app.WalletRepository
	Txs     app.TransactionRepository
	Outbox  app.OutboxRepository
	Inbox   app.InboxRepository
	Clock   app.Clock
	IDs     app.IDGenerator
	Policy  wagering.RetryPolicy
	Metrics app.Metrics
	Log     *slog.Logger
}

func newWageringService(p serviceParams) *app.WageringService {
	return app.NewWageringService(app.WageringDeps{Tx: p.Tx, Wallets: p.Wallets, Txs: p.Txs, Outbox: p.Outbox,
		Inbox: p.Inbox, Clock: p.Clock, IDs: p.IDs, Policy: p.Policy, Metrics: p.Metrics, Log: p.Log})
}

func newWalletService(p serviceParams) *app.WalletService {
	return app.NewWalletService(app.WalletDeps{Tx: p.Tx, Wallets: p.Wallets, Txs: p.Txs, Outbox: p.Outbox,
		Clock: p.Clock, IDs: p.IDs, Metrics: p.Metrics, Log: p.Log})
}

// WorkersModule registers the SQS consumer, the outbox relay and the
// pending-reference worker. Each is started after its dependencies and
// stopped before them (Fx runs OnStop hooks in reverse order).
var WorkersModule = fx.Module("workers",
	fx.Provide(newConsumer, newOutboxRelay, newPendingLoop),
	fx.Invoke(func(*OutboxLoop, *PendingLoop, *sqs.Consumer) {}),
)

// OutboxLoop and PendingLoop name the worker loops in the Fx graph.
type OutboxLoop struct{ *worker.Loop }
type PendingLoop struct{ *worker.Loop }

func newConsumer(lc fx.Lifecycle, cfg config.Config, client *awssqs.Client, queues *sqs.Queues,
	svc *app.WageringService, m *observability.Metrics, log *slog.Logger) *sqs.Consumer {
	c := sqs.NewConsumer(client, queues, svc, cfg.SQS, m, log)
	if cfg.FaultInjection == "consumer-crash-after-commit" {
		c.AfterCommit = func(id string) {
			log.Error("FAULT INJECTION: crashing after commit, before deleting the message", slog.String("messageId", id))
			os.Exit(137)
		}
	}
	if !cfg.SQS.ConsumerEnabled {
		return c
	}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			// Queue URLs were resolved by the SQS module's earlier OnStart hook.
			c.Start()
			return nil
		},
		OnStop: c.Stop,
	})
	return c
}

func newOutboxRelay(lc fx.Lifecycle, cfg config.Config, client *awssqs.Client, queues *sqs.Queues,
	repo *postgres.OutboxRepo, m *observability.Metrics, log *slog.Logger) *OutboxLoop {
	var relay *worker.OutboxRelay
	loop := worker.NewLoop("outbox-relay", cfg.Outbox.PollInterval, log, func(ctx context.Context) {
		// Drain quickly while there is backlog.
		for relay.Tick(ctx) == cfg.Outbox.BatchSize && ctx.Err() == nil {
		}
	})
	if !cfg.Outbox.Enabled {
		return &OutboxLoop{loop}
	}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			relay = worker.NewOutboxRelay(repo, sqs.NewEventPublisher(client, *queues), cfg.InstanceID+"/"+uuid.NewString(), cfg.Outbox, m, log)
			if cfg.FaultInjection == "outbox-crash-after-publish" {
				relay.AfterPublish = func(id uuid.UUID) {
					log.Error("FAULT INJECTION: crashing after publish, before confirming the outbox row", slog.String("eventId", id.String()))
					os.Exit(137)
				}
			}
			loop.Start()
			return nil
		},
		OnStop: loop.Stop,
	})
	return &OutboxLoop{loop}
}

func newPendingLoop(lc fx.Lifecycle, cfg config.Config, svc *app.WageringService, log *slog.Logger) *PendingLoop {
	resolver := worker.NewPendingResolver(svc, cfg.Pending.BatchSize, log)
	loop := worker.NewLoop("pending-reference-worker", cfg.Pending.PollInterval, log, resolver.Tick)
	if cfg.Pending.Enabled {
		lc.Append(fx.Hook{OnStart: func(context.Context) error { loop.Start(); return nil }, OnStop: loop.Stop})
	}
	return &PendingLoop{loop}
}

// HTTPModule registers the HTTP server. It is invoked last so it stops first:
// new requests are refused before the workers and connections go away.
var HTTPModule = fx.Module("http",
	fx.Provide(newAPI, registerServer),
	fx.Invoke(func(*Server) {}),
)

func newAPI(cfg config.Config, pool *pgxpool.Pool, client *awssqs.Client, queues *sqs.Queues,
	wallets *app.WalletService, wagering *app.WageringService, verifier *auth.Verifier,
	m *observability.Metrics, log *slog.Logger) *httpapi.API {
	checks := []httpapi.ReadinessCheck{
		{Name: "postgres", Check: func(ctx context.Context) error { return postgres.Ping(ctx, pool) }},
		{Name: "sqs", Check: func(ctx context.Context) error { return sqs.Ping(ctx, client, queues.Ingress) }},
	}
	return httpapi.NewAPI(wallets, wagering, verifier, m, checks, cfg.HTTP.RequestTimeout, log)
}

// Server exposes the bound address (useful when HTTP_ADDR uses port 0).
type Server struct {
	Addr string
}

func registerServer(lc fx.Lifecycle, cfg config.Config, api *httpapi.API, log *slog.Logger) *Server {
	info := &Server{}
	srv := &http.Server{
		Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: cfg.HTTP.ReadTimeout, WriteTimeout: cfg.HTTP.WriteTimeout,
	}
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ln, err := net.Listen("tcp", cfg.HTTP.Addr)
			if err != nil {
				return err
			}
			info.Addr = ln.Addr().String()
			go func() {
				defer close(done)
				if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("http server failed", slog.String("error", err.Error()))
				}
			}()
			log.Info("http server listening", slog.String("addr", info.Addr))
			return nil
		},
		OnStop: func(ctx context.Context) error {
			api.Drain()
			err := srv.Shutdown(ctx) // stops accepting, waits for in-flight requests
			<-done
			log.Info("http server stopped")
			return err
		},
	})
	return info
}

// Options composes the whole service.
func Options(cfg config.Config) fx.Option {
	return fx.Options(
		fx.Supply(cfg),
		fx.WithLogger(func(l *slog.Logger) fxevent.Logger {
			fl := &fxevent.SlogLogger{Logger: l.With(slog.String("component", "fx"))}
			fl.UseLogLevel(slog.LevelDebug) // container events only in debug; errors stay at error level
			return fl
		}),
		fx.StartTimeout(30*time.Second),
		fx.StopTimeout(cfg.HTTP.ShutdownTimeout+cfg.SQS.HandlerTimeout+5*time.Second),
		ObservabilityModule,
		PostgresModule,
		SQSModule,
		AuthModule,
		AppModule,
		WorkersModule,
		HTTPModule,
	)
}
