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

// readiness tags a provider's result as a member of the readiness group.
func readiness(f any) any {
	return fx.Annotate(f, fx.ResultTags(`group:"readiness"`))
}

// PostgresModule provides the wallet_app pool (validated on start, closed
// last) and the repositories bound to the application ports. Only components
// that move money (API, consumer, pending worker) include it.
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
		readiness(func(pool *pgxpool.Pool) httpapi.ReadinessCheck {
			return httpapi.ReadinessCheck{Name: "postgres", Check: func(ctx context.Context) error { return postgres.Ping(ctx, pool) }}
		}),
	),
)

func newPool(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	return openPool(lc, cfg.Database, "postgres", log)
}

// openPool builds a pool that is pinged on start and closed on stop.
func openPool(lc fx.Lifecycle, db config.Database, name string, log *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := postgres.NewPool(db)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := postgres.Ping(ctx, pool); err != nil {
				return fmt.Errorf("%s unavailable: %w", name, err)
			}
			log.Info("database connected", slog.String("pool", name))
			return nil
		},
		OnStop: func(context.Context) error {
			pool.Close()
			log.Info("database pool closed", slog.String("pool", name))
			return nil
		},
	})
	return pool, nil
}

// SQSModule provides the SQS client and the URLs of the queues this process
// uses (resolved on start; a missing queue aborts the startup).
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
		q, err := sqs.ResolveQueues(ctx, client, cfg.SQS, cfg.SQS.ConsumerEnabled, cfg.Outbox.Enabled)
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

// OutboxLoop and PendingLoop name the worker loops in the Fx graph.
type OutboxLoop struct{ *worker.Loop }
type PendingLoop struct{ *worker.Loop }

// RelayPool is the outbox relay's own pool, authenticated as wallet_relay.
type RelayPool struct{ *pgxpool.Pool }

// ConsumerModule runs the SQS ingress consumer.
var ConsumerModule = fx.Module("sqs-consumer",
	fx.Provide(newConsumer, readiness(func(client *awssqs.Client, queues *sqs.Queues) httpapi.ReadinessCheck {
		return httpapi.ReadinessCheck{Name: "sqs-ingress", Check: func(ctx context.Context) error { return sqs.Ping(ctx, client, queues.Ingress) }}
	})),
	fx.Invoke(func(*sqs.Consumer) {}),
)

func newConsumer(lc fx.Lifecycle, cfg config.Config, client *awssqs.Client, queues *sqs.Queues,
	svc *app.WageringService, m *observability.Metrics, log *slog.Logger) *sqs.Consumer {
	c := sqs.NewConsumer(client, queues, svc, cfg.SQS, m, log)
	if cfg.FaultInjection == "consumer-crash-after-commit" {
		c.AfterCommit = func(id string) {
			log.Error("FAULT INJECTION: crashing after commit, before deleting the message", slog.String("messageId", id))
			os.Exit(137)
		}
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

// OutboxModule runs the outbox relay with its own least-privileged pool: it
// never holds the credentials that move money.
var OutboxModule = fx.Module("outbox-relay",
	fx.Provide(
		func(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (RelayPool, error) {
			db := cfg.Database
			db.URL = cfg.Outbox.DatabaseURL
			db.MaxConns = 4
			pool, err := openPool(lc, db, "outbox-relay", log)
			return RelayPool{pool}, err
		},
		newOutboxRelay,
		readiness(func(pool RelayPool) httpapi.ReadinessCheck {
			return httpapi.ReadinessCheck{Name: "postgres-outbox", Check: func(ctx context.Context) error { return postgres.Ping(ctx, pool.Pool) }}
		}),
		readiness(func(client *awssqs.Client, queues *sqs.Queues) httpapi.ReadinessCheck {
			return httpapi.ReadinessCheck{Name: "sqs-events", Check: func(ctx context.Context) error { return sqs.Ping(ctx, client, queues.Events) }}
		}),
	),
	fx.Invoke(func(*OutboxLoop) {}),
)

func newOutboxRelay(lc fx.Lifecycle, cfg config.Config, client *awssqs.Client, queues *sqs.Queues,
	pool RelayPool, m *observability.Metrics, log *slog.Logger) *OutboxLoop {
	repo := postgres.NewOutboxRepo(postgres.NewTxManager(pool.Pool))
	var relay *worker.OutboxRelay
	loop := worker.NewLoop("outbox-relay", cfg.Outbox.PollInterval, log, func(ctx context.Context) {
		// Drain quickly while there is backlog.
		for relay.Tick(ctx) == cfg.Outbox.BatchSize && ctx.Err() == nil {
		}
	})
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

// PendingModule runs the pending-reference worker.
var PendingModule = fx.Module("pending-reference-worker",
	fx.Provide(newPendingLoop),
	fx.Invoke(func(*PendingLoop) {}),
)

func newPendingLoop(lc fx.Lifecycle, cfg config.Config, svc *app.WageringService, log *slog.Logger) *PendingLoop {
	resolver := worker.NewPendingResolver(svc, cfg.Pending.BatchSize, log)
	loop := worker.NewLoop("pending-reference-worker", cfg.Pending.PollInterval, log, resolver.Tick)
	lc.Append(fx.Hook{OnStart: func(context.Context) error { loop.Start(); return nil }, OnStop: loop.Stop})
	return &PendingLoop{loop}
}

// HTTPModule registers the HTTP server (business routes only when the API
// component is enabled; health and metrics always). It is invoked last so it
// stops first: new requests are refused before workers and pools go away.
var HTTPModule = fx.Module("http",
	fx.Provide(newAPI, registerServer),
	fx.Invoke(func(*Server) {}),
)

type apiParams struct {
	fx.In
	Cfg      config.Config
	Wallets  *app.WalletService       `optional:"true"`
	Wagering *app.WageringService     `optional:"true"`
	Verifier *auth.Verifier           `optional:"true"`
	Checks   []httpapi.ReadinessCheck `group:"readiness"`
	Metrics  *observability.Metrics
	Log      *slog.Logger
}

func newAPI(p apiParams) *httpapi.API {
	return httpapi.NewAPI(p.Wallets, p.Wagering, p.Verifier, p.Metrics, p.Checks, p.Cfg.HTTP.RequestTimeout, p.Log)
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

// Options composes the service with only the enabled components:
//
//	API_ENABLED            HTTP business routes (+ auth, wallet_app pool)
//	SQS_CONSUMER_ENABLED   ingress consumer     (+ wallet_app pool, SQS ingress/DLQ)
//	PENDING_WORKER_ENABLED pending references   (+ wallet_app pool)
//	OUTBOX_ENABLED         outbox relay         (+ wallet_relay pool, SQS events)
//
// A process only builds the connections and credentials its components use;
// with everything enabled it behaves as the single all-in-one service.
func Options(cfg config.Config) fx.Option {
	opts := []fx.Option{
		fx.Supply(cfg),
		fx.WithLogger(func(l *slog.Logger) fxevent.Logger {
			fl := &fxevent.SlogLogger{Logger: l.With(slog.String("component", "fx"))}
			fl.UseLogLevel(slog.LevelDebug) // container events only in debug; errors stay at error level
			return fl
		}),
		fx.StartTimeout(30 * time.Second),
		fx.StopTimeout(cfg.HTTP.ShutdownTimeout + cfg.SQS.HandlerTimeout + 5*time.Second),
		ObservabilityModule,
	}
	if cfg.NeedsAppDatabase() {
		opts = append(opts, PostgresModule, AppModule)
	}
	if cfg.NeedsSQS() {
		opts = append(opts, SQSModule)
	}
	if cfg.HTTP.APIEnabled {
		opts = append(opts, AuthModule)
	}
	// Workers are registered before the HTTP server so that, in reverse order,
	// the server stops first, then the workers, then the pools.
	if cfg.SQS.ConsumerEnabled {
		opts = append(opts, ConsumerModule)
	}
	if cfg.Pending.Enabled {
		opts = append(opts, PendingModule)
	}
	if cfg.Outbox.Enabled {
		opts = append(opts, OutboxModule)
	}
	return fx.Options(append(opts, HTTPModule)...)
}
