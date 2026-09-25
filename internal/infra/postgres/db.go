// Package postgres implements the application ports with pgx and explicit
// SQL. Transactions are propagated through context.Context by TxManager.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/app"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/resilience"
)

// NewPool builds a lazily connecting pool; call Ping to validate it.
func NewPool(cfg config.Database) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	pc.MaxConns = cfg.MaxConns
	pc.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	return pgxpool.NewWithConfig(context.Background(), pc)
}

type dbtx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type txKey struct{}

// TxManager opens READ COMMITTED transactions and exposes them to the
// repositories through the context. Every transaction and every query outside
// a transaction goes through the pool's circuit breaker: while it is open,
// calls fail immediately with a transient error instead of waiting on a dead
// database. A nil breaker disables the protection (tests, tools).
type TxManager struct {
	pool    *pgxpool.Pool
	breaker *resilience.Breaker
	guarded guardedPool
}

func NewTxManager(pool *pgxpool.Pool, breaker *resilience.Breaker) *TxManager {
	return &TxManager{pool: pool, breaker: breaker, guarded: guardedPool{pool: pool, breaker: breaker}}
}

// IsUnavailable is the breaker's failure predicate for PostgreSQL: connection
// errors, timeouts and server unavailability count; serialization failures,
// deadlocks, constraint violations and business errors do not (they prove
// the database is answering).
func IsUnavailable(err error) bool {
	var t *app.TransientError
	return errors.As(Classify(err), &t) && !t.Conflict
}

// NewBreaker builds the circuit breaker of a PostgreSQL pool.
func NewBreaker(name string, cfg config.Breaker, obs resilience.Observer, log *slog.Logger) *resilience.Breaker {
	return resilience.New(name, resilience.Settings{
		FailureThreshold: cfg.FailureThreshold, OpenTimeout: cfg.OpenTimeout, IsFailure: IsUnavailable,
	}, obs, log)
}

// Gate pauses background work while the pool's breaker is open; its
// half-open probe is a ping, never a unit of work.
func (m *TxManager) Gate() *resilience.Gate {
	if m.breaker == nil {
		return nil
	}
	return resilience.NewGate(m.breaker, func(ctx context.Context) error { return Ping(ctx, m.pool) })
}

// Breaker exposes the pool's breaker (readiness, metrics).
func (m *TxManager) Breaker() *resilience.Breaker { return m.breaker }

// guarded converts an open-breaker rejection into a transient error.
func guarded(err error) error {
	if errors.Is(err, resilience.ErrOpen) && !app.IsTransient(err) {
		return &app.TransientError{Err: err}
	}
	return err
}

func (m *TxManager) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return m.within(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, fn)
}

func (m *TxManager) WithinSnapshot(ctx context.Context, fn func(ctx context.Context) error) error {
	return m.within(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

func (m *TxManager) within(ctx context.Context, opts pgx.TxOptions, fn func(ctx context.Context) error) error {
	if _, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return fn(ctx) // join the caller's transaction
	}
	// The whole transaction is one call through the breaker: business
	// outcomes count as successes, unavailability as failures.
	return guarded(m.breaker.Do(func() error {
		tx, err := m.pool.BeginTx(ctx, opts)
		if err != nil {
			return Classify(err)
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
			return Classify(err)
		}
		return Classify(tx.Commit(ctx))
	}))
}

func (m *TxManager) q(ctx context.Context) dbtx {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx // already guarded by within()
	}
	return m.guarded
}

// guardedPool runs single statements outside a transaction through the
// breaker.
type guardedPool struct {
	pool    *pgxpool.Pool
	breaker *resilience.Breaker
}

func (g guardedPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	var tag pgconn.CommandTag
	err := g.breaker.Do(func() error {
		var err error
		tag, err = g.pool.Exec(ctx, sql, args...)
		return Classify(err)
	})
	return tag, guarded(err)
}

func (g guardedPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	var rows pgx.Rows
	err := g.breaker.Do(func() error {
		var err error
		rows, err = g.pool.Query(ctx, sql, args...)
		return Classify(err)
	})
	return rows, guarded(err)
}

func (g guardedPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return guardedRow{g: g, ctx: ctx, sql: sql, args: args}
}

// guardedRow defers the query to Scan, where pgx reports its errors.
type guardedRow struct {
	g    guardedPool
	ctx  context.Context
	sql  string
	args []any
}

func (r guardedRow) Scan(dest ...any) error {
	return guarded(r.g.breaker.Do(func() error {
		return Classify(r.g.pool.QueryRow(r.ctx, r.sql, r.args...).Scan(dest...))
	}))
}

// Classify wraps infrastructure failures that are worth retrying into
// app.TransientError. Domain and application errors pass through.
func Classify(err error) error {
	if err == nil || app.IsTransient(err) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case pgErr.Code == "40001", pgErr.Code == "40P01", pgErr.Code == "55P03":
			// serialization failure, deadlock, lock not available: concurrency
			return &app.TransientError{Err: err, Conflict: true}
		case pgErr.Code == "53300", pgErr.Code == "57014",
			len(pgErr.Code) == 5 && (pgErr.Code[:2] == "08" || pgErr.Code[:2] == "57"):
			// too many connections, canceled/shutdown, connection exceptions
			return &app.TransientError{Err: err}
		}
		return err
	}
	var connErr *pgconn.ConnectError
	var netErr net.Error
	if errors.As(err, &connErr) || errors.As(err, &netErr) || pgconn.Timeout(err) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, pgx.ErrTxClosed) {
		return &app.TransientError{Err: err}
	}
	var sqlState interface{ SQLState() string }
	if errors.As(err, &sqlState) {
		return err
	}
	if pgconn.SafeToRetry(err) {
		return &app.TransientError{Err: err}
	}
	return err
}

func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && (constraint == "" || pgErr.ConstraintName == constraint)
}

// Ping validates connectivity (used at startup and by readiness).
func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	return Classify(pool.Ping(ctx))
}
