// Package postgres implements the application ports with pgx and explicit
// SQL. Transactions are propagated through context.Context by TxManager.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/app"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
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
// repositories through the context.
type TxManager struct {
	pool *pgxpool.Pool
}

func NewTxManager(pool *pgxpool.Pool) *TxManager { return &TxManager{pool: pool} }

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
	tx, err := m.pool.BeginTx(ctx, opts)
	if err != nil {
		return Classify(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		return Classify(err)
	}
	return Classify(tx.Commit(ctx))
}

func (m *TxManager) q(ctx context.Context) dbtx {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return m.pool
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
		case pgErr.Code == "40001", pgErr.Code == "40P01", pgErr.Code == "55P03",
			pgErr.Code == "53300", pgErr.Code == "57014",
			len(pgErr.Code) == 5 && (pgErr.Code[:2] == "08" || pgErr.Code[:2] == "57"):
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
