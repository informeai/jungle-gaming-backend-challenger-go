//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/infra/postgres"
)

func TestMigrationsUpDownUp(t *testing.T) {
	ctx := context.Background()
	name := "it_mig_" + runID
	if err := createDatabase(ctx, name); err != nil {
		t.Fatal(err)
	}
	defer dropDatabase(name)
	u := withDB(pgAdminBase, name)
	tables := func() int {
		conn, err := pgx.Connect(ctx, u)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		var n int
		_ = conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'
			AND table_name IN ('wallets','wager_transactions','wallet_ledger_entries','inbox_messages','outbox_events')`).Scan(&n)
		return n
	}
	for i, step := range []struct {
		run  func() error
		want int
	}{
		{func() error { return postgres.MigrateUp(u) }, 5},
		{func() error { return postgres.MigrateDown(u, 1) }, 5}, // reverts only 000002 (grants)
		{func() error { return postgres.MigrateUp(u) }, 5},
		{func() error { return postgres.MigrateDown(u, 0) }, 0},
		{func() error { return postgres.MigrateUp(u) }, 5},
	} {
		if err := step.run(); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if got := tables(); got != step.want {
			t.Fatalf("step %d: %d tables, want %d", i, got, step.want)
		}
	}
	if v, err := postgres.MigrationVersion(u); err != nil || !strings.HasPrefix(v, "2 ") {
		t.Fatalf("version %q %v", v, err)
	}
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// TestSchemaInvariants proves the invariants are enforced by the database
// itself, even for SQL issued outside the application.
func TestSchemaInvariants(t *testing.T) {
	inst := startApp(t, httpConfig(t))
	w := openWallet(t, inst.Base, "100.00")
	bet := w.op(uniq("bet"), "BET", "10.00", "")
	if r := submit(t, inst.Base, bet); r.Status != 200 {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	ctx := context.Background()
	exec := func(sql string, args ...any) error {
		_, err := adminPool.Exec(ctx, sql, args...)
		return err
	}
	appConn, err := pgx.Connect(ctx, appURL)
	if err != nil {
		t.Fatal(err)
	}
	defer appConn.Close(ctx)

	cases := []struct {
		name string
		err  error
		code string
	}{
		{"ledger update", exec(`UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE wallet_id = $1`, w.ID), "23000"},
		{"ledger delete", exec(`DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, w.ID), "23000"},
		{"ledger truncate", exec(`TRUNCATE wallet_ledger_entries CASCADE`), "23000"},
		{"negative balance", exec(`UPDATE wallets SET balance_minor = -1, version = version + 1 WHERE id = $1`, w.ID), "23514"},
		{"balance change without ledger", exec(`UPDATE wallets SET balance_minor = balance_minor + 1, version = version + 1 WHERE id = $1`, w.ID), "23000"},
		{"version not bumped", exec(`UPDATE wallets SET balance_minor = balance_minor + 1 WHERE id = $1`, w.ID), "23000"},
		{"duplicate player+currency", exec(`INSERT INTO wallets VALUES ($1, (SELECT player_id FROM wallets WHERE id = $2), 'BRL', 0, 1, now(), now())`, uuid.New(), w.ID), "23505"},
		{"terminal transaction update", exec(`UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'X' WHERE external_transaction_id = $1`, bet.External), "23000"},
		{"transaction delete", exec(`DELETE FROM wager_transactions WHERE wallet_id = $1`, w.ID), "23000"},
		{"duplicate opening", exec(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, currency, amount_minor, result_balance_minor, correlation_id, created_at, updated_at, completed_at)
			SELECT $1, 'INTERNAL', 'OPENING', 'PROCESSED', id, player_id, 'BRL', 100, 100, 'c', now(), now(), now() FROM wallets WHERE id = $2`, uuid.New(), w.ID), "23505"},
		{"external OPENING", exec(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, currency, amount_minor, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, correlation_id, created_at, updated_at)
			SELECT $1, 'EXTERNAL', 'OPENING', 'REJECTED', id, player_id, 'BRL', 1, 'p', 'e', 'k', 'h', 'r', 'g', 'c', now(), now() FROM wallets WHERE id = $2`, uuid.New(), w.ID), "23514"},
		{"LOSS with amount", exec(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, currency, amount_minor, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, failure_code, correlation_id, created_at, updated_at)
			SELECT $1, 'EXTERNAL', 'LOSS', 'REJECTED', id, player_id, 'BRL', 1, 'p', 'e2', 'k2', 'h', 'r', 'g', 'X', 'c', now(), now() FROM wallets WHERE id = $2`, uuid.New(), w.ID), "23514"},
		{"committed PENDING", exec(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, currency, amount_minor, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, correlation_id, created_at, updated_at)
			SELECT $1, 'EXTERNAL', 'BET', 'PENDING', id, player_id, 'BRL', 1, 'p', 'e3', 'k3', 'h', 'r', 'g', 'c', now(), now() FROM wallets WHERE id = $2`, uuid.New(), w.ID), "23000"},
		{"duplicate ledger for transaction", exec(`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, currency, amount_minor, balance_before_minor, balance_after_minor, created_at)
			SELECT $1, wallet_id, transaction_id, direction, currency, amount_minor, balance_before_minor, balance_after_minor, now() FROM wallet_ledger_entries WHERE wallet_id = $2 LIMIT 1`, uuid.New(), w.ID), "23505"},
		{"app role cannot delete", func() error { _, err := appConn.Exec(ctx, `DELETE FROM outbox_events`); return err }(), "42501"},
		{"app role cannot update ledger", func() error {
			_, err := appConn.Exec(ctx, `UPDATE wallet_ledger_entries SET created_at = now()`)
			return err
		}(), "42501"},
	}
	for _, c := range cases {
		if got := pgCode(c.err); got != c.code {
			t.Errorf("%s: code %q (%v), want %s", c.name, got, c.err, c.code)
		}
	}
	s := stateOf(t, w.ID)
	if s.Balance != 9000 || s.LedgerEntries != 2 || s.Version != 2 {
		t.Fatalf("state changed by rejected statements: %+v", s)
	}
}
