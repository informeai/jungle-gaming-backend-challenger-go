package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/app"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wallet"
)

// WalletRepo persists wallets and ledger entries. Money is stored as BIGINT
// minor units plus a CHAR(3) currency code.
type WalletRepo struct{ m *TxManager }

func NewWalletRepo(m *TxManager) *WalletRepo { return &WalletRepo{m: m} }

var _ app.WalletRepository = (*WalletRepo)(nil)

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

func (r *WalletRepo) Insert(ctx context.Context, w *wallet.Wallet) error {
	_, err := r.m.q(ctx).Exec(ctx,
		`INSERT INTO wallets (`+walletColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), w.Currency().Code(), w.Balance().MinorUnits(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if isUniqueViolation(err, "wallets_player_currency_key") {
		return app.ErrWalletAlreadyExists
	}
	return err
}

func scanWallet(row pgx.Row) (*wallet.Wallet, error) {
	var (
		id, playerID         uuid.UUID
		currency             string
		balance, version     int64
		createdAt, updatedAt time.Time
	)
	if err := row.Scan(&id, &playerID, &currency, &balance, &version, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	b, err := money.FromMinorCode(balance, currency)
	if err != nil {
		return nil, err
	}
	return wallet.Rehydrate(id, playerID, b, version, createdAt, updatedAt)
}

func (r *WalletRepo) Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return scanWallet(r.m.q(ctx).QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id))
}

func (r *WalletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return scanWallet(r.m.q(ctx).QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id))
}

// Update is a compare-and-set on the version, so even without the row lock a
// concurrent writer could never be silently overwritten.
func (r *WalletRepo) Update(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error {
	tag, err := r.m.q(ctx).Exec(ctx,
		`UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4 WHERE id = $1 AND version = $5`,
		w.ID(), w.Balance().MinorUnits(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return app.ErrConcurrentUpdate
	}
	return nil
}

func (r *WalletRepo) InsertLedgerEntry(ctx context.Context, e *wallet.LedgerEntry) error {
	_, err := r.m.q(ctx).Exec(ctx,
		`INSERT INTO wallet_ledger_entries
		   (id, wallet_id, transaction_id, direction, currency, amount_minor, balance_before_minor, balance_after_minor, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Amount().Currency().Code(),
		e.Amount().MinorUnits(), e.BalanceBefore().MinorUnits(), e.BalanceAfter().MinorUnits(), e.CreatedAt())
	return err
}

func (r *WalletRepo) ListLedger(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]app.LedgerRow, error) {
	rows, err := r.m.q(ctx).Query(ctx,
		`SELECT seq, id, wallet_id, transaction_id, direction, currency, amount_minor, balance_before_minor, balance_after_minor, created_at
		   FROM wallet_ledger_entries WHERE wallet_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`,
		walletID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []app.LedgerRow
	for rows.Next() {
		var (
			seq                   int64
			id, wID, txID         uuid.UUID
			dir, currency         string
			amount, before, after int64
			createdAt             time.Time
		)
		if err := rows.Scan(&seq, &id, &wID, &txID, &dir, &currency, &amount, &before, &after, &createdAt); err != nil {
			return nil, err
		}
		c, err := money.ParseCurrency(currency)
		if err != nil {
			return nil, err
		}
		a, _ := money.FromMinor(amount, c)
		b, _ := money.FromMinor(before, c)
		af, _ := money.FromMinor(after, c)
		d, err := wallet.ParseDirection(dir)
		if err != nil {
			return nil, err
		}
		entry, err := wallet.NewLedgerEntry(id, wID, txID, d, a, b, af, createdAt)
		if err != nil {
			return nil, err
		}
		out = append(out, app.LedgerRow{Seq: seq, Entry: entry})
	}
	return out, rows.Err()
}

func (r *WalletRepo) LedgerTotals(ctx context.Context, walletID uuid.UUID, c money.Currency) (app.LedgerTotals, error) {
	var sum, count int64
	err := r.m.q(ctx).QueryRow(ctx,
		`SELECT COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_minor ELSE -amount_minor END), 0)::bigint, COUNT(*)
		   FROM wallet_ledger_entries WHERE wallet_id = $1 AND currency = $2`, walletID, c.Code()).Scan(&sum, &count)
	if err != nil {
		return app.LedgerTotals{}, err
	}
	m, err := money.FromMinor(sum, c)
	if err != nil {
		return app.LedgerTotals{}, err
	}
	return app.LedgerTotals{Balance: m, Entries: count}, nil
}
