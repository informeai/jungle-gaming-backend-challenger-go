package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/app"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wagering"
)

// TransactionRepo persists wager transactions.
type TransactionRepo struct{ m *TxManager }

func NewTransactionRepo(m *TxManager) *TransactionRepo { return &TransactionRepo{m: m} }

var _ app.TransactionRepository = (*TransactionRepo)(nil)

const txColumns = `id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
	provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_transaction_id, reference_transaction_id, failure_code,
	result_balance_minor, result_wallet_version, attempts, next_attempt_at, expires_at,
	correlation_id, causation_id, created_at, updated_at, completed_at`

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func txArgs(t *wagering.Transaction) []any {
	var resultBalance *int64
	if b := t.ResultBalance(); b != nil {
		v := b.MinorUnits()
		resultBalance = &v
	}
	var resultVersion *int64
	if t.ResultWalletVersion() > 0 {
		v := t.ResultWalletVersion()
		resultVersion = &v
	}
	return []any{
		t.ID(), string(t.Origin()), string(t.Kind()), string(t.Status()), t.WalletID(), t.PlayerID(),
		t.Money().Currency().Code(), t.Money().MinorUnits(),
		nullable(t.ProviderID()), nullable(t.ExternalID()), nullable(t.IdempotencyKey()), nullable(t.PayloadHash()),
		nullable(t.RoundID()), nullable(t.GameID()), nullable(t.ReferenceExternalID()), t.ReferenceID(),
		nullable(string(t.FailureCode())), resultBalance, resultVersion, t.Attempts(), t.NextAttemptAt(),
		t.ExpiresAt(), t.CorrelationID(), t.CausationID(), t.CreatedAt(), t.UpdatedAt(), t.CompletedAt(),
	}
}

const insertTx = `INSERT INTO wager_transactions (` + txColumns + `) VALUES
	($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)`

func (r *TransactionRepo) Insert(ctx context.Context, t *wagering.Transaction) error {
	_, err := r.m.q(ctx).Exec(ctx, insertTx, txArgs(t)...)
	return err
}

// InsertIfAbsent relies on the partial unique indexes on (provider_id,
// idempotency_key) and (provider_id, external_transaction_id). A concurrent
// inserter of the same key waits for the first transaction to finish.
func (r *TransactionRepo) InsertIfAbsent(ctx context.Context, t *wagering.Transaction) (bool, error) {
	tag, err := r.m.q(ctx).Exec(ctx, insertTx+` ON CONFLICT DO NOTHING`, txArgs(t)...)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Update persists the mutable state (status, outcome, schedule). The
// wager_guard_update trigger rejects changes to business fields and to
// terminal rows.
func (r *TransactionRepo) Update(ctx context.Context, t *wagering.Transaction) error {
	a := txArgs(t)
	tag, err := r.m.q(ctx).Exec(ctx, `UPDATE wager_transactions SET
		status = $2, reference_transaction_id = $3, failure_code = $4, result_balance_minor = $5,
		result_wallet_version = $6, attempts = $7, next_attempt_at = $8, expires_at = $9,
		updated_at = $10, completed_at = $11
		WHERE id = $1`,
		t.ID(), a[3], a[15], a[16], a[17], a[18], a[19], a[20], a[21], a[25], a[26])
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return app.ErrTransactionNotFound
	}
	return nil
}

func scanTx(row pgx.Row) (*wagering.Transaction, error) {
	var (
		p                                                  wagering.RehydrateParams
		origin, kind, status, currency                     string
		amount                                             int64
		providerID, externalID, key, hash, roundID, gameID *string
		refExternal, failure                               *string
		resultBalance, resultVersion                       *int64
	)
	err := row.Scan(&p.ID, &origin, &kind, &status, &p.WalletID, &p.PlayerID, &currency, &amount,
		&providerID, &externalID, &key, &hash, &roundID, &gameID, &refExternal, &p.ReferenceID, &failure,
		&resultBalance, &resultVersion, &p.Attempts, &p.NextAttemptAt, &p.ExpiresAt,
		&p.CorrelationID, &p.CausationID, &p.CreatedAt, &p.UpdatedAt, &p.CompletedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	deref := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	p.Origin, p.Kind, p.Status = wagering.Origin(origin), wagering.Kind(kind), wagering.Status(status)
	p.ProviderID, p.ExternalID, p.IdempotencyKey, p.PayloadHash = deref(providerID), deref(externalID), deref(key), deref(hash)
	p.RoundID, p.GameID, p.ReferenceExternalID = deref(roundID), deref(gameID), deref(refExternal)
	p.FailureCode = wagering.FailureCode(deref(failure))
	if p.Money, err = money.FromMinorCode(amount, currency); err != nil {
		return nil, err
	}
	if resultBalance != nil {
		b, err := money.FromMinor(*resultBalance, p.Money.Currency())
		if err != nil {
			return nil, err
		}
		p.ResultBalance = &b
	}
	if resultVersion != nil {
		p.ResultWalletVersion = *resultVersion
	}
	return wagering.Rehydrate(p)
}

func (r *TransactionRepo) one(ctx context.Context, where string, args ...any) (*wagering.Transaction, error) {
	return scanTx(r.m.q(ctx).QueryRow(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE `+where, args...))
}

func (r *TransactionRepo) Get(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	return r.one(ctx, `id = $1`, id)
}

func (r *TransactionRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	return r.one(ctx, `id = $1 FOR UPDATE`, id)
}

func (r *TransactionRepo) FindByIdempotencyKey(ctx context.Context, providerID, key string) (*wagering.Transaction, error) {
	return r.one(ctx, `origin = 'EXTERNAL' AND provider_id = $1 AND idempotency_key = $2`, providerID, key)
}

func (r *TransactionRepo) FindByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error) {
	return r.one(ctx, `origin = 'EXTERNAL' AND provider_id = $1 AND external_transaction_id = $2`, providerID, externalID)
}

func (r *TransactionRepo) HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error) {
	var exists bool
	err := r.m.q(ctx).QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wager_transactions
		WHERE reference_transaction_id = $1 AND status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK'))`,
		referenceID).Scan(&exists)
	return exists, err
}

func (r *TransactionRepo) DuePendingReferences(ctx context.Context, now time.Time, limit int) ([]app.PendingRef, error) {
	rows, err := r.m.q(ctx).Query(ctx, `SELECT id, wallet_id FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= $1 ORDER BY next_attempt_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []app.PendingRef
	for rows.Next() {
		var p app.PendingRef
		if err := rows.Scan(&p.TransactionID, &p.WalletID); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *TransactionRepo) WakeDependents(ctx context.Context, providerID, externalID string, now time.Time) error {
	_, err := r.m.q(ctx).Exec(ctx, `UPDATE wager_transactions SET next_attempt_at = $3
		WHERE status = 'PENDING_REFERENCE' AND provider_id = $1 AND reference_external_transaction_id = $2
		  AND next_attempt_at > $3`, providerID, externalID, now)
	return err
}
