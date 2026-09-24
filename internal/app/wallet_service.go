package app

import (
	"context"
	"encoding/base64"
	"log/slog"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wagering"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wallet"
)

// WalletService handles internal wallet operations.
type WalletService struct {
	tx      TxManager
	wallets WalletRepository
	txs     TransactionRepository
	outbox  OutboxRepository
	clock   Clock
	ids     IDGenerator
	metrics Metrics
	log     *slog.Logger
}

type WalletDeps struct {
	Tx      TxManager
	Wallets WalletRepository
	Txs     TransactionRepository
	Outbox  OutboxRepository
	Clock   Clock
	IDs     IDGenerator
	Metrics Metrics
	Log     *slog.Logger
}

func NewWalletService(d WalletDeps) *WalletService {
	return &WalletService{tx: d.Tx, wallets: d.Wallets, txs: d.Txs, outbox: d.Outbox, clock: d.Clock, ids: d.IDs, metrics: d.Metrics, log: d.Log}
}

type OpenWalletCommand struct {
	PlayerID      uuid.UUID
	Initial       money.Money
	CorrelationID string
}

// Open creates the wallet and, for a positive initial balance, the PROCESSED
// OPENING transaction, its CREDIT entry and outbox records, in one commit.
func (s *WalletService) Open(ctx context.Context, cmd OpenWalletCommand) (*wallet.Wallet, error) {
	if cmd.CorrelationID == "" {
		cmd.CorrelationID = s.ids().String()
	}
	var out *wallet.Wallet
	err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		o, err := wagering.OpenWallet(wagering.Env{NewID: s.ids, Now: s.clock()}, s.ids(), cmd.PlayerID, cmd.Initial, cmd.CorrelationID)
		if err != nil {
			return err
		}
		if err := s.wallets.Insert(ctx, o.Wallet); err != nil {
			return err
		}
		if o.Tx != nil {
			if err := s.txs.Insert(ctx, o.Tx); err != nil {
				return err
			}
			if err := s.wallets.InsertLedgerEntry(ctx, o.Entry); err != nil {
				return err
			}
			if err := s.outbox.Append(ctx, o.Events); err != nil {
				return err
			}
		}
		out = o.Wallet
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.log.Info("wallet opened", slog.String("walletId", out.ID().String()),
		slog.String("correlationId", cmd.CorrelationID), slog.Int64("version", out.Version()))
	return out, nil
}

func (s *WalletService) Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	w, err := s.wallets.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if w == nil {
		return nil, ErrWalletNotFound
	}
	return w, nil
}

const (
	DefaultLedgerLimit = 50
	MaxLedgerLimit     = 200
	cursorPrefix       = "l1:"
)

// LedgerPage is a page of ledger entries in stable (insertion) order.
type LedgerPage struct {
	Entries    []*wallet.LedgerEntry
	NextCursor string
}

// EncodeCursor returns an opaque cursor for the entry with the given seq.
func EncodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursorPrefix + strconv.FormatInt(seq, 10)))
}

func DecodeCursor(c string) (int64, error) {
	if c == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil || !strings.HasPrefix(string(raw), cursorPrefix) {
		return 0, ErrInvalidCursor
	}
	seq, err := strconv.ParseInt(strings.TrimPrefix(string(raw), cursorPrefix), 10, 64)
	if err != nil || seq < 0 {
		return 0, ErrInvalidCursor
	}
	return seq, nil
}

func (s *WalletService) Ledger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	after, err := DecodeCursor(cursor)
	if err != nil {
		return LedgerPage{}, err
	}
	if limit <= 0 {
		limit = DefaultLedgerLimit
	}
	if limit > MaxLedgerLimit {
		limit = MaxLedgerLimit
	}
	if _, err := s.Get(ctx, walletID); err != nil {
		return LedgerPage{}, err
	}
	rows, err := s.wallets.ListLedger(ctx, walletID, after, limit+1)
	if err != nil {
		return LedgerPage{}, err
	}
	page := LedgerPage{}
	if len(rows) > limit {
		rows = rows[:limit]
		page.NextCursor = EncodeCursor(rows[len(rows)-1].Seq)
	}
	for _, r := range rows {
		page.Entries = append(page.Entries, r.Entry)
	}
	return page, nil
}

// Reconciliation compares the stored balance with the balance rebuilt from
// the ledger, both read from the same snapshot.
type Reconciliation struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int64
}

// Reconcile never writes: it runs in a read-only REPEATABLE READ transaction.
func (s *WalletService) Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error) {
	var r Reconciliation
	err := s.tx.WithinSnapshot(ctx, func(ctx context.Context) error {
		w, err := s.wallets.Get(ctx, walletID)
		if err != nil {
			return err
		}
		if w == nil {
			return ErrWalletNotFound
		}
		totals, err := s.wallets.LedgerTotals(ctx, walletID, w.Currency())
		if err != nil {
			return err
		}
		diff, err := w.Balance().Sub(totals.Balance)
		if err != nil {
			return err
		}
		r = Reconciliation{
			WalletID: walletID, StoredBalance: w.Balance(), CalculatedBalance: totals.Balance,
			Difference: diff, Consistent: diff.IsZero(), CheckedEntries: totals.Entries,
		}
		return nil
	})
	if err != nil {
		return Reconciliation{}, err
	}
	s.metrics.ReconciliationChecked(r.Consistent)
	level := slog.LevelInfo
	if !r.Consistent {
		level = slog.LevelError
	}
	s.log.Log(ctx, level, "wallet reconciliation", slog.String("walletId", walletID.String()),
		slog.Bool("consistent", r.Consistent), slog.String("difference", r.Difference.Amount()),
		slog.Int64("checkedEntries", r.CheckedEntries))
	return r, nil
}
