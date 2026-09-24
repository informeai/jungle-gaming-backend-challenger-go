package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/app"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/events"
)

// OutboxRepo stores events in the caller's transaction and lets publishers
// claim them with leases (FOR UPDATE SKIP LOCKED + locked_until).
type OutboxRepo struct{ m *TxManager }

func NewOutboxRepo(m *TxManager) *OutboxRepo { return &OutboxRepo{m: m} }

var _ app.OutboxRepository = (*OutboxRepo)(nil)

func (r *OutboxRepo) Append(ctx context.Context, records []events.Record) error {
	if len(records) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, e := range records {
		batch.Queue(`INSERT INTO outbox_events
			(id, event_type, event_version, aggregate_type, aggregate_id, partition_key, correlation_id, payload, occurred_at, next_attempt_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
			e.EventID, e.EventType, e.Version, e.AggregateType, e.AggregateID, e.PartitionKey, e.CorrelationID, string(e.Payload), e.OccurredAt)
	}
	q := r.m.q(ctx)
	if tx, ok := q.(pgx.Tx); ok {
		return tx.SendBatch(ctx, batch).Close()
	}
	return errors.New("outbox append requires a transaction")
}

// OutboxMessage is a claimed event ready to publish.
type OutboxMessage struct {
	ID            uuid.UUID
	EventType     string
	PartitionKey  uuid.UUID
	CorrelationID string
	Payload       []byte
	Attempts      int
	OccurredAt    time.Time
}

// Claim leases up to limit due events for owner. Events whose lease expired
// (publisher crashed) become claimable again.
func (r *OutboxRepo) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]OutboxMessage, error) {
	rows, err := r.m.q(ctx).Query(ctx, `
		UPDATE outbox_events o SET locked_by = $1, locked_until = now() + $2::bigint * interval '1 millisecond', attempts = o.attempts + 1
		WHERE o.id IN (
			SELECT id FROM outbox_events
			WHERE published_at IS NULL AND next_attempt_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY occurred_at, id
			LIMIT $3
			FOR UPDATE SKIP LOCKED)
		RETURNING o.id, o.event_type, o.partition_key, o.correlation_id, o.payload::text, o.attempts, o.occurred_at`,
		owner, lease.Milliseconds(), limit)
	if err != nil {
		return nil, Classify(err)
	}
	defer rows.Close()
	var out []OutboxMessage
	for rows.Next() {
		var m OutboxMessage
		var payload string
		if err := rows.Scan(&m.ID, &m.EventType, &m.PartitionKey, &m.CorrelationID, &payload, &m.Attempts, &m.OccurredAt); err != nil {
			return nil, Classify(err)
		}
		m.Payload = []byte(payload)
		out = append(out, m)
	}
	return out, Classify(rows.Err())
}

// MarkPublished confirms the publication if owner still holds the lease.
func (r *OutboxRepo) MarkPublished(ctx context.Context, id uuid.UUID, owner string) (bool, error) {
	tag, err := r.m.q(ctx).Exec(ctx, `UPDATE outbox_events
		SET published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
		WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, id, owner)
	if err != nil {
		return false, Classify(err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkFailed releases the lease and schedules the next attempt.
func (r *OutboxRepo) MarkFailed(ctx context.Context, id uuid.UUID, owner string, next time.Time, cause string) error {
	if len(cause) > 500 {
		cause = cause[:500]
	}
	_, err := r.m.q(ctx).Exec(ctx, `UPDATE outbox_events
		SET locked_by = NULL, locked_until = NULL, next_attempt_at = $3, last_error = $4
		WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, id, owner, next, cause)
	return Classify(err)
}

// Lag returns the number of unpublished events and the age of the oldest.
func (r *OutboxRepo) Lag(ctx context.Context) (int64, time.Duration, error) {
	var count int64
	var oldest *time.Time
	err := r.m.q(ctx).QueryRow(ctx, `SELECT COUNT(*), MIN(occurred_at) FROM outbox_events WHERE published_at IS NULL`).Scan(&count, &oldest)
	if err != nil || oldest == nil {
		return count, 0, Classify(err)
	}
	return count, time.Since(*oldest), nil
}

// InboxRepo deduplicates consumed messages per consumer.
type InboxRepo struct{ m *TxManager }

func NewInboxRepo(m *TxManager) *InboxRepo { return &InboxRepo{m: m} }

var _ app.InboxRepository = (*InboxRepo)(nil)

func (r *InboxRepo) Register(ctx context.Context, consumer, messageID, hash string, now time.Time) (bool, string, error) {
	tag, err := r.m.q(ctx).Exec(ctx, `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, consumer, messageID, hash, now)
	if err != nil {
		return false, "", err
	}
	if tag.RowsAffected() == 1 {
		return true, "", nil
	}
	var stored string
	err = r.m.q(ctx).QueryRow(ctx, `SELECT payload_hash FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`,
		consumer, messageID).Scan(&stored)
	return false, stored, err
}

func (r *InboxRepo) Complete(ctx context.Context, consumer, messageID string, txID *uuid.UUID, outcome string, now time.Time) error {
	_, err := r.m.q(ctx).Exec(ctx, `UPDATE inbox_messages SET transaction_id = $3, outcome = $4, processed_at = $5
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID, txID, outcome, now)
	return err
}
