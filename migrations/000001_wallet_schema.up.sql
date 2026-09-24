-- Wallets: aggregate root. Balance is stored in minor units (BIGINT).
CREATE TABLE wallets (
    id            uuid        PRIMARY KEY,
    player_id     uuid        NOT NULL,
    currency      char(3)     NOT NULL CONSTRAINT wallets_currency_iso CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor bigint      NOT NULL CONSTRAINT wallets_balance_non_negative CHECK (balance_minor >= 0),
    version       bigint      NOT NULL CONSTRAINT wallets_version_positive CHECK (version >= 1),
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency)
);

-- Wager transactions: internal OPENING and external provider operations.
CREATE TABLE wager_transactions (
    id                                uuid        PRIMARY KEY,
    origin                            text        NOT NULL CONSTRAINT wager_origin_valid CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    kind                              text        NOT NULL CONSTRAINT wager_kind_valid CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    status                            text        NOT NULL CONSTRAINT wager_status_valid CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    wallet_id                         uuid        NOT NULL REFERENCES wallets (id),
    player_id                         uuid        NOT NULL,
    currency                          char(3)     NOT NULL CONSTRAINT wager_currency_iso CHECK (currency ~ '^[A-Z]{3}$'),
    amount_minor                      bigint      NOT NULL,
    provider_id                       text,
    external_transaction_id           text,
    idempotency_key                   text,
    payload_hash                      text,
    round_id                          text,
    game_id                           text,
    reference_external_transaction_id text,
    reference_transaction_id          uuid        REFERENCES wager_transactions (id),
    failure_code                      text,
    result_balance_minor              bigint      CONSTRAINT wager_result_balance_non_negative CHECK (result_balance_minor >= 0),
    result_wallet_version             bigint,
    attempts                          integer     NOT NULL DEFAULT 0 CONSTRAINT wager_attempts_non_negative CHECK (attempts >= 0),
    next_attempt_at                   timestamptz,
    expires_at                        timestamptz,
    correlation_id                    text        NOT NULL,
    causation_id                      text,
    created_at                        timestamptz NOT NULL,
    updated_at                        timestamptz NOT NULL,
    completed_at                      timestamptz,
    -- Internal and external operations have different shapes.
    CONSTRAINT wager_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    -- Zero policy: only LOSS carries 0.00; everything else is strictly positive.
    CONSTRAINT wager_amount_policy CHECK ((kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)),
    CONSTRAINT wager_reversal_requires_reference CHECK (kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
    CONSTRAINT wager_failure_has_code CHECK (status NOT IN ('REJECTED', 'FAILED') OR failure_code IS NOT NULL),
    CONSTRAINT wager_processed_has_result CHECK (status <> 'PROCESSED' OR (result_balance_minor IS NOT NULL AND completed_at IS NOT NULL)),
    CONSTRAINT wager_pending_reference_scheduled CHECK (status <> 'PENDING_REFERENCE' OR (next_attempt_at IS NOT NULL AND expires_at IS NOT NULL))
);

-- Persistent idempotency: one operation per (provider, external id) and per (provider, key).
CREATE UNIQUE INDEX wager_provider_external_id_key ON wager_transactions (provider_id, external_transaction_id) WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX wager_provider_idempotency_key ON wager_transactions (provider_id, idempotency_key) WHERE origin = 'EXTERNAL';
-- At most one initial credit per wallet.
CREATE UNIQUE INDEX wager_single_opening_per_wallet ON wager_transactions (wallet_id) WHERE kind = 'OPENING';
-- A referenced transaction receives at most one successful reversal (REFUND or ROLLBACK).
CREATE UNIQUE INDEX wager_single_reversal_per_reference ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');
CREATE INDEX wager_pending_reference_due ON wager_transactions (next_attempt_at) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX wager_waiting_for_reference ON wager_transactions (provider_id, reference_external_transaction_id) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX wager_wallet_idx ON wager_transactions (wallet_id);

-- Append-only ledger.
CREATE TABLE wallet_ledger_entries (
    seq                  bigint      GENERATED ALWAYS AS IDENTITY CONSTRAINT ledger_seq_key UNIQUE,
    id                   uuid        PRIMARY KEY,
    wallet_id            uuid        NOT NULL REFERENCES wallets (id),
    transaction_id       uuid        NOT NULL REFERENCES wager_transactions (id),
    direction            text        NOT NULL CONSTRAINT ledger_direction_valid CHECK (direction IN ('DEBIT', 'CREDIT')),
    currency             char(3)     NOT NULL,
    amount_minor         bigint      NOT NULL CONSTRAINT ledger_amount_positive CHECK (amount_minor > 0),
    balance_before_minor bigint      NOT NULL CONSTRAINT ledger_before_non_negative CHECK (balance_before_minor >= 0),
    balance_after_minor  bigint      NOT NULL CONSTRAINT ledger_after_non_negative CHECK (balance_after_minor >= 0),
    created_at           timestamptz NOT NULL,
    CONSTRAINT ledger_wallet_transaction_key UNIQUE (wallet_id, transaction_id),
    CONSTRAINT ledger_balance_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
    )
);
CREATE INDEX ledger_wallet_seq_idx ON wallet_ledger_entries (wallet_id, seq);

-- Inbox: durable deduplication of consumed messages.
CREATE TABLE inbox_messages (
    consumer_name  text        NOT NULL,
    message_id     text        NOT NULL,
    payload_hash   text        NOT NULL,
    transaction_id uuid        REFERENCES wager_transactions (id),
    outcome        text,
    received_at    timestamptz NOT NULL,
    processed_at   timestamptz,
    PRIMARY KEY (consumer_name, message_id)
);

-- Transactional outbox.
CREATE TABLE outbox_events (
    id              uuid        PRIMARY KEY,
    event_type      text        NOT NULL,
    event_version   integer     NOT NULL,
    aggregate_type  text        NOT NULL,
    aggregate_id    uuid        NOT NULL,
    partition_key   uuid        NOT NULL,
    correlation_id  text        NOT NULL,
    payload         jsonb       NOT NULL,
    occurred_at     timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_by       text,
    locked_until    timestamptz,
    published_at    timestamptz,
    last_error      text
);
CREATE INDEX outbox_unpublished_idx ON outbox_events (next_attempt_at) WHERE published_at IS NULL;

---------------------------------------------------------------------------
-- Protection triggers
---------------------------------------------------------------------------

CREATE FUNCTION forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% on % is forbidden (append-only)', TG_OP, TG_TABLE_NAME
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

-- Ledger: no UPDATE, DELETE or TRUNCATE.
CREATE TRIGGER ledger_no_update_delete BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER ledger_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();

-- Wallets, transactions, inbox and outbox are never deleted.
CREATE TRIGGER wallets_no_delete BEFORE DELETE ON wallets FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER wallets_no_truncate BEFORE TRUNCATE ON wallets FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER wager_no_delete BEFORE DELETE ON wager_transactions FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER wager_no_truncate BEFORE TRUNCATE ON wager_transactions FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();

-- Wallet updates: identity is immutable; the version moves by exactly one
-- when (and only when) the balance changes.
CREATE FUNCTION wallets_guard_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id OR NEW.currency <> OLD.currency OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wallet identity is immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance_minor <> OLD.balance_minor AND NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'wallet version must increase by one on balance change' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance_minor = OLD.balance_minor AND NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'wallet version can only change with the balance' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER wallets_guard_update BEFORE UPDATE ON wallets FOR EACH ROW EXECUTE FUNCTION wallets_guard_update();

-- Every balance change must be matched, in the same database transaction, by
-- a ledger entry from the old to the new balance. Checked at commit time.
CREATE FUNCTION wallets_require_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    before_minor bigint;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.balance_minor = 0 THEN
            RETURN NULL;
        END IF;
        before_minor := 0;
    ELSE
        IF NEW.balance_minor = OLD.balance_minor THEN
            RETURN NULL;
        END IF;
        before_minor := OLD.balance_minor;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM wallet_ledger_entries l
        WHERE l.wallet_id = NEW.id
          AND l.balance_before_minor = before_minor
          AND l.balance_after_minor = NEW.balance_minor
          AND l.xmin = pg_current_xact_id()::xid
    ) THEN
        RAISE EXCEPTION 'wallet % balance change % -> % has no ledger entry', NEW.id, before_minor, NEW.balance_minor
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER wallets_require_ledger AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION wallets_require_ledger();

-- Wager transactions: business fields are immutable and terminal states are final.
CREATE FUNCTION wager_guard_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'transaction % is terminal (%)', OLD.id, OLD.status USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.status = 'PENDING' AND OLD.status <> 'PENDING' THEN
        RAISE EXCEPTION 'transaction % cannot return to PENDING', OLD.id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF ROW(NEW.id, NEW.origin, NEW.kind, NEW.wallet_id, NEW.player_id, NEW.currency, NEW.amount_minor,
           NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key, NEW.payload_hash,
           NEW.round_id, NEW.game_id, NEW.reference_external_transaction_id, NEW.correlation_id, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.id, OLD.origin, OLD.kind, OLD.wallet_id, OLD.player_id, OLD.currency, OLD.amount_minor,
           OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key, OLD.payload_hash,
           OLD.round_id, OLD.game_id, OLD.reference_external_transaction_id, OLD.correlation_id, OLD.created_at) THEN
        RAISE EXCEPTION 'business fields of transaction % are immutable', OLD.id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER wager_guard_update BEFORE UPDATE ON wager_transactions FOR EACH ROW EXECUTE FUNCTION wager_guard_update();

-- PENDING is a transient in-transaction state: it is never committed. The only
-- durable waiting state is PENDING_REFERENCE, which the worker resumes.
CREATE FUNCTION wager_no_committed_pending() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM wager_transactions WHERE id = NEW.id AND status = 'PENDING') THEN
        RAISE EXCEPTION 'transaction % cannot be committed as PENDING', NEW.id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER wager_no_committed_pending AFTER INSERT ON wager_transactions
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION wager_no_committed_pending();

-- Outbox: the event snapshot is immutable and a publication is final.
CREATE FUNCTION outbox_guard_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.id, NEW.event_type, NEW.event_version, NEW.aggregate_type, NEW.aggregate_id, NEW.partition_key,
           NEW.correlation_id, NEW.payload, NEW.occurred_at, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.id, OLD.event_type, OLD.event_version, OLD.aggregate_type, OLD.aggregate_id, OLD.partition_key,
           OLD.correlation_id, OLD.payload, OLD.occurred_at, OLD.created_at) THEN
        RAISE EXCEPTION 'outbox event % is immutable', OLD.id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'outbox event % was already published', OLD.id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER outbox_guard_update BEFORE UPDATE ON outbox_events FOR EACH ROW EXECUTE FUNCTION outbox_guard_update();
CREATE TRIGGER outbox_no_delete BEFORE DELETE ON outbox_events FOR EACH ROW EXECUTE FUNCTION forbid_mutation();

-- Inbox: identity and hash are immutable.
CREATE FUNCTION inbox_guard_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.consumer_name <> OLD.consumer_name OR NEW.message_id <> OLD.message_id OR NEW.payload_hash <> OLD.payload_hash THEN
        RAISE EXCEPTION 'inbox identity is immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER inbox_guard_update BEFORE UPDATE ON inbox_messages FOR EACH ROW EXECUTE FUNCTION inbox_guard_update();
CREATE TRIGGER inbox_no_delete BEFORE DELETE ON inbox_messages FOR EACH ROW EXECUTE FUNCTION forbid_mutation();

---------------------------------------------------------------------------
-- Least privilege for the application role (created by deploy/postgres/init).
---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_app') THEN
        GRANT SELECT, INSERT, UPDATE ON wallets, wager_transactions, inbox_messages, outbox_events TO wallet_app;
        GRANT SELECT, INSERT ON wallet_ledger_entries TO wallet_app;
        GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO wallet_app;
    END IF;
END;
$$;
