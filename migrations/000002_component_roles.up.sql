-- Least privilege per component (roles are created by deploy/postgres/init.sql).
--
--   wallet_app   API, SQS consumer and pending worker: move money. On the
--                outbox they may only INSERT events in the same transaction
--                as the operation; they can neither read, claim nor mark
--                events as published.
--   wallet_relay Outbox relay: may only read outbox_events and update its
--                delivery columns. No access to wallets, transactions, ledger
--                or inbox, and it cannot alter an event's payload.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_app') THEN
        REVOKE SELECT, UPDATE ON outbox_events FROM wallet_app;
        GRANT INSERT ON outbox_events TO wallet_app;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_relay') THEN
        GRANT USAGE ON SCHEMA public TO wallet_relay;
        GRANT SELECT ON outbox_events TO wallet_relay;
        GRANT UPDATE (attempts, next_attempt_at, locked_by, locked_until, published_at, last_error)
            ON outbox_events TO wallet_relay;
    END IF;
END;
$$;
