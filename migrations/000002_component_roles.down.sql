-- Reverts 000002: back to the single wallet_app role of 000001.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_relay') THEN
        REVOKE ALL ON outbox_events FROM wallet_relay;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_app') THEN
        GRANT SELECT, INSERT, UPDATE ON outbox_events TO wallet_app;
    END IF;
END;
$$;
