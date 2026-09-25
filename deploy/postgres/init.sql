-- Runs once on an empty data directory (docker-entrypoint-initdb.d).
-- Runtime roles (least privilege; grants are applied by the migrations):
--   wallet_app   API, SQS consumer and pending worker (no DELETE/TRUNCATE/DDL,
--                ledger is SELECT/INSERT only, outbox is INSERT only)
--   wallet_relay outbox relay (reads outbox_events, updates delivery columns)
-- Migrations run as the owner (postgres).
CREATE ROLE wallet_app LOGIN PASSWORD 'wallet_app_local';
CREATE ROLE wallet_relay LOGIN PASSWORD 'wallet_relay_local';
GRANT CONNECT ON DATABASE wallet TO wallet_app, wallet_relay;
GRANT USAGE ON SCHEMA public TO wallet_app, wallet_relay;
