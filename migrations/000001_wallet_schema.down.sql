-- Reverts 000001. Dropping the tables bypasses the row/statement triggers
-- (DROP is not DELETE/TRUNCATE), which is intended for a schema rollback.
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS inbox_messages;
DROP TABLE IF EXISTS wallet_ledger_entries;
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;

DROP FUNCTION IF EXISTS inbox_guard_update();
DROP FUNCTION IF EXISTS outbox_guard_update();
DROP FUNCTION IF EXISTS wager_no_committed_pending();
DROP FUNCTION IF EXISTS wager_guard_update();
DROP FUNCTION IF EXISTS wallets_require_ledger();
DROP FUNCTION IF EXISTS wallets_guard_update();
DROP FUNCTION IF EXISTS forbid_mutation();
