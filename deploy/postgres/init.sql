-- Runs once on an empty data directory (docker-entrypoint-initdb.d).
-- wallet_app is the least-privileged runtime role: no DELETE/TRUNCATE, no
-- UPDATE on the ledger, no DDL. Migrations run as the owner (postgres).
CREATE ROLE wallet_app LOGIN PASSWORD 'wallet_app_local';
GRANT CONNECT ON DATABASE wallet TO wallet_app;
GRANT USAGE ON SCHEMA public TO wallet_app;
