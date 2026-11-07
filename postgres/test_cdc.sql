-- Run against the same PostgreSQL database after the connector is RUNNING.
-- These test events are transient and are deleted at the end; capture each CDC record first.
BEGIN;
INSERT INTO transactions (transaction_id, account_id, card_id, merchant_id, amount_minor,
                          currency, status, transaction_ts)
VALUES ('49999999-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001',
        '30000000-0000-4000-8000-000000000001', 650, 'USD', 'authorized', now());
COMMIT;

UPDATE transactions
SET amount_minor = 750, updated_at = now()
WHERE transaction_id = '49999999-0000-4000-8000-000000000001';

DELETE FROM transactions
WHERE transaction_id = '49999999-0000-4000-8000-000000000001';
