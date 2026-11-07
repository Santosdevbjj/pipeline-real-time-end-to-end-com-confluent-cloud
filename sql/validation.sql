-- Baseline profile: counts and value distribution in the seeded source database.
SELECT status, currency, COUNT(*) AS payments,
       MIN(amount_minor) AS min_minor, AVG(amount_minor) AS avg_minor,
       MAX(amount_minor) AS max_minor
FROM transactions
GROUP BY status, currency
ORDER BY payments DESC;

-- Validate candidate velocity events before enabling the streaming alert.
SELECT card_id, COUNT(*) AS payment_count,
       MIN(transaction_ts) AS first_seen, MAX(transaction_ts) AS last_seen
FROM transactions
WHERE status = 'authorized'
GROUP BY card_id, date_trunc('minute', transaction_ts)
HAVING COUNT(*) >= 3;
