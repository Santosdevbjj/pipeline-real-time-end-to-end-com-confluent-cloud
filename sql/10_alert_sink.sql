CREATE TABLE `payments.fraud.alerts` (
  alert_id STRING NOT NULL,
  card_id STRING NOT NULL,
  account_id STRING NOT NULL,
  window_start TIMESTAMP_LTZ(3) NOT NULL,
  window_end TIMESTAMP_LTZ(3) NOT NULL,
  event_count BIGINT NOT NULL,
  total_amount_minor BIGINT,
  risk_tier STRING,
  rule_id STRING NOT NULL,
  PRIMARY KEY (alert_id) NOT ENFORCED
 ) WITH (
  'connector' = 'confluent',
  'changelog.mode' = 'upsert',
  'value.format' = 'avro-registry',
  'value.fields-include' = 'all'
);

-- One-second hop produces rolling 60s windows. The keyed primary key makes updates
-- to a not-yet-final window idempotent in the upsert sink.
INSERT INTO `payments.fraud.alerts`
SELECT
  CONCAT(CAST(w.card_id AS STRING), ':', CAST(w.window_start AS STRING)) AS alert_id,
  w.card_id,
  w.account_id,
  w.window_start,
  w.window_end,
  w.event_count,
  w.total_amount_minor,
  COALESCE(a.risk_tier, 'unknown') AS risk_tier,
  'CARD_VELOCITY_3_IN_60S' AS rule_id
FROM (
  SELECT
    card_id,
    account_id,
    window_start,
    window_end,
    COUNT(*) AS event_count,
    SUM(amount_minor) AS total_amount_minor
  FROM TABLE(
    HOP(TABLE payments_transactions,
        DESCRIPTOR(transaction_ts),
        INTERVAL '1' SECOND,
        INTERVAL '60' SECOND)
  )
  WHERE status = 'authorized'
  GROUP BY card_id, account_id, window_start, window_end
  HAVING COUNT(*) >= 3
) AS w
LEFT JOIN payments_accounts AS a
  ON w.account_id = a.account_id;
