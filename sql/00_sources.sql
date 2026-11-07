-- Confluent Cloud Flink automatically exposes existing Kafka topics as inferred tables.
-- Select the payments-lab catalog and payments-basic database in the Flink workspace,
-- run SHOW TABLES, and confirm the exact table names from the connector's topics.
-- TopicNameStrategy uses <topic>-key / <topic>-value schema subjects.

-- Debezium Avro envelopes are automatically inferred as a CDC changelog when the
-- registry schema contains before/after/op. Force retract mode for explicit semantics.
ALTER TABLE `payments.public.transactions` SET (
  'value.format' = 'avro-debezium-registry',
  'changelog.mode' = 'retract'
);

ALTER TABLE `payments.public.transactions`
  MODIFY WATERMARK FOR transaction_ts AS transaction_ts - INTERVAL '5' SECOND;

ALTER TABLE `payments.public.accounts` SET (
  'value.format' = 'avro-debezium-registry',
  'changelog.mode' = 'retract'
);

-- If your environment renders dot-separated topic names differently in SHOW TABLES,
-- replace the escaped table identifiers above and below with the exact catalog objects.
CREATE TEMPORARY VIEW payments_transactions AS
SELECT transaction_id, account_id, card_id, merchant_id, amount_minor,
       currency, status, transaction_ts
FROM `payments-lab`.`payments-basic`.`payments.public.transactions`;

CREATE TEMPORARY VIEW payments_accounts AS
SELECT account_id, account_status, risk_tier
FROM `payments-lab`.`payments-basic`.`payments.public.accounts`;
