CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS customers (
    customer_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts (
    account_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    account_status TEXT NOT NULL CHECK (account_status IN ('active', 'suspended', 'closed')),
    risk_tier TEXT NOT NULL CHECK (risk_tier IN ('standard', 'elevated', 'restricted')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cards (
    card_id UUID PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES accounts(account_id),
    card_status TEXT NOT NULL CHECK (card_status IN ('active', 'blocked', 'expired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id UUID PRIMARY KEY,
    merchant_category TEXT NOT NULL,
    country_code CHAR(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES accounts(account_id),
    card_id UUID NOT NULL REFERENCES cards(card_id),
    merchant_id UUID NOT NULL REFERENCES merchants(merchant_id),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    status TEXT NOT NULL CHECK (status IN ('authorized', 'declined', 'reversed')),
    transaction_ts TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_transactions_card_ts ON transactions(card_id, transaction_ts);

-- Debezium needs a stable replica identity to include the old row in UPDATE/DELETE events.
ALTER TABLE transactions REPLICA IDENTITY FULL;
