CREATE TABLE IF NOT EXISTS transactions_invalid
(
    transaction_id Int64,
    account_id Int64,
    account_level String,
    amount Decimal(18, 2),
    account_balance Decimal(18, 2),
    currency String,
    transaction_type String,
    merchant_category String,
    country_code String,
    timestamp Nullable(DateTime),
    transaction_date Nullable(Date),
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(coalesce(transaction_date, toDate('1970-01-01')))
ORDER BY (account_id, transaction_id)
SETTINGS index_granularity = 8192;
