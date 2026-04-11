CREATE TABLE IF NOT EXISTS transactions_invalid
(
    transaction_id    Int64,
    account_id        Int64,
    account_level     String,
    amount            Decimal(18, 2),
    account_balance   Decimal(18, 2),
    currency          String,
    transaction_type  String,
    merchant_category String,
    country_code      String,
    timestamp         DateTime,
    transaction_date  Date,
    ingested_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(transaction_date)
ORDER BY (account_id, transaction_id)
SETTINGS index_granularity = 8192;