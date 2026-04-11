CREATE TABLE IF NOT EXISTS transactions_valid
(
    transaction_id    Int64,
    account_id        Int64,
    account_level     String,
    level_name        String,
    daily_limit_rub   Decimal(18, 2),
    monthly_limit_rub Decimal(18, 2),
    amount            Decimal(18, 2),
    amount_rub        Float64,
    account_balance   Decimal(18, 2),
    currency          String,
    transaction_type  String,
    merchant_category String,
    category_name     String,
    risk_score        UInt8,
    is_online         UInt8,
    country_code      String,
    country_name      String,
    region            String,
    risk_level        UInt8,
    timestamp         DateTime,
    transaction_date  Date,
    ingested_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(transaction_date)
ORDER BY (account_id, transaction_id)
SETTINGS index_granularity = 8192;