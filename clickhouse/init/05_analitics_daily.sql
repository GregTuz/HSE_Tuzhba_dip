CREATE TABLE IF NOT EXISTS dm_analytics_daily
(
    account_level     String,
    merchant_category String,
    category_name     String,
    country_code      String,
    country_name      String,
    region            String,
    transaction_date  Date,
    hour              UInt8,
    day_of_week       UInt8,
    transaction_count UInt32,
    total_amount_rub  Float64,
    avg_amount_rub    Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(transaction_date)
ORDER BY (transaction_date, account_level, merchant_category, country_code, hour);