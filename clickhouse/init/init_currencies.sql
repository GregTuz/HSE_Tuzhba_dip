CREATE TABLE IF NOT EXISTS currency_rates (
    dt          Date,
    currency    LowCardinality(String),
    rate_to_rub Decimal(18, 6),
    nominal     UInt16,
    loaded_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY dt
ORDER BY (dt, currency);