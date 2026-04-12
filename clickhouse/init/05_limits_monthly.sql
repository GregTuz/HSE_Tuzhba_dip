CREATE TABLE IF NOT EXISTS dm_monthly_limits
(
    account_id           Int64,
    month                Date,
    account_level        String,
    monthly_turnover_rub Float64,
    monthly_limit_rub    Decimal(18, 2),
    is_exceeded          UInt8
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(month)
ORDER BY (account_id, month);