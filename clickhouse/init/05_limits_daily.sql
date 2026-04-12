CREATE TABLE IF NOT EXISTS dm_daily_limits
(
    account_id          Int64,
    dt                  Date,
    account_level       String,
    daily_turnover_rub  Float64,
    daily_limit_rub     Decimal(18, 2),
    is_exceeded         UInt8
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY (account_id, dt);