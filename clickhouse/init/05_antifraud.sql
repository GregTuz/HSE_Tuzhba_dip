CREATE TABLE IF NOT EXISTS dm_antifraud_daily
(
    account_id    Int64,
    dt            Date,
    is_suspicious UInt8,
    reason        String
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY (account_id, dt);