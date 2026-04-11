CREATE MATERIALIZED VIEW IF NOT EXISTS mv_transactions_invalid
TO transactions_invalid
AS
SELECT
    transaction_id,
    account_id,
    account_level,
    amount,
    account_balance,
    currency,
    transaction_type,
    merchant_category,
    country_code,
    parseDateTimeBestEffortOrNull(timestamp) AS timestamp,
    toDateOrNull(transaction_date) AS transaction_date,
    now() AS ingested_at

FROM kafka_transactions_invalid;