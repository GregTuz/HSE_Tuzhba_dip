CREATE MATERIALIZED VIEW IF NOT EXISTS mv_transactions_valid
TO transactions_valid
AS
SELECT
    t.transaction_id,
    t.account_id,
    t.account_level,
    dictGetString('dict_account_level', 'level_name', t.account_level) AS level_name,
    toDecimal64(dictGet('dict_account_level', 'daily_limit_rub', t.account_level), 2) AS daily_limit_rub,
    toDecimal64(dictGet('dict_account_level', 'monthly_limit_rub', t.account_level), 2) AS monthly_limit_rub,
    t.amount,
    toFloat64(t.amount) * toFloat64(cr_txn.rate_to_rub) AS amount_rub,
    t.account_balance,
    t.currency,
    t.transaction_type,
    t.merchant_category,
    dictGetString('dict_merchant_category', 'category_name', t.merchant_category) AS category_name,
    dictGetUInt8('dict_merchant_category', 'risk_score', t.merchant_category) AS risk_score,
    dictGetUInt8('dict_merchant_category', 'is_online', t.merchant_category) AS is_online,
    t.country_code,
    dictGetString('dict_country', 'country_name', t.country_code) AS country_name,
    dictGetString('dict_country', 'region', t.country_code) AS region,
    dictGetUInt8('dict_country', 'risk_level', t.country_code) AS risk_level,
    parseDateTimeBestEffort(t.timestamp) AS timestamp,
    toDate(t.transaction_date) AS transaction_date,
    now() AS ingested_at

FROM kafka_transactions_valid AS t

LEFT JOIN (
    SELECT dt, currency, rate_to_rub
    FROM currency_rates
) AS cr_txn ON toDate(t.transaction_date) = cr_txn.dt AND t.currency = cr_txn.currency;