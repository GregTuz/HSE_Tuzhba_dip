CREATE TABLE IF NOT EXISTS dict_account_level_source
(
    level_code        String,
    level_name        String,
    daily_limit_rub   Decimal(18, 2),
    monthly_limit_rub Decimal(18, 2)
) ENGINE = MergeTree()
ORDER BY level_code;

CREATE DICTIONARY IF NOT EXISTS dict_account_level
(
    level_code        String,
    level_name        String,
    daily_limit_rub   Decimal(18, 2),
    monthly_limit_rub Decimal(18, 2)
)
PRIMARY KEY level_code
SOURCE(CLICKHOUSE(TABLE 'dict_account_level_source'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(0);