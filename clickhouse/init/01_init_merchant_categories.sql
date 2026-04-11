CREATE TABLE IF NOT EXISTS dict_merchant_category_source
(
    category_code String,
    category_name String,
    risk_score    UInt8,
    is_online     UInt8
) ENGINE = MergeTree()
ORDER BY category_code;

CREATE DICTIONARY IF NOT EXISTS dict_merchant_category
(
    category_code String,
    category_name String,
    risk_score    UInt8,
    is_online     UInt8
)
PRIMARY KEY category_code
SOURCE(CLICKHOUSE(TABLE 'dict_merchant_category_source'))
LAYOUT(FLAT())
LIFETIME(0);