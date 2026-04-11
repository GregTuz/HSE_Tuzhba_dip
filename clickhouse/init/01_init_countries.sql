CREATE TABLE IF NOT EXISTS dict_country_source
(
    country_code String,
    country_name String,
    region       String,
    risk_level   UInt8
) ENGINE = MergeTree()
ORDER BY country_code;

CREATE DICTIONARY IF NOT EXISTS dict_country
(
    country_code String,
    country_name String,
    region       String,
    risk_level   UInt8
)
PRIMARY KEY country_code
SOURCE(CLICKHOUSE(TABLE 'dict_country_source'))
LAYOUT(FLAT())
LIFETIME(0);