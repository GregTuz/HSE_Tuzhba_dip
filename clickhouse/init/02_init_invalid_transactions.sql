CREATE TABLE IF NOT EXISTS kafka_transactions_invalid
(
    transaction_id Int64,
    account_id Int64,
    timestamp String,
    transaction_type String,
    amount Decimal(18, 2),
    account_balance Decimal(18, 2),
    transaction_date String,
    merchant_category String,
    country_code String,
    account_level String,
    currency String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions.invalid',
    kafka_group_name = 'clickhouse_invalid_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 10000,
    kafka_poll_timeout_ms = 10000,
    kafka_flush_interval_ms = 10000;