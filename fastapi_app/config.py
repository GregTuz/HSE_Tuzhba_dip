from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # csv_path: str = "/app/data/Financial_Transactions_Enriched.csv"
    csv_path: str = "/app/data/Financial_Transactions_15M.csv"
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_valid: str = "transactions.valid"
    kafka_topic_invalid: str = "transactions.invalid"
    broken_transactions_count: int = 500

    class Config:
        env_file = ".env"


settings = Settings()