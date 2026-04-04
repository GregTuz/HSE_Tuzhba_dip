from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    csv_path: str = "/app/data/Financial_Transactions_Enriched.csv"
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "transactions"

    class Config:
        env_file = ".env"


settings = Settings()
