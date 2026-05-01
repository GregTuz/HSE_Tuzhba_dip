import asyncio
import json
import logging
import random

import pandas as pd
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

from config import settings
from models import TransactionModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Transaction Producer", version="0.2.0")

producer = Producer({
    "bootstrap.servers": settings.kafka_bootstrap_servers,
    "client.id": "fastapi-producer",
})


def delivery_report(err, msg):
    if err:
        logger.error(f"Ошибка доставки в {msg.topic()}: {err}")
    else:
        logger.debug(f"Доставлено в {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


stream_task: asyncio.Task | None = None


def load_df() -> pd.DataFrame:
    return pd.read_csv(settings.csv_path, sep=",")


def load_random_transaction() -> dict:
    df = load_df()
    return df.sample(1).iloc[0].to_dict()


def load_broken_transaction() -> dict:
    df = load_df()
    broken_slice = df.tail(settings.broken_transactions_count)
    return broken_slice.sample(1).iloc[0].to_dict()


def send_to_kafka(payload: bytes, key: bytes, is_valid: bool):
    topic = settings.kafka_topic_valid if is_valid else settings.kafka_topic_invalid
    producer.produce(
        topic=topic,
        key=key,
        value=payload,
        callback=delivery_report,
    )

    producer.poll(0)


def validate_transaction(data: dict) -> tuple[bytes, bytes, bool]:
    key = str(data.get("account_id", "unknown")).encode("utf-8")
    try:
        transaction = TransactionModel(**data)
        payload = transaction.model_dump_json().encode("utf-8")
        return payload, key, True
    except ValidationError:
        payload = json.dumps(data, default=str).encode("utf-8")
        return payload, key, False


async def stream_loop():
    logger.info("Стрим запущен")
    try:
        while True:
            count = random.randint(1, 500)
            logger.info(f"Отправка {count} транзакций...")

            for _ in range(count):
                data = load_random_transaction()
                payload, key, is_valid = validate_transaction(data)
                send_to_kafka(payload, key, is_valid)

            producer.flush(timeout=5)
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("Стрим остановлен")
        producer.flush(timeout=10)


@app.get(
    "/transaction",
    response_model=TransactionModel,
    summary="Получить случайную транзакцию из CSV",
)
def get_transaction():
    try:
        data = load_random_transaction()
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail=f"CSV файл не найден: {settings.csv_path}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка чтения CSV: {e}")

    try:
        return TransactionModel(**data)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Ошибка валидации: {e}")


@app.get(
    "/transaction/broken",
    summary="Получить случайную сломанную транзакцию из CSV",
)
def get_broken_transaction():
    try:
        data = load_broken_transaction()
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail=f"CSV файл не найден: {settings.csv_path}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка чтения CSV: {e}")

    return data


@app.post(
    "/transaction/send",
    summary="Валидировать и отправить одну транзакцию в Kafka",
)
def send_transaction(transaction: TransactionModel):
    try:
        payload = transaction.model_dump_json().encode("utf-8")
        key = str(transaction.account_id).encode("utf-8")
        send_to_kafka(payload, key, is_valid=True)
        producer.flush(timeout=5)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка отправки в Kafka: {e}")

    return {
        "status": "ok",
        "message": f"Транзакция {transaction.transaction_id} отправлена в Kafka",
        "topics": settings.kafka_topic_valid,
    }


@app.post(
    "/stream/start",
    summary="Запустить непрерывную отправку транзакций в Kafka",
)
async def start_stream():
    global stream_task

    if stream_task and not stream_task.done():
        raise HTTPException(status_code=400, detail="Стрим уже запущен")

    stream_task = asyncio.create_task(stream_loop())
    return {"status": "started", "message": "Стрим запущен. 1-500 транзакций каждую секунду."}


@app.post(
    "/stream/stop",
    summary="Остановить непрерывную отправку транзакций",
)
async def stop_stream():
    global stream_task

    if not stream_task or stream_task.done():
        raise HTTPException(status_code=400, detail="Стрим не запущен")

    stream_task.cancel()
    try:
        await stream_task
    except asyncio.CancelledError:
        pass

    return {"status": "stopped", "message": "Стрим остановлен"}


@app.get(
    "/stream/status",
    summary="Статус стрима",
)
def stream_status():
    running = stream_task is not None and not stream_task.done()
    return {"running": running}