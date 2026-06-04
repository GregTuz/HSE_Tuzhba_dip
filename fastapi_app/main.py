import asyncio
import json
import logging
import random
import time

import pandas as pd
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

from config import settings
from models import TransactionModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Transaction Producer", version="0.3.0")

producer = Producer({
    "bootstrap.servers": settings.kafka_bootstrap_servers,
    "client.id": "fastapi-producer",
    "linger.ms": 5,
    "batch.size": 65536,
    "compression.type": "lz4",
    "queue.buffering.max.messages": 500000,
    "queue.buffering.max.kbytes": 524288,
})

_df_cache = None
_records_cache = None
_broken_cache = None


def get_df():
    global _df_cache
    if _df_cache is None:
        logger.info(f"Загружаем CSV: {settings.csv_path}")
        _df_cache = pd.read_csv(settings.csv_path, sep=",")
        logger.info(f"CSV загружен: {len(_df_cache)} строк")
    return _df_cache


def get_records():
    global _records_cache
    if _records_cache is None:
        df = get_df()
        normal = df.iloc[:-settings.broken_transactions_count]
        _records_cache = normal.to_dict(orient='records')
        logger.info(f"Кэш нормальных транзакций: {len(_records_cache)} записей")
    return _records_cache


def get_broken_records():
    global _broken_cache
    if _broken_cache is None:
        df = get_df()
        broken = df.tail(settings.broken_transactions_count)
        _broken_cache = broken.to_dict(orient='records')
        logger.info(f"Кэш сломанных транзакций: {len(_broken_cache)} записей")
    return _broken_cache


def load_random_transaction():
    return random.choice(get_records())


def load_broken_transaction():
    return random.choice(get_broken_records())


def delivery_report(err, msg):
    if err:
        logger.error(f"Ошибка доставки в {msg.topic()}: {err}")
    else:
        logger.debug(f"Доставлено в {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def send_to_kafka(payload, key, is_valid):
    topic = settings.kafka_topic_valid if is_valid else settings.kafka_topic_invalid
    producer.produce(topic=topic, key=key, value=payload, callback=delivery_report)
    producer.poll(0)


def validate_transaction(data):
    key = str(data.get("account_id", "unknown")).encode("utf-8")
    try:
        transaction = TransactionModel(**data)
        payload = transaction.model_dump_json().encode("utf-8")
        return payload, key, True
    except ValidationError:
        payload = json.dumps(data, default=str).encode("utf-8")
        return payload, key, False


stream_task = None
STREAM_BATCH_SIZE = 2000


async def stream_loop():
    records = get_records()
    n = len(records)
    tx_id = int(time.time() * 1000)
    iteration = 0
    recent_ids = []  # буфер последних ID для повторной отправки

    try:
        while True:
            t_start = time.perf_counter()
            indices = [random.randrange(n) for _ in range(STREAM_BATCH_SIZE)]

            sent = 0
            for idx in indices:
                data = records[idx].copy()

                # 100 дублей из предыдущей итерации на каждый батч
                if recent_ids and sent < 100:
                    data['transaction_id'] = random.choice(recent_ids)
                else:
                    data['transaction_id'] = tx_id
                    recent_ids.append(tx_id)
                    tx_id += 1
                    # Держим буфер не больше 1000 последних ID
                    if len(recent_ids) > 333:
                        recent_ids.pop(0)

                key = str(data.get('account_id', 'unknown')).encode('utf-8')
                payload = json.dumps(data, default=str).encode('utf-8')
                producer.produce(
                    topic=settings.kafka_topic_valid,
                    key=key,
                    value=payload,
                    callback=delivery_report,
                )
                producer.poll(0)
                sent += 1

            await asyncio.get_event_loop().run_in_executor(
                None, lambda: producer.flush(timeout=1)
            )

            t_end = time.perf_counter()
            elapsed = t_end - t_start
            iteration += 1

            ts = time.strftime('%H:%M:%S')
            logger.info(
                f"[{ts}] iter={iteration} "
                f"sent={sent} "
                f"elapsed={elapsed:.3f}s "
                f"rate={sent/elapsed:.0f} msg/s "
                f"tx_id_last={tx_id}"
            )

            await asyncio.sleep(max(0, 1 - elapsed))

    except asyncio.CancelledError:
        logger.info("Стрим остановлен")
        producer.flush(timeout=10)


@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, get_records)
    await loop.run_in_executor(None, get_broken_records)


@app.get("/transaction", response_model=TransactionModel)
def get_transaction():
    try:
        data = load_random_transaction()
        return TransactionModel(**data)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail=f"CSV не найден: {settings.csv_path}")
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=f"Ошибка валидации: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка: {e}")


@app.get("/transaction/broken")
def get_broken_transaction():
    try:
        return load_broken_transaction()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/transaction/send")
def send_transaction(transaction: TransactionModel):
    try:
        payload = transaction.model_dump_json().encode("utf-8")
        key = str(transaction.account_id).encode("utf-8")
        send_to_kafka(payload, key, is_valid=True)
        producer.flush(timeout=5)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "ok", "message": f"Транзакция {transaction.transaction_id} отправлена"}


@app.post("/stream/start")
async def start_stream():
    global stream_task
    if stream_task and not stream_task.done():
        raise HTTPException(status_code=400, detail="Стрим уже запущен")
    stream_task = asyncio.create_task(stream_loop())
    return {"status": "started", "message": f"Стрим запущен. {STREAM_BATCH_SIZE} транзакций/сек."}


@app.post("/stream/stop")
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


@app.get("/stream/status")
def stream_status():
    running = stream_task is not None and not stream_task.done()
    return {"running": running}
