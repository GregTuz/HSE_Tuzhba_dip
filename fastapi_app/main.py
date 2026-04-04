import json

import pandas as pd
from fastapi import FastAPI, HTTPException

from config import settings
from models import TransactionModel

app = FastAPI(title="Transaction Producer", version="0.1.0")


def load_random_transaction() -> dict:
    df = pd.read_csv(settings.csv_path, sep=",")
    row = df.sample(1).iloc[0].to_dict()
    return row


@app.get("/transaction", response_model=TransactionModel, summary="Получить случайную транзакцию из CSV")
def get_transaction():
    try:
        data = load_random_transaction()
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail=f"CSV файл не найден: {settings.csv_path}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка чтения CSV: {e}")

    try:
        transaction = TransactionModel(**data)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Ошибка валидации данных из CSV: {e}")

    return transaction


@app.post("/transaction/send", summary="Валидировать и отправить транзакцию в Kafka")
def send_transaction(transaction: TransactionModel):
    payload = json.loads(transaction.model_dump_json())

    return {
        "status": "ok",
        "message": f"Транзакция {transaction.transaction_id} прошла валидацию и готова к отправке",
        "payload": payload,
    }