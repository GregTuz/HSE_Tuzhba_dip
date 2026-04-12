from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


CURRENCIES = ["USD", "EUR", "RUB", "GBP"]


class CurrencyEnum(str, Enum):
    USD = "USD"
    EUR = "EUR"
    RUB = "RUB"
    GBP = "GBP"


class TransactionModel(BaseModel):
    model_config = ConfigDict(json_encoders={Decimal: float})

    transaction_id: int
    account_id: int
    timestamp: datetime
    transaction_type: Optional[str] = None
    amount: Decimal
    account_balance: Decimal
    transaction_date: Optional[str] = None
    merchant_category: Optional[str] = None
    country_code: Optional[str] = None
    account_level: Optional[str] = None
    currency: CurrencyEnum

    @field_validator("timestamp")
    @classmethod
    def timestamp_not_in_future(cls, v: datetime) -> datetime:
        now = datetime.now(tz=v.tzinfo)
        if v > now:
            raise ValueError(f"timestamp не может быть в будущем: {v}")
        return v

    @model_validator(mode="after")
    def amount_less_than_balance(self) -> "TransactionModel":
        if not (0 < self.amount < self.account_balance):
            raise ValueError(
                f"amount ({self.amount}) должен быть > 0 и < account_balance ({self.account_balance})"
            )
        return self