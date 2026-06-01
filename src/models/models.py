from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, UniqueConstraint

class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(index=True, unique=True)
    hashed_password: str

class StockNote(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(index=True)
    symbol: str = Field(index=True)
    text: str = ""
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class Holding(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(index=True)
    symbol: str = Field(index=True)
    qty: float
    avg_price: float

    __table_args__ = (UniqueConstraint("user_id", "symbol"),)
