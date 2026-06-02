# backend/db.py

import os
from typing import Optional
from datetime import datetime

from sqlmodel import SQLModel, Field, create_engine, Session
from sqlalchemy import UniqueConstraint

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./sentinews.db")
engine = create_engine(DATABASE_URL, echo=False)


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str
    hashed_password: str


# ---------- per-stock notes ----------

class StockNote(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(index=True)
    symbol: str = Field(index=True)
    text: str = ""
    updated_at: datetime = Field(default_factory=datetime.utcnow)


# ---------- portfolio holdings ----------

class Holding(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(index=True)
    symbol: str = Field(index=True)
    qty: float
    avg_price: float

    class Config:
        # Enforce unique (user_id, symbol) at the DB level via SQLAlchemy
        __table_args__ = (UniqueConstraint("user_id", "symbol"),)


def init_db():
    SQLModel.metadata.create_all(engine)


def get_session():
    return Session(engine)
