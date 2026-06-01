from typing import Any, List, Optional, Dict
from pydantic import BaseModel, Field

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    user_id: Optional[int] = None

class UserBase(BaseModel):
    email: str

class UserCreate(UserBase):
    password: str

class UserResponse(UserBase):
    id: int

    class Config:
        from_attributes = True

class SignupBody(BaseModel):
    email: str
    password: str

class ForgotPasswordBody(BaseModel):
    email: str

class NoteBody(BaseModel):
    text: str

class NoteResponse(BaseModel):
    symbol: str
    text: str
    updated_at: str

class SearchResult(BaseModel):
    symbol: str
    name: str
    exchange: Optional[str] = None
    type: Optional[str] = None

class Snapshot(BaseModel):
    last_price: float
    change_1d_pct: Optional[float] = None
    high_52w: Optional[float] = None
    low_52w: Optional[float] = None

class PerformanceResponse(BaseModel):
    symbol: str
    last_price: float
    last_date: str
    performance: dict
    history: List[Dict]
    snapshot: Optional[Snapshot] = None

class DigestRequest(BaseModel):
    watchlist: Optional[List[str]] = []
