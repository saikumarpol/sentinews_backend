from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import select, Session
from src.api.deps import get_db, get_current_user
from src.models.models import User, StockNote
from src.schemas.schemas import NoteBody, NoteResponse, PerformanceResponse, Snapshot
from src.services.market_service import fetch_daily_history, compute_performance, _validate_symbol

router = APIRouter()

@router.get("/{symbol}/performance", response_model=PerformanceResponse)
async def get_performance(symbol: str):
    history = await fetch_daily_history(symbol)
    perf = compute_performance(history)
    last_price = history[-1]["close"]
    last_date = history[-1]["date"]
    
    # Simple snapshot
    snapshot = Snapshot(last_price=last_price)
    if len(history) >= 2:
        prev_close = history[-2]["close"]
        snapshot.change_1d_pct = round((last_price - prev_close) / prev_close * 100, 2)
    
    return PerformanceResponse(
        symbol=symbol,
        last_price=last_price,
        last_date=last_date,
        performance=perf,
        history=history[-30:],  # last 30 days
        snapshot=snapshot
    )

@router.get("/notes/{symbol}", response_model=NoteResponse)
def get_note(symbol: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    symbol = _validate_symbol(symbol)
    note = db.exec(select(StockNote).where(StockNote.user_id == current_user.id, StockNote.symbol == symbol)).first()
    if not note:
        return NoteResponse(symbol=symbol, text="", updated_at="")
    return NoteResponse(symbol=symbol, text=note.text, updated_at=note.updated_at.isoformat())

@router.post("/notes/{symbol}", response_model=NoteResponse)
def upsert_note(symbol: str, body: NoteBody, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    symbol = _validate_symbol(symbol)
    note = db.exec(select(StockNote).where(StockNote.user_id == current_user.id, StockNote.symbol == symbol)).first()
    if note:
        note.text = body.text
        db.add(note)
    else:
        note = StockNote(user_id=current_user.id, symbol=symbol, text=body.text)
        db.add(note)
    db.commit()
    db.refresh(note)
    return NoteResponse(symbol=symbol, text=note.text, updated_at=note.updated_at.isoformat())
