from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import select, Session
from src.api.deps import get_db, get_current_user
from src.models.models import User, Holding
from pydantic import BaseModel

router = APIRouter()

class HoldingCreate(BaseModel):
    symbol: str
    qty: float
    avg_price: float

@router.get("/snapshot")
def get_portfolio_snapshot(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    holdings = db.exec(select(Holding).where(Holding.user_id == current_user.id)).all()
    return {"holdings": holdings}

@router.post("/")
def add_holding(holding: HoldingCreate, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    existing = db.exec(select(Holding).where(Holding.user_id == current_user.id, Holding.symbol == holding.symbol.upper())).first()
    if existing:
        existing.qty = holding.qty
        existing.avg_price = holding.avg_price
        db.add(existing)
    else:
        new_holding = Holding(user_id=current_user.id, symbol=holding.symbol.upper(), qty=holding.qty, avg_price=holding.avg_price)
        db.add(new_holding)
    db.commit()
    return {"message": "Holding updated"}

@router.delete("/{symbol}")
def remove_holding(symbol: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    holding = db.exec(select(Holding).where(Holding.user_id == current_user.id, Holding.symbol == symbol.upper())).first()
    if not holding:
        raise HTTPException(status_code=404, detail="Holding not found")
    db.delete(holding)
    db.commit()
    return {"message": "Holding removed"}
