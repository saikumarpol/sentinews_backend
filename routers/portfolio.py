# backend/routers/portfolio.py
#
# Mount in app.py:
#   from routers import portfolio
#   app.include_router(portfolio.router)

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from pydantic import BaseModel
import yfinance as yf
import pandas as pd
from typing import List, Optional
from datetime import date, timedelta

from db import get_session, User, Holding
from auth import get_current_user

router = APIRouter(prefix="/portfolio", tags=["portfolio"])

# ── Pydantic schemas ──────────────────────────────────────────────────────────

class HoldingIn(BaseModel):
    symbol:    str
    qty:       float
    avg_price: float


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return float(val)
    except Exception:
        return None


def enrich_holding(h: "Holding") -> dict:
    """Fetch live price and metadata for a single holding via yfinance."""
    sym = h.symbol.upper()
    ticker = yf.Ticker(sym)
    current_price = None
    change_1d_pct = None
    change_1d = None
    sector = None
    name   = None
    mcap   = None
    beta   = None
    pe     = None
    div    = None
    high52 = None
    low52  = None
    asset  = "EQUITY"
    info   = {}

    try:
        # Note: when called in parallel, this is much faster
        info = ticker.info or {}
        current_price = info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose")
        
        # SMART FALLBACK: If no price came back, and no dot in symbol, assume it's an Indian stock missing .NS
        if not current_price and "." not in sym:
            ns_sym = f"{sym}.NS"
            ns_ticker = yf.Ticker(ns_sym)
            ns_info = ns_ticker.info or {}
            ns_price = ns_info.get("currentPrice") or ns_info.get("regularMarketPrice") or ns_info.get("previousClose")
            if ns_price:
                info = ns_info
                sym = ns_sym # use this corrected symbol from now on
    except Exception:
        pass

    try:
        current_price = safe(
            info.get("currentPrice")
            or info.get("regularMarketPrice")
            or info.get("previousClose")
        )
        prev = safe(info.get("previousClose"))
        if current_price and prev and prev != 0:
            change_1d = safe(current_price - prev)
            change_1d_pct = safe(((current_price - prev) / prev) * 100)
        sector = info.get("sector")
        name   = info.get("longName") or info.get("shortName")
        mcap   = safe(info.get("marketCap"))
        beta   = safe(info.get("beta"))
        pe     = safe(info.get("trailingPE") or info.get("forwardPE"))
        div    = safe(info.get("dividendYield"))
        if div is not None:
            div = div * 100 # Convert format 0.015 to 1.5%
        high52 = safe(info.get("fiftyTwoWeekHigh"))
        low52  = safe(info.get("fiftyTwoWeekLow"))
        asset  = info.get("quoteType", "EQUITY")
    except Exception:
        pass

    invested = safe(h.qty * h.avg_price)
    current  = safe(h.qty * current_price) if current_price else None
    pnl      = safe(current - invested)    if current is not None else None
    pnl_pct  = safe((pnl / invested) * 100) if pnl is not None and invested else None
    day_pnl  = safe(h.qty * change_1d) if change_1d is not None else None

    return {
        "symbol":         sym, # Use corrected symbol if fallback triggered
        "original_sym":   h.symbol,
        "qty":            h.qty,
        "avg_price":      h.avg_price,
        "current_price":  current_price,
        "current_value":  current,
        "invested_value": invested,
        "pnl":            pnl,
        "pnl_pct":        pnl_pct,
        "day_pnl":        day_pnl,
        "change_1d_pct":  change_1d_pct,
        "sector":         sector,
        "name":           name,
        "market_cap":     mcap,
        "beta":           beta,
        "pe_ratio":       pe,
        "dividend_yield": div,
        "high_52w":       high52,
        "low_52w":        low52,
        "asset_type":     asset,
    }

def build_advanced_portfolio_data(enriched: list, days: int = 1825) -> dict:
    """
    Reconstruct daily portfolio value for the past 5 years (1825 days).
    Calculates Multi-Horizon Returns and Max Drawdown per stock and portfolio.
    """
    if not enriched:
        return {"history": [], "stock_perf": {}, "port_perf": {}}

    symbols = [h["symbol"] for h in enriched]
    qty_map = {h["symbol"]: h["qty"] for h in enriched}

    end   = date.today()
    start = end - timedelta(days=days + 30)

    try:
        # BATCH DOWNLOAD is 10x faster than looping
        df = yf.download(
            symbols,
            start=start.isoformat(),
            end=end.isoformat(),
            interval="1d",
            progress=False,
            auto_adjust=True,
            # group_by="ticker" # Removed to keep simpler MultiIndex if >1, else flat
        )
        if df.empty:
            return {"history": [], "stock_perf": {}, "port_perf": {}}

        # Calculate individual stock performance matrix
        stock_perf = {}
        for sym in symbols:
            try:
                # Handle single vs multi-symbol DF
                if len(symbols) > 1:
                    data = df["Close"][sym]
                else:
                    data = df["Close"]
                
                series = data.ffill().dropna()
                if series.empty: continue
                
                last = series.iloc[-1]
                def get_pct(lookback):
                    if len(series) > lookback:
                        return safe(((last - series.iloc[-lookback]) / series.iloc[-lookback]) * 100)
                    return None
                    
                # Approx trading days: 1W=5, 1M=21, 3M=63, 1Y=252, 3Y=756, 5Y=1260
                stock_perf[sym] = {
                    "1W": get_pct(5),
                    "1M": get_pct(21),
                    "3M": get_pct(63),
                    "1Y": get_pct(252),
                    "3Y": get_pct(756),
                    "5Y": get_pct(1260),
                }
                # Max drawdown 1Y
                if len(series) > 252:
                    y1_series = series.tail(252)
                    roll_max = y1_series.cummax()
                    drawdown = (y1_series - roll_max) / roll_max
                    stock_perf[sym]["max_drawdown_1y"] = safe(drawdown.min() * 100)
                else:
                    stock_perf[sym]["max_drawdown_1y"] = None
                    
            except Exception:
                pass

        # Reconstruct total portfolio value series
        daily_values = pd.Series(0.0, index=df.index)
        
        for sym in symbols:
            try:
                if len(symbols) > 1:
                    close = df["Close"][sym]
                else:
                    close = df["Close"]
                daily_values += close.ffill() * qty_map[sym]
            except Exception:
                continue

        # MODERN PANDAS FIX: fillna(method="ffill") -> ffill()
        combined = daily_values.ffill().dropna()
        
        port_last = combined.iloc[-1] if not combined.empty else 0
        def get_port_pct(lookback):
            if len(combined) > lookback and combined.iloc[-lookback] != 0:
                return safe(((port_last - combined.iloc[-lookback]) / combined.iloc[-lookback]) * 100)
            return None

        port_perf = {
            "1W": get_port_pct(5),
            "1M": get_port_pct(21),
            "3M": get_port_pct(63),
            "1Y": get_port_pct(252),
            "3Y": get_port_pct(756),
            "5Y": get_port_pct(1260),
        }
        
        # Portfolio Max Drawdown 1Y
        if len(combined) > 252:
            y1_port = combined.tail(252)
            rmax = y1_port.cummax()
            dd = (y1_port - rmax) / rmax
            port_perf["max_drawdown_1y"] = safe(dd.min() * 100)
        else:
            port_perf["max_drawdown_1y"] = None

        history = [
            {"date": str(idx.date()), "value": safe(val)}
            for idx, val in combined.tail(252).items() # return only 1yr of visual history to save payload size
        ]
        
        return {"history": history, "stock_perf": stock_perf, "port_perf": port_perf}
        
    except Exception as e:
        import logging
        logging.getLogger("sentinews.portfolio").error(f"History build failed: {e}")
        return {"history": [], "stock_perf": {}, "port_perf": {}}

def get_benchmark_history(days: int = 365) -> list:
    """Fetch NIFTY 50 (^NSEI) history for comparison against portfolio."""
    end = date.today()
    start = end - timedelta(days=days + 30)

    try:
        df = yf.download(
            "^NSEI",
            start=start.isoformat(),
            end=end.isoformat(),
            interval="1d",
            progress=False,
            auto_adjust=True,
        )
        if df.empty:
            return []

        close = df["Close"]
        if isinstance(close, pd.DataFrame):
            close = close["^NSEI"]
            
        combined = close.ffill().dropna()

        return [
            {"date": str(idx.date()), "value": safe(val)}
            for idx, val in combined.tail(days).items()
        ]
    except Exception as e:
        import logging
        logging.getLogger("sentinews.portfolio").error(f"Benchmark history build failed: {e}")
        return []


# ── CRUD endpoints ────────────────────────────────────────────────────────────

@router.post("", status_code=201)
def add_holding(
    payload: HoldingIn,
    user: User = Depends(get_current_user),
):
    sym = payload.symbol.strip().upper()
    with get_session() as db:
        existing = db.exec(
            select(Holding).where(Holding.user_id == user.id, Holding.symbol == sym)
        ).first()
        if existing:
            # Weighted-average merge on duplicate adds
            total_qty  = existing.qty + payload.qty
            avg_price  = (
                (existing.qty * existing.avg_price + payload.qty * payload.avg_price)
                / total_qty
            )
            existing.qty       = total_qty
            existing.avg_price = avg_price
        else:
            db.add(Holding(
                user_id=user.id,
                symbol=sym,
                qty=payload.qty,
                avg_price=payload.avg_price,
            ))
        db.commit()
    return {"status": "ok"}


@router.delete("/{symbol}")
def remove_holding(
    symbol: str,
    user: User = Depends(get_current_user),
):
    sym = symbol.strip().upper()
    with get_session() as db:
        h = db.exec(
            select(Holding).where(Holding.user_id == user.id, Holding.symbol == sym)
        ).first()
        if not h:
            raise HTTPException(404, "Holding not found")
        db.delete(h)
        db.commit()
    return {"status": "ok"}


@router.get("")
def list_holdings(
    user: User = Depends(get_current_user),
):
    with get_session() as db:
        rows = db.exec(select(Holding).where(Holding.user_id == user.id)).all()
        return [
            {
                "symbol": r.symbol, "qty": r.qty, "avg_price": r.avg_price,
                "current_price": None, "current_value": None,
                "invested_value": None, "pnl": None, "pnl_pct": None,
                "change_1d_pct": None, "sector": None, "name": None,
            }
            for r in rows
        ]


@router.get("/snapshot")
def get_portfolio_snapshot(
    user: User = Depends(get_current_user),
):
    """
    Enriched portfolio: live prices, P&L, totals, and history.
    NOTE: calls yfinance for every holding — consider a cache/background task
    for production use.
    """
    with get_session() as db:
        rows = db.exec(select(Holding).where(Holding.user_id == user.id)).all()

    if not rows:
        return {
            "holdings": [], "total_invested": 0, "total_current": 0,
            "total_pnl": 0, "total_pnl_pct": 0, "total_day_pnl": 0, 
            "total_day_pnl_pct": 0, "history": [], "sector_data": [], "mcap_data": [],
            "asset_data": [], "diversification_score": 0, "portfolio_performance": {}
        }

    # PARALLEL ENRICHMENT for info fetch
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:
        enriched = list(executor.map(enrich_holding, rows))

    total_invested = sum(h["invested_value"] or 0 for h in enriched)
    total_current  = sum(h["current_value"]  or 0 for h in enriched)
    total_pnl      = total_current - total_invested
    total_pnl_pct  = safe((total_pnl / total_invested) * 100) if total_invested else None
    total_day_pnl  = sum(h["day_pnl"] or 0 for h in enriched)
    
    # Calculate Day PnL Pct based on previous day's total value
    previous_total = total_current - total_day_pnl
    total_day_pnl_pct = safe((total_day_pnl / previous_total) * 100) if previous_total else None

    # Analysis Center data & Diversification
    sector_weightage = {}
    mcap_weightage = {"Large Cap": 0, "Mid Cap": 0, "Small Cap": 0, "Unknown": 0}
    asset_weightage = {}
    
    for h in enriched:
        val = h["current_value"] or 0
        sec = h["sector"] or "Other"
        sector_weightage[sec] = sector_weightage.get(sec, 0) + val
        
        asset = h["asset_type"] or "EQUITY"
        asset_weightage[asset] = asset_weightage.get(asset, 0) + val
        
        mcap = h["market_cap"] or 0
        if mcap >= 20000e7: # > 20,000 Cr
            mcap_weightage["Large Cap"] += val
        elif mcap >= 5000e7: # 5k - 20k Cr
            mcap_weightage["Mid Cap"] += val
        elif mcap > 0:
            mcap_weightage["Small Cap"] += val
        else:
            mcap_weightage["Unknown"] += val

    # Smart Diversification Score Logic (0-100)
    div_score = 0
    num_holdings = len(enriched)
    if num_holdings >= 15: div_score += 30
    elif num_holdings >= 8: div_score += 20
    elif num_holdings >= 4: div_score += 10
    
    if total_current > 0:
        max_sector_weight = max(sector_weightage.values()) / total_current
        if max_sector_weight <= 0.25: div_score += 35
        elif max_sector_weight <= 0.40: div_score += 20
        elif max_sector_weight <= 0.60: div_score += 10
        
        max_single_weight = max([h["current_value"] or 0 for h in enriched]) / total_current
        if max_single_weight <= 0.10: div_score += 35
        elif max_single_weight <= 0.20: div_score += 25
        elif max_single_weight <= 0.35: div_score += 10

    # Format for charts (sort by value descending)
    sector_data = [{"name": k, "value": safe(v)} for k, v in sorted(sector_weightage.items(), key=lambda i: i[1], reverse=True) if v > 0]
    mcap_data = [{"name": k, "value": safe(v)} for k, v in mcap_weightage.items() if v > 0]
    asset_data = [{"name": k, "value": safe(v)} for k, v in asset_weightage.items() if v > 0]

    advanced_data = build_advanced_portfolio_data(enriched)
    
    # Inject stock perf into enriched
    stock_perf = advanced_data.get("stock_perf", {})
    for h in enriched:
        h["performance"] = stock_perf.get(h["symbol"], {})

    benchmark_history = get_benchmark_history(days=252) # Only fetch 1Y visual history

    return {
        "holdings":       enriched,
        "total_invested": safe(total_invested),
        "total_current":  safe(total_current),
        "total_pnl":      safe(total_pnl),
        "total_pnl_pct":  total_pnl_pct,
        "total_day_pnl":  safe(total_day_pnl),
        "total_day_pnl_pct": total_day_pnl_pct,
        "diversification_score": min(div_score, 100),
        "portfolio_performance": advanced_data.get("port_perf", {}),
        "history":        advanced_data.get("history", []),
        "benchmark_history": benchmark_history,
        "sector_data":    sector_data,
        "mcap_data":      mcap_data,
        "asset_data":     asset_data,
    }


@router.get("/news")
def get_portfolio_news(
    user: User = Depends(get_current_user),
):
    """Aggregate news for all holdings."""
    with get_session() as db:
        rows = db.exec(select(Holding).where(Holding.user_id == user.id)).all()

    symbols   = [r.symbol for r in rows]
    all_news  = []

    for sym in symbols:
        try:
            ticker = yf.Ticker(sym)
            raw    = ticker.news or []
            for item in raw[:5]:
                published_ts = item.get("providerPublishTime", 0)
                all_news.append({
                    "id":           item.get("uuid", ""),
                    "headline":     item.get("title", ""),
                    "summary":      item.get("summary", ""),
                    "source":       item.get("publisher", ""),
                    "url":          item.get("link", ""),
                    "published_at": None,
                    "sentiment":    0.0,
                    "action":       "hold",
                    "tickers":      item.get("relatedTickers", [sym]),
                })
        except Exception:
            continue

    # Deduplicate by id, sort by most recent
    seen, dedup = set(), []
    for n in all_news:
        if n["id"] not in seen:
            seen.add(n["id"])
            dedup.append(n)

    return {"news": dedup}
