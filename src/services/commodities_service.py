import os
import math
import logging
import asyncio
import httpx
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import yfinance as yf
from src.core.config import settings
from src.services.ai_service import score_sentiment

logger = logging.getLogger("sentinews.commodities")

# --- TTL Caching ---
__comm_cache: Dict[str, Dict] = {}

def _get_cached_data(key: str, ttl_seconds: int):
    global __comm_cache
    now = datetime.now(timezone.utc)
    entry = __comm_cache.get(key)
    if entry and entry["expires_at"] > now:
        return entry["data"]
    return None

def _set_cached_data(key: str, data: Any, ttl_seconds: int):
    global __comm_cache
    __comm_cache[key] = {
        "data": data,
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds),
    }

# Symbol definitions
COMMODITY_DEFS = [
    {"yf": "CL=F", "td": "CRUDEOIL", "name": "Crude Oil (WTI)", "category": "Energy", "unit": "USD/Bbl"},
    {"yf": "BZ=F", "td": "BRENT", "name": "Brent Crude", "category": "Energy", "unit": "USD/Bbl"},
    {"yf": "NG=F", "td": "NG", "name": "Natural Gas", "category": "Energy", "unit": "USD/MMBtu"},
    {"yf": "GC=F", "td": "XAU", "name": "Gold", "category": "Metals", "unit": "USD/oz"},
    {"yf": "SI=F", "td": "XAG", "name": "Silver", "category": "Metals", "unit": "USD/oz"},
    {"yf": "HG=F", "td": "HG", "name": "Copper", "category": "Metals", "unit": "USD/lb"},
]

INDEX_DEFS = [
    {"yf": "^NSEI", "name": "Nifty 50", "td": "NIFTY"},
    {"yf": "^BSESN", "name": "Sensex", "td": "SENSEX"},
    {"yf": "^GSPC", "name": "S&P 500", "td": "SPX"},
    {"yf": "^DJI", "name": "Dow Jones", "td": "DJI"},
    {"yf": "^IXIC", "name": "Nasdaq Comp", "td": "IXIC"},
]

CURRENCY_DEFS = [
    {"yf": "USDINR=X", "name": "USD/INR", "td": "USDINR"},
    {"yf": "EURUSD=X", "name": "EUR/USD", "td": "EURUSD"},
    {"yf": "GBPUSD=X", "name": "GBP/USD", "td": "GBPUSD"},
]

def _safe(val) -> Optional[float]:
    try:
        f = float(val)
        return None if math.isnan(f) or math.isinf(f) else round(f, 4)
    except (TypeError, ValueError):
        return None

def _pct(curr, prev) -> Optional[float]:
    try:
        if curr and prev and prev != 0:
            return round((curr - prev) / prev * 100, 2)
    except Exception:
        pass
    return None

async def _yf_history(symbol: str, period: str = "3mo") -> Optional[Any]:
    try:
        def _fetch():
            ticker = yf.Ticker(symbol)
            return ticker.history(period=period, interval="1d", auto_adjust=True)
        h = await asyncio.to_thread(_fetch)
        if h.empty: return None
        return h["Close"].dropna()
    except Exception as exc:
        logger.debug("yf history failed for %s: %s", symbol, exc)
        return None

def _quote_from_series(close) -> Dict:
    empty = {"price": None, "day_change": None, "day_pct": None,
             "weekly_pct": None, "monthly_pct": None, "high_52w": None, "low_52w": None}
    if close is None or len(close) < 2:
        return empty
    curr = _safe(close.iloc[-1])
    prev = _safe(close.iloc[-2])
    w_ago = _safe(close.iloc[-6]) if len(close) >= 6 else None
    m_ago = _safe(close.iloc[-22]) if len(close) >= 22 else None
    return {
        "price": curr,
        "day_change": _safe(curr - prev) if curr and prev else None,
        "day_pct": _pct(curr, prev),
        "weekly_pct": _pct(curr, w_ago),
        "monthly_pct": _pct(curr, m_ago),
        "high_52w": _safe(float(close.max())),
        "low_52w": _safe(float(close.min())),
    }

async def _td_quote(td_symbol: str) -> Optional[float]:
    if not settings.TWELVEDATA_API_KEY or not td_symbol:
        return None
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.twelvedata.com/price",
                params={"symbol": td_symbol, "apikey": settings.TWELVEDATA_API_KEY},
                timeout=8,
            )
            d = resp.json()
            if d.get("status") == "error": return None
            return _safe(d.get("price"))
    except Exception:
        return None

async def _build_single_item(d: Dict, category_default: str, now: str) -> Dict:
    sym = d["yf"]
    close = await _yf_history(sym, period="3mo")
    q = _quote_from_series(close)
    price = q.get("price")
    if (price is None or price <= 0) and d.get("td"):
        new_price = await _td_quote(d["td"])
        if new_price:
            q["price"] = new_price
            price = new_price
    
    # Simple Technical Sentiment
    tech_score = 0.5
    if q.get("high_52w") and q.get("low_52w") and q["high_52w"] > q["low_52w"]:
        p = price or 0.0
        tech_score = (p - q["low_52w"]) / (q["high_52w"] - q["low_52w"])
        tech_score = max(0, min(1, tech_score))
    
    action = "NEUTRAL"
    if tech_score > 0.65: action = "BULLISH"
    elif tech_score < 0.35: action = "BEARISH"

    return {
        "name": d["name"],
        "symbol": sym,
        "td_symbol": d.get("td", ""),
        "category": d.get("category", category_default),
        "unit": d.get("unit", ""),
        "price": price or 0.0,
        "day_change": q.get("day_change") or 0.0,
        "day_pct": q.get("day_pct") or 0.0,
        "sentiment": round(float(tech_score), 2),
        "action": action,
        "weekly_pct": q.get("weekly_pct"),
        "monthly_pct": q.get("monthly_pct"),
        "high_52w": q.get("high_52w"),
        "low_52w": q.get("low_52w"),
        "last_update": now,
        "source": "yfinance" if (price and price != 0.0) else "unavailable",
    }

async def fetch_market_dashboard(is_futures: bool = False) -> Dict[str, Any]:
    cache_key = "dashboard_futures" if is_futures else "dashboard"
    cached = _get_cached_data(cache_key, 60)
    if cached: return cached

    now = datetime.now(timezone.utc).isoformat()
    c_tasks = [_build_single_item(d, "Commodity", now) for d in COMMODITY_DEFS]
    i_tasks = [_build_single_item(d, "Indices", now) for d in INDEX_DEFS]
    k_tasks = [_build_single_item(d, "Currencies", now) for d in CURRENCY_DEFS]

    commodities_res = await asyncio.gather(*c_tasks)
    indices_res = await asyncio.gather(*i_tasks)
    currencies_res = await asyncio.gather(*k_tasks)

    res = {
        "commodities": commodities_res,
        "currencies": currencies_res,
        "indices": indices_res,
        "last_updated": now,
        "is_futures": is_futures,
    }
    _set_cached_data(cache_key, res, 60)
    return res
