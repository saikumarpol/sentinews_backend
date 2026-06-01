import re
import logging
import httpx
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from fastapi import HTTPException
from src.core.config import settings

logger = logging.getLogger("sentinews.market_service")

# simple in-memory cache for price history: {symbol: (expires_at, history_list)}
_price_cache: Dict[str, Tuple[datetime, List[Dict]]] = {}

def _validate_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    # allow letters, digits, dot and dash
    if not s or any(c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-" for c in s):
        raise HTTPException(status_code=400, detail="Invalid symbol")
    return s

async def fetch_daily_history(symbol: str) -> List[Dict]:
    """
    Fetch daily close prices using Twelve Data TIME_SERIES.
    """
    if not settings.TWELVEDATA_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="Price API key not configured",
        )

    symbol = _validate_symbol(symbol)

    now = datetime.utcnow()
    cached = _price_cache.get(symbol)
    if cached and cached[0] > now:
        return cached[1]

    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "1day",
        "outputsize": 5000,
        "apikey": settings.TWELVEDATA_API_KEY,
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(url, params=params)
    except httpx.RequestError:
        raise HTTPException(status_code=502, detail="Price data service unavailable")

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Price data error")

    data = resp.json()
    if data.get("status") == "error":
        msg = data.get("message", "Price data error")
        if "exceeded" in msg.lower() or "limit" in msg.lower():
            raise HTTPException(
                status_code=429,
                detail="Rate limit reached for free data API, try again later",
            )
        raise HTTPException(status_code=404, detail=f"No price data: {msg}")

    values = data.get("values")
    if not values:
        raise HTTPException(status_code=404, detail="No price data found for symbol")

    rows: List[Dict] = []
    for bar in values:
        try:
            date_str = bar["datetime"]
            close = float(bar["close"])
        except (KeyError, ValueError):
            continue
        rows.append({"date": date_str, "close": close})

    rows.sort(key=lambda x: x["date"])

    # cache for 5 minutes
    _price_cache[symbol] = (now + timedelta(minutes=5), rows)
    return rows

def compute_return(history: List[Dict], days: int):
    if len(history) < days + 1:
        return None
    start = history[-(days + 1)]["close"]
    end = history[-1]["close"]
    return (end - start) / start * 100.0

def compute_performance(history: List[Dict]):
    horizons = {
        "1D": 1,
        "1W": 5,
        "1M": 21,
        "1Y": 252,
        "3Y": 252 * 3,
        "5Y": 252 * 5,
        "10Y": 252 * 10,
    }
    perf: Dict[str, Optional[float]] = {}
    for label, days in horizons.items():
        perf[label] = compute_return(history, days)
    return perf

def _fii_dii_placeholder():
    return {
        "date": datetime.utcnow().date().isoformat(),
        "fii": {
            "buy": 24204.09,
            "sell": 17188.00,
            "net": -4685.15,
        },
        "dii": {
            "buy": 16027.33,
            "sell": 9776.88,
            "net": 6250.45,
        },
        "currency": "INR_CR",
        "source": "placeholder",
    }

async def _fetch_fii_dii_live_impl():
    try:
        import asyncio
        from nsepython import nse_fiidii
        import pandas as pd
        df = await asyncio.to_thread(nse_fiidii)
        if df is not None and not df.empty:
            fii_row, dii_row, parsed_date = None, None, datetime.utcnow().date().isoformat()
            for _, row in df.iterrows():
                cat = str(row.get('category', '')).upper()
                if not parsed_date or row.get('date'):
                    try:
                        parsed_date = datetime.strptime(str(row.get('date')).strip(), "%d-%b-%Y").strftime("%Y-%m-%d")
                    except Exception:
                        pass
                
                data_dict = {
                    "buy": float(row.get('buyValue', 0)),
                    "sell": float(row.get('sellValue', 0)),
                    "net": float(row.get('netValue', 0)),
                }
                if 'DII' in cat:
                    dii_row = data_dict
                elif 'FII' in cat or 'FPI' in cat:
                    fii_row = data_dict
            
            if fii_row and dii_row:
                return {
                    "date": parsed_date,
                    "fii": fii_row,
                    "dii": dii_row,
                    "currency": "INR_CR",
                    "source": "nsepython"
                }
    except Exception as exc:
        logger.warning(f"nsepython FII/DII failed: {exc}. Trying fallback...")

    url = "https://www.nseindia.com/reports/fii-dii"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.nseindia.com/",
    }
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            return _fii_dii_placeholder()

        html = resp.text
        row_pattern = re.compile(
            r'<tr[^>]*>\s*<td[^>]*>\s*([^<]+)\s*</td>\s*'
            r'<td[^>]*>\s*([\d]{1,2}-[A-Za-z]+-[\d]{4})\s*</td>\s*'
            r'<td[^>]*>\s*([\d,\.\-]+)\s*</td>\s*'
            r'<td[^>]*>\s*([\d,\.\-]+)\s*</td>\s*'
            r'<td[^>]*>\s*([\-\d,\.]+)\s*</td>',
            re.IGNORECASE | re.DOTALL,
        )

        def _num(s: str) -> float:
            return float(s.replace(",", ""))

        def _parse_date(d: str) -> str:
            return datetime.strptime(d.strip(), "%d-%b-%Y").strftime("%Y-%m-%d")

        fii_row, dii_row, parsed_date = None, None, datetime.utcnow().date().isoformat()
        for match in row_pattern.finditer(html):
            category = match.group(1).strip().upper()
            date_raw = match.group(2).strip()
            buy, sell, net = _num(match.group(3)), _num(match.group(4)), _num(match.group(5))
            try:
                row_date = _parse_date(date_raw)
            except ValueError:
                row_date = datetime.utcnow().date().isoformat()

            if "DII" in category and dii_row is None:
                dii_row, parsed_date = {"buy": buy, "sell": sell, "net": net}, row_date
            elif ("FII" in category or "FPI" in category) and fii_row is None:
                fii_row, parsed_date = {"buy": buy, "sell": sell, "net": net}, row_date

            if fii_row and dii_row: break

        if not fii_row or not dii_row: return _fii_dii_placeholder()
        return {"date": parsed_date, "fii": fii_row, "dii": dii_row, "currency": "INR_CR", "source": "nse-scrape"}
    except Exception:
        return _fii_dii_placeholder()

_fii_dii_cache = {"data": None, "expires_at": None}

async def fetch_fii_dii_live():
    global _fii_dii_cache
    now = datetime.utcnow()
    if _fii_dii_cache["data"] and _fii_dii_cache["expires_at"] > now:
        return _fii_dii_cache["data"]
    data = await _fetch_fii_dii_live_impl()
    _fii_dii_cache["data"], _fii_dii_cache["expires_at"] = data, now + timedelta(minutes=30)
    return data
