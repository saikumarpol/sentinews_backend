# backend/routers/stock_detail.py
#
# Mount in app.py:
#   from routers import stock_detail
#   app.include_router(stock_detail.router)

import yfinance as yf
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import pandas as pd
from datetime import datetime, timezone, timedelta

router = APIRouter(tags=["stock"])

# ── Helpers ──────────────────────────────────────────────────────────────────

RANGE_MAP = {
    "1W": ("7d",  "1d"),
    "1M": ("1mo", "1d"),
    "3M": ("3mo", "1d"),
    "1Y": ("1y",  "1d"),
    "5Y": ("5y",  "1wk"),
}


def safe(val):
    """Convert numpy/nan values to Python native or None."""
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return float(val)
    except Exception:
        return None


def resolve_symbol(symbol: str) -> str:
    """
    Auto-suffix Indian stocks if missing .NS, but only if the original symbol
    doesn't return any data (preserving US tickers like AAPL, MSFT).
    """
    if "." in symbol:
        return symbol
    if symbol.isupper() and len(symbol) <= 12:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d")
            if not hist.empty:
                return symbol
        except Exception:
            pass
        return f"{symbol}.NS"
    return symbol


def compute_rsi(series: pd.Series, period: int = 14) -> Optional[float]:
    delta = series.diff().dropna()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, float("nan"))
    rsi   = 100 - (100 / (1 + rs))
    return safe(rsi.iloc[-1]) if len(rsi) > 0 else None


def compute_macd(series: pd.Series):
    ema12 = series.ewm(span=12, adjust=False).mean()
    ema26 = series.ewm(span=26, adjust=False).mean()
    macd_line   = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram   = macd_line - signal_line
    return {
        "value":     safe(macd_line.iloc[-1]),
        "signal":    safe(signal_line.iloc[-1]),
        "histogram": safe(histogram.iloc[-1]),
    }


def performance_from_history(hist: pd.DataFrame) -> dict:
    """Compute returns for standard horizons from a daily price DataFrame."""
    if hist.empty:
        return {}
    close = hist["Close"]
    last  = float(close.iloc[-1])
    perf  = {}
    horizons = {
        "1D": 1, "1W": 5, "1M": 21, "3M": 63,
        "1Y": 252, "3Y": 756, "5Y": 1260,
    }
    for label, days in horizons.items():
        if len(close) > days:
            past = float(close.iloc[-(days + 1)])
            perf[label] = safe(((last - past) / past) * 100) if past else None
        else:
            perf[label] = None
    return perf


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/stock/{symbol}/detail")
def get_stock_detail(symbol: str):
    """
    Full snapshot + 1Y history + technicals + performance for a symbol.
    """
    symbol = resolve_symbol(symbol)
    ticker = yf.Ticker(symbol)

    try:
        info = ticker.info or {}
    except Exception:
        info = {}

    # 1-year daily history for chart and indicator calculation
    try:
        hist_1y = ticker.history(period="1y", interval="1d")
    except Exception:
        hist_1y = pd.DataFrame()

    if hist_1y.empty:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol}")

    close = hist_1y["Close"]
    last_price = safe(close.iloc[-1])
    prev_price = safe(close.iloc[-2]) if len(close) > 1 else last_price
    change_1d  = safe(last_price - prev_price) if last_price and prev_price else None
    change_1d_pct = safe(((last_price - prev_price) / prev_price) * 100) if prev_price else None

    # Technicals (calculated from 1Y history)
    sma20 = safe(close.rolling(20).mean().iloc[-1])
    ma50  = safe(close.rolling(50).mean().iloc[-1])
    ma200 = safe(close.rolling(200).mean().iloc[-1])
    rsi14 = compute_rsi(close)
    macd  = compute_macd(close)

    # Performance
    performance = performance_from_history(hist_1y)

    # History rows for chart (full 1Y)
    history_rows = []
    for idx, row in hist_1y.iterrows():
        history_rows.append({
            "date":   str(idx.date()),
            "open":   safe(row["Open"]),
            "high":   safe(row["High"]),
            "low":    safe(row["Low"]),
            "close":  safe(row["Close"]),
            "volume": int(row["Volume"]) if row["Volume"] else 0,
        })

    return {
        "symbol":   symbol,
        "name":     info.get("longName") or info.get("shortName") or symbol,
        "exchange": info.get("exchange") or "NSE",
        "sector":   info.get("sector"),
        "industry": info.get("industry"),
        "snapshot": {
            "last_price":     last_price,
            "change_1d":      change_1d,
            "change_1d_pct":  change_1d_pct,
            "volume":         int(hist_1y["Volume"].iloc[-1]) if "Volume" in hist_1y.columns else None,
            "market_cap":     safe(info.get("marketCap")),
            "pe_ratio":       safe(info.get("trailingPE") or info.get("forwardPE")),
            "pb_ratio":       safe(info.get("priceToBook")),
            "roe":            safe(info.get("returnOnEquity") * 100 if info.get("returnOnEquity") else None),
            "debt_to_equity": safe(info.get("debtToEquity")),
            "eps":            safe(info.get("trailingEps")),
            "dividend_yield": safe((info.get("dividendYield") or 0) * 100),
            "high_52w":       safe(info.get("fiftyTwoWeekHigh")),
            "low_52w":        safe(info.get("fiftyTwoWeekLow")),
        },
        "technicals": {
            "rsi14": rsi14,
            "macd":  macd,
            "sma20": sma20,
            "ma50":  ma50,
            "ma200": ma200,
        },
        "performance": performance,
        "history":     history_rows,
        "earnings":    info.get("earnings", {}),
        "calendar":    ticker.calendar if hasattr(ticker, "calendar") else {},
        "last_date":   str(hist_1y.index[-1].date()),
    }


@router.get("/stock/{symbol}/history")
def get_stock_history(symbol: str, range: str = Query("1Y")):
    """Return OHLCV history for a specific time range."""
    symbol = resolve_symbol(symbol)
    period, interval = RANGE_MAP.get(range, ("1y", "1d"))
    ticker = yf.Ticker(symbol)
    try:
        hist = ticker.history(period=period, interval=interval)
    except Exception:
        raise HTTPException(status_code=404, detail="History not available")

    if hist.empty:
        return {"history": []}

    return {
        "history": [
            {
                "date":   str(idx.date()),
                "close":  safe(row["Close"]),
                "open":   safe(row["Open"]),
                "high":   safe(row["High"]),
                "low":    safe(row["Low"]),
                "volume": int(row["Volume"]) if row["Volume"] else 0,
            }
            for idx, row in hist.iterrows()
        ]
    }


def format_to_ist(iso_utc_str: str) -> str:
    if not iso_utc_str:
        return ""
    try:
        s = iso_utc_str
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        dt_ist = dt.astimezone(timezone(timedelta(hours=5, minutes=30)))
        return dt_ist.strftime("%d %b, %I:%M %p IST")
    except Exception:
        try:
            t_parts = iso_utc_str.split("T")
            date_part = t_parts[0]
            time_part = t_parts[1].split(".")[0].split("+")[0].split("-")[0].split("Z")[0]
            dt = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S")
            dt_ist = dt.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=5, minutes=30)))
            return dt_ist.strftime("%d %b, %I:%M %p IST")
        except Exception:
            return iso_utc_str


@router.get("/stock/{symbol}/news")
def get_stock_news(symbol: str):
    """
    Return news for a specific symbol using yfinance.
    For richer results with sentiment, swap for your real news service.
    """
    symbol = resolve_symbol(symbol)
    ticker = yf.Ticker(symbol)
    try:
        raw = ticker.news or []
    except Exception:
        raw = []

    news = []
    for item in raw[:20]:
        published_ts = item.get("providerPublishTime", 0)
        published_at = (
            datetime.fromtimestamp(published_ts).isoformat()
            if published_ts else None
        )
        published_at_ist = format_to_ist(published_at) if published_at else None
        news.append({
            "id":           item.get("uuid", ""),
            "headline":     item.get("title", ""),
            "summary":      item.get("summary", ""),
            "source":       item.get("publisher", ""),
            "url":          item.get("link", ""),
            "published_at": published_at,
            "published_at_ist": published_at_ist,
            "sentiment":    0.0,   # plug in your sentiment model here
            "action":       "hold",
            "tickers":      item.get("relatedTickers", [symbol]),
        })

    return {"news": news}
