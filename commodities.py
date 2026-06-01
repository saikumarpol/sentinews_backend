# backend/commodities.py
#
# Market data for the Commodities Dashboard.
# Primary source: yfinance (free, no rate limits, batch download)
# Secondary source: Twelve Data quote API (used only if yfinance returns None)
#
# yfinance symbols:
#   Futures: CL=F (crude), GC=F (gold), SI=F (silver), NG=F (nat gas) etc.
#   Forex:   USDINR=X, EURUSD=X etc.
#   Indices: ^NSEI, ^BSESN, ^GSPC etc.

import os
import math
import logging
import asyncio
import httpx
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import yfinance as yf
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("sentinews.commodities")

TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")

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


# ── Symbol definitions ─────────────────────────────────────────────────────
# Each entry: yf_symbol (used for actual fetching), display name, category, unit

COMMODITY_DEFS = [
    # Energy
    {"yf": "CL=F",   "td": "CRUDEOIL",     "name": "Crude Oil (WTI)", "category": "Energy",       "unit": "USD/Bbl"},
    {"yf": "BZ=F",   "td": "BRENT",         "name": "Brent Crude",     "category": "Energy",       "unit": "USD/Bbl"},
    {"yf": "NG=F",   "td": "NG",            "name": "Natural Gas",     "category": "Energy",       "unit": "USD/MMBtu"},
    {"yf": "RB=F",   "td": "GASOLINE",      "name": "Gasoline RBOB",   "category": "Energy",       "unit": "USD/Gal"},
    {"yf": "HO=F",   "td": "HEATING-OIL",   "name": "Heating Oil",    "category": "Energy",       "unit": "USD/Gal"},
    # Metals
    {"yf": "GC=F",   "td": "XAU",           "name": "Gold",            "category": "Metals",       "unit": "USD/oz"},
    {"yf": "SI=F",   "td": "XAG",           "name": "Silver",          "category": "Metals",       "unit": "USD/oz"},
    {"yf": "PL=F",   "td": "XPT",           "name": "Platinum",        "category": "Metals",       "unit": "USD/oz"},
    {"yf": "PA=F",   "td": "XPD",           "name": "Palladium",       "category": "Metals",       "unit": "USD/oz"},
    {"yf": "HG=F",   "td": "HG",            "name": "Copper",          "category": "Metals",       "unit": "USD/lb"},
    {"yf": "ALI=F",  "td": "ALU",           "name": "Aluminium",       "category": "Metals",       "unit": "USD/T"},
    {"yf": "ZN=F",   "td": "ZINC",          "name": "Zinc",            "category": "Metals",       "unit": "USD/T"},
    # Agriculture
    {"yf": "ZW=F",   "td": "WHEAT",         "name": "Wheat",           "category": "Agriculture",  "unit": "USD/Bu"},
    {"yf": "ZC=F",   "td": "CORN",          "name": "Corn",            "category": "Agriculture",  "unit": "USD/Bu"},
    {"yf": "ZS=F",   "td": "SOYBEAN",       "name": "Soybean",         "category": "Agriculture",  "unit": "USD/Bu"},
    {"yf": "SB=F",   "td": "SUGAR",         "name": "Sugar",           "category": "Agriculture",  "unit": "USD/lb"},
    {"yf": "KC=F",   "td": "COFFEE",        "name": "Coffee",          "category": "Agriculture",  "unit": "USD/lb"},
    {"yf": "CT=F",   "td": "COTTON",        "name": "Cotton",          "category": "Agriculture",  "unit": "USD/lb"},
    {"yf": "CC=F",   "td": "COCOA",         "name": "Cocoa",           "category": "Agriculture",  "unit": "USD/T"},
]

CURRENCY_DEFS = [
    {"yf": "USDINR=X",  "name": "USD/INR"},
    {"yf": "EURUSD=X",  "name": "EUR/USD"},
    {"yf": "GBPUSD=X",  "name": "GBP/USD"},
    {"yf": "USDJPY=X",  "name": "USD/JPY"},
    {"yf": "EURINR=X",  "name": "EUR/INR"},
    {"yf": "GBPINR=X",  "name": "GBP/INR"},
    {"yf": "AUDUSD=X",  "name": "AUD/USD"},
    {"yf": "USDCAD=X",  "name": "USD/CAD"},
    {"yf": "USDCHF=X",  "name": "USD/CHF"},
    {"yf": "NZDUSD=X",  "name": "NZD/USD"},
    {"yf": "GBPEUR=X",  "name": "GBP/EUR"},
    {"yf": "EURCHF=X",  "name": "EUR/CHF"},
]

INDEX_DEFS = [
    # Indian
    {"yf": "^NSEI",     "name": "Nifty 50",       "td": "NIFTY"},
    {"yf": "^BSESN",    "name": "Sensex",         "td": "SENSEX"},
    {"yf": "^NSEBANK",  "name": "Nifty Bank",     "td": "BANKNIFTY"},
    {"yf": "NIFTY_F1.NS","name": "GIFT Nifty",     "td": "NIFTY"},
    # US
    {"yf": "^GSPC",     "name": "S&P 500",        "td": "SPX"},
    {"yf": "^DJI",      "name": "Dow Jones",      "td": "DJI"},
    {"yf": "^IXIC",     "name": "Nasdaq Comp",    "td": "IXIC"},
    {"yf": "^NDX",      "name": "Nasdaq 100",     "td": "NDX"},
    # Europe
    {"yf": "^FTSE",     "name": "FTSE 100",       "td": "FTSE"},
    {"yf": "^GDAXI",    "name": "DAX 40",         "td": "GDAXI"},
    {"yf": "^FCHI",     "name": "CAC 40",         "td": "FCHI"},
    {"yf": "^IBEX",     "name": "IBEX 35",        "td": "IBEX"},
    # Asia
    {"yf": "^N225",     "name": "Nikkei 225",     "td": "NI225"},
    {"yf": "^HSI",      "name": "Hang Seng",      "td": "HSI"},
    {"yf": "^FTSEA50",  "name": "China A50",      "td": "FTSEA50"},
    # Global
    {"yf": "URTH",      "name": "MSCI World",     "td": "URTH"},
    {"yf": "^VIX",      "name": "India VIX",      "td": "VIX"},
]

CURRENCY_DEFS = [
    {"yf": "USDINR=X", "name": "USD/INR", "td": "USDINR"},
    {"yf": "EURUSD=X", "name": "EUR/USD", "td": "EURUSD"},
    {"yf": "USDJPY=X", "name": "USD/JPY", "td": "USDJPY"},
    {"yf": "GBPUSD=X", "name": "GBP/USD", "td": "GBPUSD"},
    {"yf": "USDCHF=X", "name": "USD/CHF", "td": "USDCHF"},
    {"yf": "AUDUSD=X", "name": "AUD/USD", "td": "AUDUSD"},
    {"yf": "USDCAD=X", "name": "USD/CAD", "td": "USDCAD"},
    {"yf": "NZDUSD=X", "name": "NZD/USD", "td": "NZDUSD"},
    {"yf": "GBPEUR=X", "name": "GBP/EUR", "td": "GBPEUR"},
    {"yf": "EURCHF=X", "name": "EUR/CHF", "td": "EURCHF"},
    {"yf": "EURINR=X", "name": "EUR/INR", "td": "EURINR"},
    {"yf": "GBPINR=X", "name": "GBP/INR", "td": "GBPINR"},
]

# --- FUTURES DEFINITIONS ---
COMMODITY_FUT_DEFS = COMMODITY_DEFS # Most commodities already use futures contracts

INDEX_FUT_DEFS = [
    {"yf": "NIFTY_F1.NS",    "name": "Nifty Futures",   "category": "Indian", "td": "NIFTY"},
    {"yf": "BANKNIFTY_F1.NS", "name": "Bank Nifty Fut", "category": "Indian", "td": "BANKNIFTY"},
    {"yf": "ES=F",           "name": "S&P 500 Fut",      "category": "US",     "td": "SPX"},
    {"yf": "NQ=F",           "name": "Nasdaq 100 Fut",   "category": "US",     "td": "NDX"},
    {"yf": "YM=F",           "name": "Dow Future",       "category": "US",     "td": "DJI"},
    {"yf": "RTY=F",          "name": "Russell 2000 Fut", "category": "US",     "td": "RTY"},
]

CURRENCY_FUT_DEFS = [
    {"yf": "USDINR=F", "name": "USD/INR Fut", "category": "Major", "td": "USDINR"},
    {"yf": "EURUSD=F", "name": "EUR/USD Fut", "category": "Major", "td": "EURUSD"},
    {"yf": "GBPUSD=F", "name": "GBP/USD Fut", "category": "Major", "td": "GBPUSD"},
    {"yf": "JPY=F",    "name": "JPY Future",   "category": "Major", "td": "USDJPY"},
    {"yf": "AUD=F",    "name": "AUD Future",   "category": "Major", "td": "AUDUSD"},
    {"yf": "CAD=F",    "name": "CAD Future",   "category": "Major", "td": "USDCAD"},
]


# ── Helpers ─────────────────────────────────────────────────────────────────

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
    """Fetch price history for a single symbol. Returns close Series or None."""
    try:
        # Use to_thread since yfinance history() is blocking I/O
        def _fetch():
            ticker = yf.Ticker(symbol)
            return ticker.history(period=period, interval="1d", auto_adjust=True)
        
        h = await asyncio.to_thread(_fetch)
        if h.empty:
            return None
        return h["Close"].dropna()
    except Exception as exc:
        logger.debug("yf history failed for %s: %s", symbol, exc)
        return None


def _extract_data_from_batch(df, symbol: str) -> Dict[str, Optional[Any]]:
    empty = {"close": None, "high": None, "low": None}
    if df is None or df.empty:
        return empty
    try:
        if isinstance(df.columns, pd.MultiIndex):
            close_col = "Close" if "Close" in df.columns.levels[0] else "close"
            high_col = "High" if "High" in df.columns.levels[0] else "high"
            low_col = "Low" if "Low" in df.columns.levels[0] else "low"
            
            close_s = df[close_col][symbol].dropna() if symbol in df[close_col].columns else None
            high_s = df[high_col][symbol].dropna() if symbol in df[high_col].columns else None
            low_s = df[low_col][symbol].dropna() if symbol in df[low_col].columns else None
            
            return {"close": close_s, "high": high_s, "low": low_s}
        else:
            close_col = "Close" if "Close" in df.columns else ("close" if "close" in df.columns else None)
            high_col = "High" if "High" in df.columns else ("high" if "high" in df.columns else None)
            low_col = "Low" if "Low" in df.columns else ("low" if "low" in df.columns else None)
            
            return {
                "close": df[close_col].dropna() if close_col else None,
                "high": df[high_col].dropna() if high_col else None,
                "low": df[low_col].dropna() if low_col else None,
            }
    except Exception as e:
        logger.debug("Failed to extract data for %s: %s", symbol, e)
    return empty


def _quote_from_data(data: Dict) -> Dict:
    close = data.get("close")
    high = data.get("high")
    low = data.get("low")
    
    empty = {"price": None, "day_change": None, "day_pct": None,
             "weekly_pct": None, "monthly_pct": None, "high_52w": None, "low_52w": None,
             "daily_high": None, "daily_low": None}
             
    if close is None or len(close) < 2:
        return empty
        
    curr  = _safe(close.iloc[-1])
    prev  = _safe(close.iloc[-2])
    w_ago = _safe(close.iloc[-6])  if len(close) >= 6  else None
    m_ago = _safe(close.iloc[-22]) if len(close) >= 22 else None
    
    daily_high = _safe(high.iloc[-1]) if high is not None and len(high) > 0 else curr
    daily_low = _safe(low.iloc[-1]) if low is not None and len(low) > 0 else curr
    
    return {
        "price":       curr,
        "day_change":  _safe(curr - prev) if curr and prev else None,
        "day_pct":     _pct(curr, prev),
        "weekly_pct":  _pct(curr, w_ago),
        "monthly_pct": _pct(curr, m_ago),
        "high_52w":    _safe(float(close.max())),
        "low_52w":     _safe(float(close.min())),
        "daily_high":  daily_high,
        "daily_low":   daily_low,
    }



# ── Twelve Data fallback (single symbol) ──────────────────────────────────

async def _td_quote(td_symbol: str) -> Optional[float]:
    """Fetch single price from Twelve Data. Used only as a fallback."""
    if not TWELVEDATA_API_KEY or not td_symbol:
        return None
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.twelvedata.com/price",
                params={"symbol": td_symbol, "apikey": TWELVEDATA_API_KEY},
                timeout=8,
            )
            d = resp.json()
            if d.get("status") == "error":
                return None
            return _safe(d.get("price"))
    except Exception:
        return None


# ── Main build functions ──────────────────────────────────────────────────

async def _fetch_yf_batch(tickers: List[str], period: str = "3mo") -> Optional[Any]:
    """Fetch price history for multiple symbols in a single batch. Returns DataFrame or None."""
    try:
        def _fetch():
            return yf.download(tickers, period=period, interval="1d", auto_adjust=True, progress=False, threads=False)
        
        df = await asyncio.to_thread(_fetch)
        if df.empty:
            return None
        return df
    except Exception as exc:
        logger.error("yf batch download failed: %s", exc)
        return None


async def _build_single_item(d: Dict, data: Dict, category_default: str, now: str) -> Dict:
    sym = d["yf"]
    q = _quote_from_data(data)

    # Twelve Data fallback if yfinance price is missing or zero
    price = q.get("price")
    if (price is None or price <= 0) and d.get("td"):
        new_price = await _td_quote(d["td"])
        if new_price:
            q["price"] = new_price
            price = new_price
            logger.info("TD fallback for %s: %s", sym, price)

    # Simple Sentiment integration based on Name
    # 1. News-based sentiment
    sentiment_score = 0.0
    try:
        from news_scraper import _NEWS_CACHE
        from ai_processor import basic_sentiment_from_headline, _action_from_sentiment
        
        top_articles = _NEWS_CACHE.get("articles", [])[:100]
        asset_name = d["name"].lower()
        asset_scores = []
        for art in top_articles:
            headline = art.get("headline", "").lower()
            if asset_name in headline:
                asset_scores.append(basic_sentiment_from_headline(headline))
        
        if asset_scores:
            sentiment_score = sum(asset_scores) / len(asset_scores)
    except Exception:
        pass

    # 2. Technical-based sentiment (Position in 52-week range)
    tech_score = 0.5
    if q.get("high_52w") and q.get("low_52w") and q["high_52w"] > q["low_52w"]:
        p = price or 0.0
        tech_score = (p - q["low_52w"]) / (q["high_52w"] - q["low_52w"])
        tech_score = max(0, min(1, tech_score))
    
    # 3. Final Blend: prioritize news if significant, else tech
    # If news is Exactly 0.5 (Neutral), lean on technical score for more variance
    if sentiment_score == 0.0:
        final_sentiment = tech_score
    else:
        # Weight news 60%, technical 40%
        final_sentiment = (sentiment_score * 0.6) + (tech_score * 0.4)

    action = "NEUTRAL"
    if final_sentiment > 0.65: action = "BULLISH"
    elif final_sentiment < 0.35: action = "BEARISH"

    return {
        "name":        d["name"],
        "symbol":      sym,
        "td_symbol":   d.get("td", ""),
        "category":    d.get("category", category_default),
        "unit":        d.get("unit", ""),
        "price":       price or 0.0,
        "day_change":  q.get("day_change") or 0.0,
        "day_pct":     q.get("day_pct") or 0.0,
        "sentiment":   round(float(final_sentiment), 2),
        "action":      action,
        "weekly_pct":  q.get("weekly_pct"),
        "monthly_pct": q.get("monthly_pct"),
        "high_52w":    q.get("high_52w"),
        "low_52w":     q.get("low_52w"),
        "daily_high":  q.get("daily_high"),
        "daily_low":   q.get("daily_low"),
        "last_update": now,
        "source":      "yfinance" if (price and price != 0.0) else "unavailable",
    }

async def _build_items_async(defs: List[Dict], df: Any, category_default: str) -> List[Dict]:
    """Build result dicts using pre-fetched batch DataFrame."""
    now = datetime.now(timezone.utc).isoformat()
    tasks = [_build_single_item(d, _extract_data_from_batch(df, d["yf"]), category_default, now) for d in defs]
    return await asyncio.gather(*tasks)


async def fetch_market_dashboard(is_futures: bool = False, force_refresh: bool = False) -> Dict[str, Any]:
    """
    Fetch all commodities, currencies, and indices in parallel.
    is_futures: if True, returns futures data instead of spot/default.
    """
    cache_key = "dashboard_futures" if is_futures else "dashboard"
    if not force_refresh:
        cached = _get_cached_data(cache_key, 600)
        if cached:
            return cached

    comm_defs = COMMODITY_FUT_DEFS if is_futures else COMMODITY_DEFS
    curr_defs = CURRENCY_FUT_DEFS if is_futures else CURRENCY_DEFS
    indx_defs = INDEX_FUT_DEFS if is_futures else INDEX_DEFS

    logger.info("Fetching %s dashboard: %d commodities, %d currencies, %d indices",
                "futures" if is_futures else "market",
                len(comm_defs), len(curr_defs), len(indx_defs))

    # Collect all definitions and unique tickers
    all_defs = comm_defs + curr_defs + indx_defs
    all_tickers = list(set(d["yf"] for d in all_defs))

    # Batch download yfinance data
    df = await _fetch_yf_batch(all_tickers, period="3mo")

    c_task = _build_items_async(comm_defs, df, "Commodity")
    k_task = _build_items_async(curr_defs, df, "Currencies")
    i_task = _build_items_async(indx_defs, df, "Indices")

    commodities_res, currencies_res, indices_res = await asyncio.gather(c_task, k_task, i_task)

    filled_comm = sum(1 for c in commodities_res if c["price"] != 0.0)
    filled_curr = sum(1 for c in currencies_res if c["price"] != 0.0)
    filled_idx  = sum(1 for c in indices_res    if c["price"] != 0.0)
    
    logger.info("Dashboard filled: %d/%d comm, %d/%d curr, %d/%d idx",
                filled_comm, len(COMMODITY_DEFS),
                filled_curr, len(CURRENCY_DEFS),
                filled_idx,  len(INDEX_DEFS))

    res = {
        "commodities": commodities_res,
        "currencies":  currencies_res,
        "indices":     indices_res,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "is_futures":  is_futures,
    }
    _set_cached_data(cache_key, res, 600)
    return res


# ── Legacy compatibility ──────────────────────────────────────────────────

async def fetch_all_commodities() -> List[Dict[str, Any]]:
    dash = await fetch_market_dashboard()
    return dash["commodities"]

async def fetch_all_currencies() -> List[Dict[str, Any]]:
    dash = await fetch_market_dashboard()
    return dash["currencies"]

async def fetch_all_indices() -> List[Dict[str, Any]]:
    dash = await fetch_market_dashboard()
    return dash["indices"]

