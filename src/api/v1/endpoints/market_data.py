# backend/routers/indian_market.py
#
# PURPOSE: Live Indian market data using nsepython (scrapes NSE India directly).
# This is ADDITIVE — does NOT touch Twelve Data or any existing endpoints.
#
# Mount in app.py (already done):
#   from routers import indian_market
#   app.include_router(indian_market.router)
#
# Data priority chain:
#   nsepython (near real-time NSE) → yfinance (.NS fallback)

import logging
import math
import os
import httpx
import json
import asyncio
from datetime import datetime, time, timedelta
from typing import Optional, List, Dict, Any

import yfinance as yf
from src.core.config import settings
from src.services.news_service import get_market_feed_async

router = APIRouter(prefix="/market", tags=["indian-market"])


# ── yfinance ticker equivalents for index fallback ─────────────────────────

YFINANCE_INDEX_MAP = {
    "NIFTY 50":         "^NSEI",
    "NIFTY BANK":       "^NSEBANK",
    "NIFTY IT":         "^CNXIT",
    "INDIA VIX":        "^INDIAVIX",
    "SENSEX":           "^BSESN",
    "NIFTY NEXT 50":    "NIFTY_NEXT_50.NS",
    "NIFTY MID 100":    "^NSEMDCP100",
    "NIFTY SML 100":    "^NSESMLCP100",
    "NIFTY AUTO":       "^CNXAUTO",
    "NIFTY FMCG":       "^CNXFMCG",
    "NIFTY PHARMA":     "^CNXPHARMA",
    "NIFTY METAL":      "^CNXMETAL",
}

# nsepython symbol names for nse_quote_ltp
NSE_INDEX_SYMBOLS = {
    "NIFTY 50":         "NIFTY 50",
    "NIFTY BANK":       "NIFTY BANK",
    "NIFTY IT":         "NIFTY IT",
    "INDIA VIX":        "INDIA VIX",
    "NIFTY NEXT 50":    "NIFTY NEXT 50",
    "NIFTY MID 100":    "NIFTY MIDCAP 100",
    "NIFTY SML 100":    "NIFTY SMALLCAP 100",
}


# ── Helpers ────────────────────────────────────────────────────────────────

def _safe_float(val) -> Optional[float]:
    """Convert any value to float, returning None for NaN/Inf/None."""
    try:
        if val is None:
            return None
        f = float(val)
        return None if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return None


def _yfinance_quote(ticker_symbol: str) -> Optional[Dict]:
    """Fetch a quick real-time quote from yfinance for a single symbol."""
    try:
        ticker = yf.Ticker(ticker_symbol)
        price = prev = None
        
        # Try fast_info first (most efficient)
        try:
            info = ticker.fast_info
            price = _safe_float(getattr(info, "last_price", None))
            prev  = _safe_float(getattr(info, "previous_close", None))
        except:
            pass

        # If fast_info failed (common yf bug) or returned None, use history
        if price is None:
            # period='1d' or '2d' usually bypasses metadata errors like 'currentTradingPeriod'
            hist = ticker.history(period="2d", interval="1d")
            if not hist.empty:
                price = _safe_float(hist["Close"].iloc[-1])
                # Previous close is the close of the day before ONLY if history has 2 days
                if len(hist) > 1:
                    prev = _safe_float(hist["Close"].iloc[-2])
                else:
                    # If only 1 day, we don't have a change baseline from history
                    prev = price
        
        if price is None:
            return None

        change     = _safe_float(price - prev) if prev is not None else None
        change_pct = _safe_float((price - prev) / prev * 100) if prev and prev != 0 else None

        return {
            "last_price": price,
            "prev_close": prev,
            "change":     change,
            "change_pct": change_pct,
        }
    except Exception as exc:
        logger.warning("yfinance quote failed for %s: %s", ticker_symbol, exc)
        return None

def _get_indian_stock_price(sym: str) -> Optional[Dict]:
    """Multi-source fetcher for Indian stocks. Priority: NSE -> Yahoo -> TwelveData."""
    # 1. NSE Python (Real-time during hours)
    try:
        from nsepython import nse_quote_ltp
        ltp = nse_quote_ltp(sym, "LTP")
        val = _safe_float(ltp) if str(ltp) not in ("-", "", "None") else None
        if val:
            # For change/prev close, we'd need another call, but LTP is primary
            return {"last_price": val, "source": "nsepython"}
    except:
        pass

    # 2. Yahoo Finance
    q = _yfinance_quote(f"{sym}.NS")
    if q and q.get("last_price"):
        q["source"] = "yfinance"
        return q

    # 3. TwelveData
    q = _twelvedata_quote(f"{sym}.NS")
    if q and q.get("last_price"):
        q["source"] = "twelvedata"
        return q

    return None

def _twelvedata_quote(symbol: str) -> Optional[Dict]:
    """Fallback fetch from TwelveData /quote API."""
    td_key = settings.TWELVEDATA_API_KEY
    if not td_key:
        return None
        
    # TwelveData standard symbols: "^NSEI" -> "NIFTY", etc.
    # For Indian stocks, they use format like "RELIANCE:NSE" instead of "RELIANCE.NS"
    td_sym = symbol.replace(".NS", ":NSE").replace(".BO", ":BSE")
    if symbol == "^NSEI": td_sym = "NIFTY:NSE"
    elif symbol == "^BSESN": td_sym = "SENSEX:BSE"
    elif symbol == "^NSEBANK": td_sym = "BANKNIFTY:NSE"
    elif symbol == "^INDIAVIX": td_sym = "VIX:NSE"
    elif symbol == "^CNXIT": td_sym = "NIFTYIT:NSE"
        
    try:
        url = "https://api.twelvedata.com/quote"
        resp = httpx.get(url, params={"symbol": td_sym, "apikey": td_key}, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if "close" in data:
                price = _safe_float(data.get("close"))
                prev  = _safe_float(data.get("previous_close"))
                chg   = _safe_float(data.get("change"))
                pct   = _safe_float(data.get("percent_change"))
                if price:
                    return {
                        "last_price": price,
                        "prev_close": prev,
                        "change":     chg,
                        "change_pct": pct,
                    }
    except Exception as exc:
        logger.debug("twelvedata quote failed for %s: %s", td_sym, exc)
    return None


def _df_to_records(df, limit: int = 10) -> List[Dict]:
    """Convert a pandas DataFrame to a list of plain dicts (JSON-serializable)."""
    records = []
    for _, row in df.head(limit).iterrows():
        records.append({
            "symbol":     str(row.get("symbol", "")),
            "name":       str(row.get("companyName", row.get("symbol", ""))),
            "last_price": _safe_float(row.get("lastPrice")),
            "change":     _safe_float(row.get("change")),
            "change_pct": _safe_float(row.get("pChange")),
            "volume":     int(row["totalTradedVolume"]) if "totalTradedVolume" in row and row["totalTradedVolume"] else None,
            "source":     "nsepython",
        })
    return records


# --- TTL Caching ---
__cache: Dict[str, Dict] = {}

def _get_cached(key: str, ttl_seconds: int):
    global __cache
    now = datetime.utcnow()
    entry = __cache.get(key)
    if entry and entry["expires_at"] > now:
        return entry["data"]
    return None

def _set_cached(key: str, data: Any, ttl_seconds: int):
    global __cache
    __cache[key] = {
        "data": data,
        "expires_at": datetime.utcnow() + timedelta(seconds=ttl_seconds),
    }

# ── Endpoints ──────────────────────────────────────────────────────────────


@router.get("/indices", summary="Live NSE/BSE indices")
async def get_live_indices():
    """
    Returns live prices for major Indian indices in parallel.
    Source: nsepython (nse_quote_ltp) → yfinance fallback.
    """
    cached = _get_cached("indices", 60)
    if cached: return cached

    import asyncio

    async def _fetch_one_idx(idx_name, yf_sym):
        entry: Dict[str, Any] = {"name": idx_name}

        if idx_name in NSE_INDEX_SYMBOLS:
            try:
                from nsepython import nse_quote_ltp
                ltp = await asyncio.to_thread(nse_quote_ltp, NSE_INDEX_SYMBOLS[idx_name], "LTP")
                ltp_val = _safe_float(ltp) if str(ltp) != "-" else None
                if ltp_val:
                    entry["last_price"] = ltp_val
                    entry["source"] = "nsepython"
            except Exception as exc:
                logger.debug("nsepython LTP failed for %s: %s", idx_name, exc)

        if not entry.get("last_price"):
            q = await asyncio.to_thread(_yfinance_quote, yf_sym)
            if q:
                entry.update({
                    "last_price": q.get("last_price"),
                    "prev_close": q.get("prev_close"),
                    "change":     q.get("change"),
                    "change_pct": q.get("change_pct"),
                    "source":     "yfinance",
                })
                
        if not entry.get("last_price"):
            q = await asyncio.to_thread(_twelvedata_quote, yf_sym)
            if q:
                entry.update({
                    "last_price": q.get("last_price"),
                    "prev_close": q.get("prev_close"),
                    "change":     q.get("change"),
                    "change_pct": q.get("change_pct"),
                    "source":     "twelvedata",
                })

        entry["as_of"] = datetime.now().isoformat()
        return entry

    tasks = [_fetch_one_idx(k, v) for k, v in YFINANCE_INDEX_MAP.items()]
    idx_results = await asyncio.gather(*tasks)
    res = {"indices": idx_results, "as_of": datetime.now().isoformat()}
    _set_cached("indices", res, 60)
    return res


@router.get("/top-gainers", summary="NSE top gainers (live)")
def get_top_gainers():
    """
    Top 10 gainers from NSE via nsepython.
    Returns a JSON-serialisable list; nsepython DataFrames are converted internally.
    Falls back to yfinance sample if NSE scraping fails.
    """
    cached = _get_cached("top_gainers", 60)
    if cached: return cached

    try:
        from nsepython import nse_get_top_gainers
        df = nse_get_top_gainers()
        gainers = _df_to_records(df, limit=10)
        res = {"gainers": gainers, "as_of": datetime.now().isoformat()}
        _set_cached("top_gainers", res, 60)
        return res
    except Exception as exc:
        logger.warning("nsepython gainers failed: %s — using yfinance fallback", exc)

    # yfinance fallback: snapshot of major NSE large-caps
    fallback_symbols = [
        "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS",
        "ICICIBANK.NS", "BHARTIARTL.NS", "SBIN.NS", "LT.NS",
        "WIPRO.NS", "BAJFINANCE.NS",
    ]
    gainers = []
    for sym in fallback_symbols:
        q = _yfinance_quote(sym)
        if not q or not q.get("last_price"):
            q = _twelvedata_quote(sym)
            if q:
                q["source"] = "twelvedata"
        if q and q.get("change_pct") and q["change_pct"] > 0:
            gainers.append({
                "symbol":     sym.replace(".NS", ""),
                "last_price": q["last_price"],
                "change_pct": q["change_pct"],
                "change":     q["change"],
                "source":     q.get("source", "yfinance"),
            })
    gainers.sort(key=lambda x: x.get("change_pct") or 0, reverse=True)
    return {"gainers": gainers[:10], "as_of": datetime.now().isoformat()}


@router.get("/top-losers", summary="NSE top losers (live)")
def get_top_losers():
    """
    Top 10 losers from NSE via nsepython.
    Falls back to yfinance sample if NSE scraping fails.
    """
    cached = _get_cached("top_losers", 60)
    if cached: return cached

    try:
        from nsepython import nse_get_top_losers
        df = nse_get_top_losers()
        losers = _df_to_records(df, limit=10)
        res = {"losers": losers, "as_of": datetime.now().isoformat()}
        _set_cached("top_losers", res, 60)
        return res
    except Exception as exc:
        logger.warning("nsepython losers failed: %s — using yfinance fallback", exc)

    fallback_symbols = [
        "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS",
        "ICICIBANK.NS", "BHARTIARTL.NS", "SBIN.NS", "LT.NS",
        "WIPRO.NS", "BAJFINANCE.NS",
    ]
    losers = []
    for sym in fallback_symbols:
        q = _yfinance_quote(sym)
        if not q or not q.get("last_price"):
            q = _twelvedata_quote(sym)
            if q:
                q["source"] = "twelvedata"
        if q and q.get("change_pct") and q["change_pct"] < 0:
            losers.append({
                "symbol":     sym.replace(".NS", ""),
                "last_price": q["last_price"],
                "change_pct": q["change_pct"],
                "change":     q["change"],
                "source":     q.get("source", "yfinance"),
            })
    losers.sort(key=lambda x: x.get("change_pct") or 0)
    return {"losers": losers[:10], "as_of": datetime.now().isoformat()}


@router.get("/live-quote/{symbol}", summary="Live NSE equity quote")
def get_live_quote(symbol: str):
    """
    Live quote for an NSE equity symbol (e.g. RELIANCE, TCS, INFY).
    Pass just the ticker without exchange suffix.

    Source: nsepython (quote_equity → priceInfo) → yfinance (.NS) fallback.
    Twelve Data is NOT used here — this is a complementary endpoint.
    """
    sym = symbol.strip().upper()
    # Strip exchange suffixes if provided by mistake
    for suffix in (".NS", ".BO", ".BSE", ".NSE"):
        if sym.endswith(suffix):
            sym = sym[:-len(suffix)]
            break

    # --- nsepython (real-time NSE data) ---
    try:
        from nsepython import quote_equity
        data = quote_equity(sym)
        pi   = data.get("priceInfo", {})
        whl  = pi.get("weekHighLow", {})

        ltp  = _safe_float(pi.get("lastPrice"))
        if ltp:
            return {
                "symbol":     sym,
                "exchange":   "NSE",
                "last_price": ltp,
                "prev_close": _safe_float(pi.get("previousClose") or pi.get("close")),
                "change":     _safe_float(pi.get("change")),
                "change_pct": _safe_float(pi.get("pChange")),
                "open":       _safe_float(pi.get("open")),
                "day_high":   _safe_float(pi.get("intraDayHighLow", {}).get("max")),
                "day_low":    _safe_float(pi.get("intraDayHighLow", {}).get("min")),
                "high_52w":   _safe_float(whl.get("max")),
                "low_52w":    _safe_float(whl.get("min")),
                "vwap":       _safe_float(pi.get("vwap")),
                "source":     "nsepython",
                "as_of":      datetime.now().isoformat(),
            }
    except Exception as exc:
        logger.debug("nsepython quote_equity failed for %s: %s", sym, exc)

    # --- yfinance fallback ---
    q = _yfinance_quote(f"{sym}.NS")
    if not q or not q.get("last_price"):
        q = _twelvedata_quote(f"{sym}.NS")
        if q:
            q["source"] = "twelvedata"
            
    if q and q.get("last_price"):
        q.update({
            "symbol":   sym,
            "exchange": "NSE",
            "source":   q.get("source", "yfinance"),
            "as_of":    datetime.now().isoformat(),
        })
        return q

    raise HTTPException(
        status_code=404,
        detail=(
            f"No live quote found for '{sym}'. "
            "Ensure it is a valid NSE ticker. "
            "Note: NSE real-time data is only available Mon–Fri 9:15–15:30 IST; "
            "yfinance provides last-close outside those hours."
        ),
    )


@router.get("/market-status", summary="NSE market open/closed status")
def get_market_status():
    """
    Returns the current NSE market status (open/closed).
    Logic: Time-based check (IST) is used as the primary source for 'Open' status,
    with nsepython as an auxiliary check.
    """
    cached = _get_cached("market_status", 60)
    if cached: return cached

    now_ist = datetime.utcnow()
    # Weekday check: 0=Mon, 4=Fri
    # Market hours (IST): 9:15 AM to 3:30 PM
    # UTC hours: 3:45 AM to 10:00 AM
    hour_utc  = now_ist.hour + now_ist.minute / 60
    weekday   = now_ist.weekday()
    
    # Rough time-based check
    is_open_time = (weekday < 5) and (3.75 <= hour_utc <= 10.0)
    
    status_str = "Open" if is_open_time else "Closed"
    source = "time-based"

    try:
        from nsepython import nse_marketStatus
        st_data = nse_marketStatus()
        # nsepython returns a list of market statuses, look for 'Normal Market'
        if isinstance(st_data, list):
            for m in st_data:
                if m.get("market") == "Capital Market":
                    # If nsepython says market is open, trust it even if time check says closed (special sessions)
                    if m.get("marketStatus") == "Open":
                        status_str = "Open"
                        source = "nsepython"
                    break
    except Exception:
        pass

    res = {
        "status":  {"marketStatus": status_str},
        "as_of":   datetime.now().isoformat(),
        "source":  source,
    }
    _set_cached("market_status", res, 60)
    return res


@router.get("/events", summary="NSE Upcoming Corporate Events")
def get_events():
    """
    Returns upcoming corporate events from NSE (Earnings, Dividends, Board Meetings).
    Source: nsepython (nse_events)
    """
    cached = _get_cached("events", 3600)
    if cached: return cached

    try:
        from nsepython import nse_events
        df = nse_events()
        if df is None or df.empty:
            return {"events": [], "as_of": datetime.now().isoformat()}
            
        # Limit to next 50 upcoming events
        records = []
        for _, row in df.head(50).iterrows():
            records.append({
                "symbol":   str(row.get("symbol", "")),
                "company":  str(row.get("company", "")),
                "purpose":  str(row.get("purpose", "")),
                "date":     str(row.get("date", "")),
            })
        res = {"events": records, "as_of": datetime.now().isoformat()}
        _set_cached("events", res, 3600)
        return res
    except Exception as exc:
        logger.error("nsepython events failed: %s", exc)
        return {"events": [], "error": "Failed to fetch events", "as_of": datetime.now().isoformat()}

IGNORE_TICKERS = {
    "IEA", "BCCI", "LIVE", "PNG", "KYC", "EPFO", "EPF", "HDFC", "GST", "IT", "USA",
    "SEBI", "CBDT", "DBS", "BIT", "NET", "NEW", "ALL", "SET", "OUT", "FOR", "BUY", "SELL", "HOLD"
}

@router.get("/stocks-in-focus", summary="Trending stocks from news & focus")
async def get_stocks_in_focus():
    """
    Returns a list of stocks currently 'in focus' based on recent news headlines.
    Aggregates tickers from the market feed.
    Parallelized fetching for better performance.
    """
    cached = _get_cached("stocks_in_focus", 300)
    if cached: return cached

    try:
        import asyncio
        from news_scraper import get_market_feed_async
        feed = await get_market_feed_async()
        
        # Collect all tickers from news articles
        all_tickers = []
        articles = feed.get("headline_news", [])
        for art in articles:
            tickers = art.get("tickers", [])
            for t in tickers:
                t_up = t.upper().strip()
                # Basic validation: 2-12 chars, not in ignore list
                if 2 <= len(t_up) <= 12 and t_up not in IGNORE_TICKERS:
                    all_tickers.append(t_up)
        
        # Get unique tickers (top 15)
        unique_tickers = list(dict.fromkeys(all_tickers))[:15]
        
        async def _fetch_one(sym):
            try:
                import asyncio
                q = await asyncio.to_thread(_get_indian_stock_price, sym)
                if q and q.get("last_price"):
                    return {
                        "symbol": sym,
                        "last_price": q["last_price"],
                        "change_pct": q.get("change_pct"),
                        "change": q.get("change"),
                        "source": q.get("source")
                    }
            except:
                pass
            return None

        # Parallelize the 15 calls
        tasks = [_fetch_one(s) for s in unique_tickers]
        results_raw = await asyncio.gather(*tasks)
        results = [r for r in results_raw if r is not None]
                
        res = {"stocks": results, "as_of": datetime.now().isoformat()}
        _set_cached("stocks_in_focus", res, 300)
        return res
    except Exception as exc:
        logger.error(f"Stocks in focus failed: {exc}")
        return {"stocks": [], "error": str(exc)}
