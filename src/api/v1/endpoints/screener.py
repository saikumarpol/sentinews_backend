"""
backend/routers/screener.py
===========================
Full NSE equity screener — dynamic symbol list from NSE equity master CSV.
Scans 1500+ stocks with technicals + fundamentals, cached for 15 minutes.
"""

import io
import math
import logging
import threading
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import time

import httpx
import pandas as pd
import yfinance as yf
import requests
from src.core.config import settings
from src.core.cache import cache

logger = logging.getLogger("sentinews.screener")
router = APIRouter()

# ─── Symbol cache (full NSE equity list, refreshed daily)
_ALL_SYMBOLS: List[str] = []
_SYMBOLS_TS: Optional[datetime] = None

# ─── Scan result cache
_SCAN_RESULT: List[Dict] = []
_SCAN_TS: Optional[datetime] = None
_SCAN_RUNNING = False
_SCAN_LOCK = threading.Lock()

SCAN_TTL_MINUTES = 15          # cache scan results
SYMBOL_TTL_HOURS = 12          # refresh symbol list every 12h

# NSE equity master CSV (publicly available, no login required)
NSE_EQ_CSV = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

# Fallback Nifty 500 curated list in case NSE CSV is unavailable
FALLBACK_SYMBOLS = [
    "RELIANCE","TCS","HDFCBANK","BHARTIARTL","ICICIBANK","SBIN","INFY","HINDUNILVR",
    "ITC","LT","BAJFINANCE","HCLTECH","KOTAKBANK","AXISBANK","ASIANPAINT","MARUTI",
    "SUNPHARMA","TITAN","M&M","BAJAJFINSV","NTPC","POWERGRID","ULTRACEMCO","COALINDIA",
    "NESTLEIND","WIPRO","ONGC","JSWSTEEL","TATAMOTORS","GRASIM","BPCL","TECHM",
    "ADANIENT","TATACONSUM","HINDALCO","DRREDDY","CIPLA","DIVISLAB","BRITANNIA",
    "ADANIPORTS","HEROMOTOCO","BAJAJ-AUTO","EICHERMOT","SHREECEM","APOLLOHOSP",
    "TRENT","INDUSINDBK","SIEMENS","PIDILITIND","HAVELLS","NAUKRI","COLPAL","MARICO",
    "LUPIN","AMBUJACEM","MUTHOOTFIN","TORNTPHARM","INDUSTOWER","HINDPETRO","IOC",
    "ALKEM","BIOCON","IPCALAB","AUROPHARMA","LALPATHLAB","ABBOTINDIA","PFIZER",
    "APLAPOLLO","TATAPOWER","ADANIGREEN","ADANITRANS","ADANIGAS",
    "CANBK","PNB","BANKBARODA","FEDERALBNK","IDFCFIRSTB","YESBANK","AUBANK","RBLBANK",
    "M&MFIN","BAJAJHLDNG","MANAPPURAM","CHOLAFIN","SUNDARMFIN","SHRIRAMFIN",
    "LICHSGFIN","HDFCAMC","NIPPONLIFE","ABCAPITAL","ICICIGI","SBILIFE","HDFCLIFE",
    "IRCTC","IEX","CDSL","MCX","ANGELONE","ZOMATO","NYKAA","DELHIVERY",
    "JUBLFOOD","WESTLIFE","DMART","ABFRL","RAYMOND","VEDL","HINDZINC","NMDC",
    "SAIL","JSWSTEEL","TATASTEEL","MOIL","DEEPAKNTR","TATACHEM","CHEMPLASTS",
    "AARTI","SRF","NOCIL","PIDILITIND","ASTRAL","PRINCEPIPE","SUPREME","POLYCAB",
    "KEI","HAVELLS","CROMPTON","BLUESTAR","VOLTAS","WHIRLPOOL","AMBER","DIXON",
    "PERSISTENT","LTTS","KPITTECH","MPHASIS","COFORGE","HEXAWARE","OFSS","CYIENT",
    "TATAELXSI","SONATSOFTW","INTELLECT","TANLA","ECLERX","NEWGEN","MASTEK",
    "TVSMOTOR","BAJAJAUTO","HEROMOTOCO","EICHERMOT","ESCORTS","MOTHERSON","BALKRISIND",
    "CEAT","MRF","EXIDEIND","SKF","SCHAEFFLER","TIMKEN","ENDURANCE",
    "DLF","GODREJPROP","PRESTIGE","BRIGADE","SOBHA","PHOENIXLTD","OBEROIRLTY",
    "GLENMARK","JUBLPHARMA","NATCOPHARM","AJANTPHRM","GRANULES","LAURUS","DIVISLAB",
    "STRIDES","SOLARA","ERIS","ZYDUSLIFE","TORNTPHARM","ALEMBICLTD","SPARC",
    "KIMS","MAXHEALTH","FORTIS","NARAYANAMUR","METROPOLIS","LALPATHLAB","DRREDDY",
    "NTPC","POWERGRID","TATAPOWER","ADANIGREEN","SJVN","NHPC","IRFC","RVNL",
    "RECLTD","PFC","HUDCO","NBCC","IREDA","IRCON","RAILTEL","BEL","HAL","BHEL","BEML",
    "GAIL","IGL","MGL","PETRONET","ATGL","GUJGASLTD","GSPL","ONGC","BPCL","HINDPETRO",
    "COLPAL","MARICO","EMAMILTD","JYOTHYLAB","BRITANNIA","NESTLEIND","TATACONSUM",
    "PATANJALI","BATAINDIA","RELAXO","PAGEIND","METRO","TITAN","KALYAN","SENCO",
    "CRISIL","EDELWEISS","MOTILALOFS","ANGELONE","CDSL","KFINTECH","BSE","MCX",
]


def get_all_symbols() -> List[str]:
    """Fetch the full NSE equity list from NSE master CSV. Falls back to curated list."""
    global _ALL_SYMBOLS, _SYMBOLS_TS

    now = datetime.utcnow()
    if _ALL_SYMBOLS and _SYMBOLS_TS and (now - _SYMBOLS_TS) < timedelta(hours=SYMBOL_TTL_HOURS):
        return _ALL_SYMBOLS

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*",
            "Referer": "https://www.nseindia.com/",
        }
        resp = httpx.get(NSE_EQ_CSV, headers=headers, timeout=20, follow_redirects=True)
        resp.raise_for_status()

        df = pd.read_csv(io.StringIO(resp.text))
        # NSE CSV has a "SYMBOL" column
        symbol_col = [c for c in df.columns if "SYMBOL" in c.upper()]
        if not symbol_col:
            raise ValueError("Symbol column not found in NSE CSV")

        symbols = [
            str(s).strip()
            for s in df[symbol_col[0]].dropna().tolist()
            if str(s).strip() and len(str(s).strip()) <= 20
        ]
        # Remove indices and invalid
        exclude = {"NIFTY", "NIFTY50", "SENSEX", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}
        symbols = [s for s in symbols if s not in exclude and "&" not in s or s in ("M&M", "M&MFIN", "BAJAJ-AUTO")]

        # Ensure FALLBACK (which are Nifty 500 top market cap) are strictly at the beginning
        top_syms = [s for s in FALLBACK_SYMBOLS if s in symbols]
        other_syms = [s for s in symbols if s not in set(top_syms)]
        final_list = top_syms + other_syms

        _ALL_SYMBOLS = final_list
        _SYMBOLS_TS = now
        logger.info("Loaded %d NSE equity symbols from CSV (Top Market Cap sorted first)", len(final_list))
        return final_list

    except Exception as exc:
        logger.warning("NSE CSV fetch failed (%s). Using fallback list of %d symbols.", exc, len(FALLBACK_SYMBOLS))
        _ALL_SYMBOLS = FALLBACK_SYMBOLS
        _SYMBOLS_TS = now
        return FALLBACK_SYMBOLS


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


def _safe(val) -> Optional[float]:
    try:
        if val is None:
            return None
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return round(f, 2)
    except (TypeError, ValueError):
        return None


def _fetch_info(args):
    sym, yf_sym = args
    try:
        # Avoid heavy pounding
        time.sleep(0.15)
        # Use our persistent session
        ticker = yf.Ticker(yf_sym)
        info = ticker.info or {}
        return sym, info
    except Exception as e:
        # Return empty dict if blocked so screener ignores fundamentals but returns technicals
        return sym, {}


def _download_batch(yf_syms: List[str]) -> pd.DataFrame:
    """Download OHLCV for a batch of symbols."""
    try:
        return yf.download(
            yf_syms,
            period="1y",
            interval="1d",
            threads=True
        )
    except Exception:
        return pd.DataFrame()


def run_full_scan() -> List[Dict[str, Any]]:
    """
    Full NSE scan — downloads OHLCV in batches of 300,
    then concurrently fetches fundamentals.
    """
    from concurrent.futures import ThreadPoolExecutor

    symbols = get_all_symbols()
    yf_syms = [f"{s}.NS" for s in symbols]
    n = len(symbols)
    logger.info("Screener: starting full scan of %d NSE symbols", n)

    # ── 1. Download OHLCV in batches of 50 ──
    BATCH = 50
    ohlcv_map: Dict[str, pd.DataFrame] = {}

    for start in range(0, n, BATCH):
        batch_syms = yf_syms[start:start + BATCH]
        raw = _download_batch(batch_syms)
        time.sleep(1.5)  # delay between batches to respect rate limits
        if raw.empty:
            continue

        for yf_sym in batch_syms:
            try:
                df = raw[yf_sym] if isinstance(raw.columns, pd.MultiIndex) else raw
                if "Close" in df.columns:
                    ohlcv_map[yf_sym] = df
            except Exception:
                continue

    logger.info("Download done. %d usable tickers.", len(ohlcv_map))

    # ── 2. Fetch fundamentals concurrently ──
    pairs = [(sym, f"{sym}.NS") for sym in symbols if f"{sym}.NS" in ohlcv_map]
    fund_map: Dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        for sym, info in ex.map(_fetch_info, pairs):
            fund_map[sym] = info

    logger.info("Fundamentals fetched. Building results…")

    # ── 3. Build results ──
    results = []
    for sym in symbols:
        yf_sym = f"{sym}.NS"
        df = ohlcv_map.get(yf_sym)
        if df is None:
            continue
        try:
            close = df["Close"].dropna()
            volume = df["Volume"].dropna()
            if len(close) < 20:
                continue

            curr = float(close.iloc[-1])
            prev = float(close.iloc[-2]) if len(close) > 1 else curr
            chg = ((curr - prev) / prev) * 100

            high52 = float(close.max())
            low52  = float(close.min())
            dist_h = ((curr - high52) / high52) * 100

            sma50  = float(close.rolling(50).mean().iloc[-1])  if len(close) >= 50  else None
            sma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
            rsi14  = float(calculate_rsi(close, 14).iloc[-1])

            vol    = float(volume.iloc[-1])
            vsma20 = float(volume.rolling(20).mean().iloc[-1]) if len(volume) >= 20 else vol
            vrat   = vol / vsma20 if vsma20 > 0 else 1.0

            info = fund_map.get(sym, {})
            pe   = _safe(info.get("trailingPE") or info.get("forwardPE"))
            pb   = _safe(info.get("priceToBook"))
            eps  = _safe(info.get("trailingEps"))
            roe_r = info.get("returnOnEquity")
            roa_r = info.get("returnOnAssets")
            div_r = info.get("dividendYield")
            deq_r = info.get("debtToEquity")
            mcap  = _safe(info.get("marketCap"))
            sector = info.get("sector") or "—"

            roe = _safe(roe_r * 100) if roe_r is not None else None
            roa = _safe(roa_r * 100) if roa_r is not None else None    # ROCE proxy
            div = _safe(div_r * 100) if div_r is not None else None
            deq = _safe(deq_r)

            # Extra derived metrics
            rev_g_r = info.get("revenueGrowth")
            eps_g_r = info.get("earningsGrowth")
            sales_g = _safe(rev_g_r * 100) if rev_g_r is not None else None
            eps_g   = _safe(eps_g_r * 100) if eps_g_r is not None else None
            curr_ratio = _safe(info.get("currentRatio"))
            quick_ratio = _safe(info.get("quickRatio"))

            results.append({
                "symbol":      sym,
                "sector":      sector,
                "price":       _safe(curr),
                "change_pct":  _safe(chg),
                "high_52w":    _safe(high52),
                "low_52w":     _safe(low52),
                "dist_high":   _safe(dist_h),
                "sma50":       _safe(sma50),
                "sma200":      _safe(sma200),
                "rsi":         _safe(rsi14),
                "vol_ratio":   _safe(vrat),
                # Fundamentals
                "market_cap":  mcap,
                "pe":          pe,
                "pb":          pb,
                "eps":         eps,
                "roe":         roe,
                "roce":        roa,
                "div_yield":   div,
                "debt_to_eq":  deq,
                "sales_growth": sales_g,
                "eps_growth":   eps_g,
                "current_ratio": curr_ratio,
                "quick_ratio":   quick_ratio,
                # Preset tags
                "is_breakout":   vrat  > 2.0 and chg  > 2.0,
                "is_oversold":   rsi14 < 30,
                "is_overbought": rsi14 > 70,
                "near_52w_high": dist_h > -5,
                "near_52w_low":  dist_h < -45,
                "above_sma200":  (sma200 is not None and curr > sma200),
                "below_sma200":  (sma200 is not None and curr < sma200),
                "golden_cross":  (sma50 is not None and sma200 is not None and sma50 > sma200),
                "is_value":      (pe is not None and 0 < pe < 15),
                "is_high_roe":   (roe is not None and roe > 15),
                "is_high_div":   (div is not None and div > 3),
                "is_low_debt":   (deq is not None and deq < 0.5),
                "is_quality":    (roe is not None and roe > 15 and deq is not None and deq < 1),
                "is_growth":     (eps_g is not None and eps_g > 20 and sales_g is not None and sales_g > 15),
                "is_momentum":   (dist_h > -10 and rsi14 > 55 and chg > 0),
                "is_turnaround": (dist_h < -30 and rsi14 < 45 and pe is not None and pe > 0),
                "is_smallcap":   (mcap is not None and mcap < 5000e7),   # < 5000 Cr
                "is_midcap":     (mcap is not None and 5000e7 <= mcap < 20000e7),
                "is_largecap":   (mcap is not None and mcap >= 20000e7),
            })
        except Exception:
            continue

    logger.info("Screener scan complete: %d results", len(results))
    
    # Sort results by market cap descending before returning
    results.sort(key=lambda x: x.get("market_cap") or 0, reverse=True)
    return results


def _bg_scan():
    """Background thread to warm up the cache."""
    global _SCAN_RESULT, _SCAN_TS, _SCAN_RUNNING
    try:
        result = run_full_scan()
        with _SCAN_LOCK:
            _SCAN_RESULT = result
            _SCAN_TS = datetime.utcnow()
    except Exception as exc:
        logger.error("Background scan failed: %s", exc)
    finally:
        _SCAN_RUNNING = False


@router.get("/screener")
def get_screener_data():

    CACHE_FULL    = "screener_nse_full"
    CACHE_PARTIAL = "screener_nse_partial"

    # Check if background scan finished with more data than what's cached
    if _SCAN_RESULT:
        partial = cache.get(CACHE_PARTIAL)
        full = cache.get(CACHE_FULL)
        cached_cnt = len(full) if full else (len(partial) if partial else 0)
        if len(_SCAN_RESULT) > cached_cnt:
            cache.set(CACHE_FULL, _SCAN_RESULT, ttl=900)
            cache.set(CACHE_PARTIAL, None, ttl=1)  # effectively delete
            logger.info("Screener: upgraded to full scan (%d stocks)", len(_SCAN_RESULT))

    # Serve from full cache if available (scanning complete)
    full_data = cache.get(CACHE_FULL)
    if full_data and not cache.get(CACHE_PARTIAL):
        return {"success": True, "count": len(full_data), "data": full_data,
                "from_cache": True, "scanning": False}

    # Start background scan if not running
    if not _SCAN_RUNNING:
        with _SCAN_LOCK:
            if not _SCAN_RUNNING:
                _SCAN_RUNNING = True
                import threading as _t
                _t.Thread(target=_bg_scan, daemon=True).start()

    # Serve partial cache while background scan runs
    partial_data = cache.get(CACHE_PARTIAL)
    if partial_data:
        return {"success": True, "count": len(partial_data), "data": partial_data,
                "from_cache": True, "scanning": True}

    # No data at all yet — trigger scan synchronously for quick preview
    try:
        # get_all_symbols() is already prioritized (Nifty 500 first)
        symbols = get_all_symbols()[:50]
        logger.info("Screener: serving quick preview of first %d symbols", len(symbols))
        yf_syms = [f"{s}.NS" for s in symbols]
        from concurrent.futures import ThreadPoolExecutor

        raw = yf.download(yf_syms, period="1y", interval="1d", group_by="ticker",
                          auto_adjust=True, progress=False, threads=True)

        pairs = [(s, f"{s}.NS") for s in symbols]
        fund_map = {}
        with ThreadPoolExecutor(max_workers=4) as ex:
            for sym, info in ex.map(_fetch_info, pairs):
                fund_map[sym] = info

        results = []
        for i, sym in enumerate(symbols):
            yf_sym = yf_syms[i]
            try:
                df = raw[yf_sym] if isinstance(raw.columns, pd.MultiIndex) else raw
                close = df["Close"].dropna()
                volume = df["Volume"].dropna()
                if len(close) < 20:
                    continue

                curr = float(close.iloc[-1])
                prev = float(close.iloc[-2]) if len(close) > 1 else curr
                chg  = ((curr - prev) / prev) * 100
                high52 = float(close.max())
                low52  = float(close.min())
                dist_h = ((curr - high52) / high52) * 100
                sma50  = float(close.rolling(50).mean().iloc[-1])  if len(close) >= 50  else None
                sma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
                rsi14  = float(calculate_rsi(close, 14).iloc[-1])
                vol    = float(volume.iloc[-1])
                vsma20 = float(volume.rolling(20).mean().iloc[-1]) if len(volume) >= 20 else vol
                vrat   = vol / vsma20 if vsma20 > 0 else 1.0

                info = fund_map.get(sym, {})
                pe   = _safe(info.get("trailingPE") or info.get("forwardPE"))
                pb   = _safe(info.get("priceToBook"))
                eps  = _safe(info.get("trailingEps"))
                roe_r   = info.get("returnOnEquity")
                roa_r   = info.get("returnOnAssets")
                div_r   = info.get("dividendYield")
                eps_g_r = info.get("earningsGrowth")
                rev_g_r = info.get("revenueGrowth")

                roe  = _safe(roe_r * 100) if roe_r is not None else None
                roa  = _safe(roa_r * 100) if roa_r is not None else None
                div  = _safe(div_r * 100) if div_r is not None else None
                deq  = _safe(info.get("debtToEquity"))
                mcap = _safe(info.get("marketCap"))
                eps_g = _safe(eps_g_r * 100) if eps_g_r is not None else None
                sales_g = _safe(rev_g_r * 100) if rev_g_r is not None else None

                results.append({
                    "symbol": sym, "sector": info.get("sector") or "—",
                    "price": _safe(curr), "change_pct": _safe(chg),
                    "high_52w": _safe(high52), "low_52w": _safe(low52),
                    "dist_high": _safe(dist_h), "sma50": _safe(sma50), "sma200": _safe(sma200),
                    "rsi": _safe(rsi14), "vol_ratio": _safe(vrat),
                    "market_cap": mcap, "pe": pe, "pb": pb, "eps": eps,
                    "roe": roe, "roce": roa, "div_yield": div, "debt_to_eq": deq,
                    "sales_growth": sales_g, "eps_growth": eps_g,
                    "is_breakout": vrat > 2.0 and chg > 2.0,
                    "is_oversold": rsi14 < 30, "is_overbought": rsi14 > 70,
                    "near_52w_high": dist_h > -5, "near_52w_low": dist_h < -45,
                    "above_sma200": sma200 is not None and curr > sma200,
                    "below_sma200": sma200 is not None and curr < sma200,
                    "golden_cross": sma50 is not None and sma200 is not None and sma50 > sma200,
                    "is_value": pe is not None and 0 < pe < 15,
                    "is_high_roe": roe is not None and roe > 15,
                    "is_high_div": div is not None and div > 3,
                    "is_low_debt": deq is not None and deq < 0.5,
                    "is_quality": roe is not None and roe > 15 and deq is not None and deq < 1,
                    "is_growth": eps_g is not None and eps_g > 20 and sales_g is not None and sales_g > 15,
                    "is_momentum": dist_h > -10 and rsi14 > 55 and chg > 0,
                    "is_turnaround": dist_h < -30 and rsi14 < 45 and pe is not None and pe > 0,
                    "is_smallcap": mcap is not None and mcap < 5000e7,
                    "is_midcap": mcap is not None and 5000e7 <= mcap < 20000e7,
                    "is_largecap": mcap is not None and mcap >= 20000e7,
                })
            except Exception:
                continue

        results.sort(key=lambda x: x.get("market_cap") or 0, reverse=True)
        cache.set(CACHE_PARTIAL, results, ttl=120)   # 2 min — overridden when full scan completes
        return {"success": True, "count": len(results), "data": results,
                "from_cache": False, "scanning": True}

    except Exception as exc:
        logger.error("Screener quick preview failed: %s", exc)
        raise HTTPException(500, str(exc))
