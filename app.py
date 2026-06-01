from typing import Any, List, Optional, Dict, Tuple
import logging
import os
import re
from datetime import datetime, timedelta
import asyncio

import httpx
from dotenv import load_dotenv
from fastapi import (
    FastAPI,
    Depends,
    HTTPException,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, Field
from sqlmodel import select

# Load environment variables from .env
load_dotenv()

# ---------- LOGGER ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
logger = logging.getLogger("sentinews")

from news_scraper import get_market_feed, get_commodities_dashboard
from ai_processor import summarize_news_for_watchlist
from db import init_db, get_session, User, StockNote
from auth import (
    hash_password,
    verify_password,
    create_access_token,
    get_current_user,
)
from routers import stock_detail, portfolio, indian_market, market_reports, screener, daily_news

app = FastAPI(title="Sentinews API", version="0.1.0")

# Include routers
app.include_router(stock_detail.router)
app.include_router(portfolio.router)
app.include_router(indian_market.router)
app.include_router(market_reports.router)
app.include_router(screener.router)
app.include_router(daily_news.router)

# ---------- GLOBAL EXCEPTION HANDLER (CORS FIX) ----------
from fastapi.responses import JSONResponse
from fastapi import Request

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Ensure 500 errors still return CORS headers so the browser 
    doesn't show 'Blocked by CORS'.
    """
    logger.error(f"Global Error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "error": str(exc)},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        }
    )

# ---------- CORS ----------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,   # must be False when allow_origins=["*"]
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Twelve Data config (free data) ----------

TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
print("DEBUG TWELVEDATA_API_KEY prefix:", TWELVEDATA_API_KEY[:6] if TWELVEDATA_API_KEY else "Not Set")

# simple in-memory cache for price history: {symbol: (expires_at, history_list)}
_price_cache: Dict[str, Tuple[datetime, List[Dict]]] = {}

class SimpleCache:
    def __init__(self):
        self._data = {}

    def get(self, key):
        if key in self._data:
            expiry, val = self._data[key]
            if datetime.utcnow() < expiry:
                return val
            del self._data[key]
        return None

    def set(self, key, val, ttl=300):
        expiry = datetime.utcnow() + timedelta(seconds=ttl)
        self._data[key] = (expiry, val)

cache = SimpleCache()


def _validate_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    # allow letters, digits, dot and dash
    if not s or any(c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-" for c in s):
        raise HTTPException(status_code=400, detail="Invalid symbol")
    return s


async def fetch_daily_history(symbol: str) -> List[Dict]:
    """
    Fetch daily close prices using Twelve Data TIME_SERIES.
    Returns list of {"date": str, "close": float} sorted oldest->newest.
    Uses simple in-memory cache to reduce API calls.
    """
    if not TWELVEDATA_API_KEY:
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
        "apikey": TWELVEDATA_API_KEY,
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(url, params=params)
    except httpx.RequestError:
        raise HTTPException(status_code=502, detail="Price data service unavailable")

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Price data error")

    data = resp.json()
    # Twelve Data: {"status":"error","message":"..."} on error
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
            date_str = bar["datetime"]  # e.g. "2024-03-01"
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


# ---------- MODELS ----------

class DigestRequest(BaseModel):
    watchlist: Optional[List[str]] = []


class SignupBody(BaseModel):
    email: str
    password: str


class ForgotPasswordBody(BaseModel):
    email: str


# ---------- NOTES MODELS ----------

class NoteBody(BaseModel):
    text: str


class NoteResponse(BaseModel):
    symbol: str
    text: str
    updated_at: str


# ---------- SEARCH MODELS ----------

class SearchResult(BaseModel):
    symbol: str
    name: str
    exchange: Optional[str] = None
    type: Optional[str] = None





# ---------- CACHE MANAGER ----------


class CacheManager:
    """Simple in-memory TTL cache."""

    def __init__(self) -> None:
        self._store: Dict[str, Dict] = {}

    def get(self, key: str) -> Any:
        """Return cached value if it exists and has not expired, else None."""
        entry = self._store.get(key)
        if entry is None:
            return None
        if datetime.utcnow() > entry["expires_at"]:
            del self._store[key]
            return None
        return entry["value"]

    def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """Store *value* under *key* with an expiry of *ttl* seconds."""
        self._store[key] = {
            "value": value,
            "expires_at": datetime.utcnow() + timedelta(seconds=ttl),
        }

    def clear(self) -> None:
        """Remove all cached entries."""
        self._store.clear()


# Module-level cache singleton
cache = CacheManager()

# Default cache TTL (seconds) — read from env, fallback 300
_CACHE_TTL = int(os.getenv("CACHE_DURATION", "300"))





# ---------- STARTUP ----------

@app.on_event("startup")
def on_startup():
    init_db()


# ---------- ROOT & HEALTH ----------


@app.get("/", summary="API root")
async def root():
    """Root endpoint — confirms the API is running."""
    return {
        "name": "Sentinews API",
        "version": "0.1.0",
        "status": "running",
        "docs": "/docs",
    }


@app.get("/health", summary="Health check")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "api_version": "0.1.0",
        "data_sources": ["nsepython", "yfinance", "twelve-data", "newsapi", "rss-feeds"],
        "timestamp": datetime.utcnow().isoformat(),
    }


# ---------- AUTH: SIGNUP & LOGIN ----------

@app.post("/signup")
def signup(body: SignupBody):
    """
    Create a new user with email + password, return JWT token.
    """
    with get_session() as session:
        email_clean = body.email.lower().strip()
        existing = session.exec(
            select(User).where(User.email == email_clean)
        ).first()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered",
            )

        user = User(
            email=email_clean,
            hashed_password=hash_password(body.password),
        )
        session.add(user)
        session.commit()
        session.refresh(user)

    token = create_access_token({"user_id": user.id})
    return {"access_token": token, "token_type": "bearer"}


@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Login with email + password (email is sent as `username`), return JWT.
    """
    with get_session() as session:
        email_clean = form_data.username.lower().strip()
        user = session.exec(
            select(User).where(User.email == email_clean)
        ).first()

        if not user or not verify_password(
            form_data.password, user.hashed_password
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
            )

    token = create_access_token({"user_id": user.id})
    return {"access_token": token, "token_type": "bearer"}


@app.post("/forgot-password")
def forgot_password(body: ForgotPasswordBody):
    """
    Simulate sending a password reset email.
    In a real app, this would generate a token and send an email.
    """
    with get_session() as session:
        user = session.exec(
            select(User).where(User.email == body.email)
        ).first()

    # We always return success to avoid email enumeration
    return {"message": "If an account exists with that email, a reset link has been sent."}


# ---------- FII / DII SNAPSHOT ----------

def _fii_dii_placeholder():
    """Fallback placeholder FII/DII data."""
    return {
        "date": datetime.utcnow().date().isoformat(),
        "fii": {
            "buy": 12502.85,
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
    """
    Fetch live FII/DII data.
    Primary: nsepython.nse_fiidii()
    Secondary: httpx regex scraper for NSE India reports page.
    Falls back to placeholder on any error.
    """
    # 1. Primary Method: nsepython
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

    # 2. Secondary Method: HTTP scraper
    url = "https://www.nseindia.com/reports/fii-dii"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
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

        fii_row = None
        dii_row = None
        parsed_date = datetime.utcnow().date().isoformat()

        for match in row_pattern.finditer(html):
            category = match.group(1).strip().upper()
            date_raw = match.group(2).strip()
            buy = _num(match.group(3))
            sell = _num(match.group(4))
            net = _num(match.group(5))

            try:
                row_date = _parse_date(date_raw)
            except ValueError:
                row_date = datetime.utcnow().date().isoformat()

            if "DII" in category and dii_row is None:
                dii_row = {"buy": buy, "sell": sell, "net": net}
                parsed_date = row_date
            elif ("FII" in category or "FPI" in category) and fii_row is None:
                fii_row = {"buy": buy, "sell": sell, "net": net}
                parsed_date = row_date

            if fii_row and dii_row:
                break

        if not fii_row or not dii_row:
            return _fii_dii_placeholder()

        return {
            "date": parsed_date,
            "fii": fii_row,
            "dii": dii_row,
            "currency": "INR_CR",
            "source": "nse-scrape"
        }
    except Exception:
        return _fii_dii_placeholder()


_fii_dii_cache = {"data": None, "expires_at": None}

async def fetch_fii_dii_live():
    """Cached wrapper for FII/DII data."""
    global _fii_dii_cache
    now = datetime.utcnow()
    if _fii_dii_cache["data"] and _fii_dii_cache["expires_at"] > now:
        return _fii_dii_cache["data"]
    
    data = await _fetch_fii_dii_live_impl()
    _fii_dii_cache["data"] = data
    _fii_dii_cache["expires_at"] = now + timedelta(minutes=30)
    return data


def _parse_fiidii_list(raw_data: Any) -> Dict:
    """
    Helper to parse Barawakar FII/DII list into a structured dict.

    Returns the frontend-expected format.
    """
    if not isinstance(raw_data, list):
        # Fallback for unexpected formats
        return _fii_dii_placeholder()

    result = {
        "date": datetime.utcnow().date().isoformat(),
        "fii": {"buy": 0.0, "sell": 0.0, "net": 0.0},
        "dii": {"buy": 0.0, "sell": 0.0, "net": 0.0},
        "source": "barawakar"
    }

    def _to_float(val: Any) -> float:
        try:
            if isinstance(val, str):
                return float(val.replace(",", ""))
            return float(val or 0)
        except (ValueError, TypeError):
            return 0.0

    fii_found = False
    dii_found = False

    for item in raw_data:
        if not isinstance(item, dict):
            continue
        
        cat = item.get("category", "").upper()
        # Barawakar uses buyValue, sellValue, netValue
        buy = _to_float(item.get("buyValue"))
        sell = _to_float(item.get("sellValue"))
        net = _to_float(item.get("netValue"))
        date_str = item.get("date")

        is_fii = "FII" in cat or "FPI" in cat
        is_dii = "DII" in cat

        if is_fii and not fii_found:
            result["fii"] = {"buy": buy, "sell": sell, "net": net}
            if date_str:
                result["date"] = date_str
            fii_found = True
        elif is_dii and not dii_found:
            result["dii"] = {"buy": buy, "sell": sell, "net": net}
            if date_str:
                result["date"] = date_str
            dii_found = True
            
        if fii_found and dii_found:
            break
            
    return result


# ---------- PUBLIC MARKET FEED (SUPER FEED) ----------

@app.get("/market-feed")
async def market_feed():
    """
    Super market feed: sections for news, geo, fii/dii, commodities, etc.
    Serves from background-warmed cache for instant loads.
    """
    from news_scraper import get_market_feed_async
    base_feed = await get_market_feed_async() 
    headline_news = base_feed.get("headline_news", [])

    # Enrich with sentiment (local and fast)
    from ai_processor import enrich_news_batch
    enriched = enrich_news_batch(headline_news)

    # Watchlist filter
    digest_items = summarize_news_for_watchlist(enriched, watchlist=[])

    # FII/DII live fetch
    fii_dii = await fetch_fii_dii_live()

    valid_scores = [item["sentiment"] for item in digest_items if isinstance(item, dict) and isinstance(item.get("sentiment"), (int, float))]
    avg_score = sum(valid_scores) / len(valid_scores) if valid_scores else 0.5
        
    overall_label = "Neutral"
    if avg_score >= 0.65: overall_label = "Extremely Bullish"
    elif avg_score >= 0.55: overall_label = "Bullish"
    elif avg_score >= 0.45: overall_label = "Neutral"
    elif avg_score >= 0.35: overall_label = "Bearish"
    else: overall_label = "Extremely Bearish"
        
    overall_sentiment = {"score": avg_score, "label": overall_label}

    return {
        "headline_news":     digest_items,
        "overall_sentiment": overall_sentiment,
        "fii_dii":           fii_dii,
        "total_articles":    len(digest_items),
        "status":            "ok"
    }

async def background_news_scraper():
    """Warms up the news cache every 10 minutes."""
    from news_scraper import get_market_feed_async
    while True:
        try:
            logger.info("Background: Refreshing news feed...")
            await get_market_feed_async(force_refresh=True)
            logger.info("Background: News refresh success.")

            # Refresh stocks in focus in background
            logger.info("Background: Refreshing stocks in focus...")
            from routers.indian_market import get_stocks_in_focus
            await get_stocks_in_focus(force_refresh=True)
            logger.info("Background: Stocks in focus refresh success.")

            # Warm up commodities/indices/currencies dashboards in background
            logger.info("Background: Refreshing commodities, indices, and currencies...")
            from news_scraper import get_commodities_dashboard, get_indices_dashboard, get_currencies_dashboard
            
            # Spot
            c_spot = await get_commodities_dashboard(is_futures=False, force_refresh=True)
            cache.set("commodities_dashboard_spot", c_spot, ttl=600)
            
            i_spot = await get_indices_dashboard(is_futures=False, force_refresh=True)
            cache.set("indices_dashboard_spot", i_spot, ttl=600)
            
            x_spot = await get_currencies_dashboard(is_futures=False, force_refresh=True)
            cache.set("currencies_dashboard_spot", x_spot, ttl=600)

            # Futures
            c_fut = await get_commodities_dashboard(is_futures=True, force_refresh=True)
            cache.set("commodities_dashboard_futures", c_fut, ttl=600)
            
            i_fut = await get_indices_dashboard(is_futures=True, force_refresh=True)
            cache.set("indices_dashboard_futures", i_fut, ttl=600)
            
            x_fut = await get_currencies_dashboard(is_futures=True, force_refresh=True)
            cache.set("currencies_dashboard_futures", x_fut, ttl=600)

            logger.info("Background: Dashboards refresh success.")
        except Exception as e:
            logger.error(f"Background: News/Focus/Dashboards refresh failed: {e}")
        await asyncio.sleep(600)

@app.on_event("startup")
async def on_startup():
    init_db()
    # Kick off background scrapers
    import asyncio
    asyncio.create_task(background_news_scraper())
    
    # NEW: Warm up the screener as well
    from routers.screener import get_screener_data
    try:
        # This will trigger the background thread in screener.py
        get_screener_data()
        logger.info("Background: Screener scan initiated.")
    except Exception as e:
        logger.error(f"Background: Screener init failed: {e}")

    logger.info("Sentinews API started with background tasks.")


# ---------- USER-SPECIFIC DIGEST (AUTH REQUIRED) ----------

@app.post("/digest")
def digest(
    req: DigestRequest,
    current_user: User = Depends(get_current_user),
):
    """
    Personalized digest based on watchlist from request body.
    """
    feed = get_market_feed()
    news_items = feed.get("headline_news", []) if isinstance(feed, dict) else feed
    from ai_processor import enrich_news_batch
    enriched = enrich_news_batch(news_items)
    digest_items = summarize_news_for_watchlist(
        enriched,
        watchlist=req.watchlist or [],
    )
    return digest_items


# ---------- SYMBOL SEARCH (PUBLIC) ----------

@app.get("/search", response_model=List[SearchResult])
async def search_symbol(query: str):
    """
    Search stocks by name or symbol using Twelve Data symbol_search.
    Falls back to Yahoo Finance search if Twelve Data key is not set.
    """
    q = query.strip()
    if not q:
        return []

    if not TWELVEDATA_API_KEY:
        # Fallback to Yahoo Finance search
        try:
            url = "https://query2.finance.yahoo.com/v1/finance/search"
            params = {"q": q}
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, params=params, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                quotes = data.get("quotes") or []
                results: List[SearchResult] = []
                for item in quotes:
                    sym = item.get("symbol")
                    name = item.get("longname") or item.get("shortname") or ""
                    exch = item.get("exchange") or item.get("exchDisp")
                    t = item.get("quoteType") or item.get("typeDisp")
                    if not sym or not name:
                        continue
                    results.append(
                        SearchResult(symbol=sym, name=name, exchange=exch, type=t)
                    )
                return results[:10]
        except Exception as e:
            logger.error(f"Yahoo Finance search fallback failed: {e}")
        
        raise HTTPException(
            status_code=500,
            detail="Price API key not configured and search fallback failed",
        )

    url = "https://api.twelvedata.com/symbol_search"
    params = {
        "symbol": q,
        "apikey": TWELVEDATA_API_KEY,
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
    except httpx.RequestError:
        raise HTTPException(status_code=502, detail="Search service unavailable")

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Search error")

    data = resp.json()
    items = data.get("data") or []
    results: List[SearchResult] = []
    for item in items:
        sym = item.get("symbol")
        name = item.get("instrument_name") or item.get("name") or ""
        exch = item.get("exchange") or item.get("mic_code")
        t = item.get("instrument_type") or item.get("type")
        if not sym or not name:
            continue
        results.append(
            SearchResult(symbol=sym, name=name, exchange=exch, type=t)
        )

    return results[:10]


# ---------- NOTES PER STOCK (AUTH REQUIRED) ----------

@app.get("/notes/{symbol}", response_model=NoteResponse)
def get_note(
    symbol: str,
    current_user: User = Depends(get_current_user),
):
    """
    Get saved note for a stock symbol for the current user.
    """
    symbol = _validate_symbol(symbol)
    with get_session() as session:
        note = session.exec(
            select(StockNote).where(
                StockNote.user_id == current_user.id,
                StockNote.symbol == symbol,
            )
        ).first()
        if not note:
            return NoteResponse(symbol=symbol, text="", updated_at="")
        return NoteResponse(
            symbol=symbol,
            text=note.text,
            updated_at=note.updated_at.isoformat(),
        )


@app.post("/notes/{symbol}", response_model=NoteResponse)
def upsert_note(
    symbol: str,
    body: NoteBody,
    current_user: User = Depends(get_current_user),
):
    """
    Create or update a note for a stock symbol for the current user.
    """
    symbol = _validate_symbol(symbol)
    with get_session() as session:
        note = session.exec(
            select(StockNote).where(
                StockNote.user_id == current_user.id,
                StockNote.symbol == symbol,
            )
        ).first()
        now = datetime.utcnow()
        if note:
            note.text = body.text
            note.updated_at = now
        else:
            note = StockNote(
                user_id=current_user.id,
                symbol=symbol,
                text=body.text,
                updated_at=now,
            )
            session.add(note)
        session.commit()
        session.refresh(note)
        return NoteResponse(
            symbol=symbol,
            text=note.text,
            updated_at=note.updated_at.isoformat(),
        )


# ---------- WATCHLIST PERFORMANCE (AUTH REQUIRED) ----------

@app.get("/watchlist/performance", response_model=PerformanceResponse)
async def watchlist_performance(
    symbol: str,
):
    """
    Free-version performance: uses Twelve Data daily data with caching.
    """
    history = await fetch_daily_history(symbol)
    last = history[-1]
    perf = compute_performance(history)

    # send last 3 years of data max (smaller payload)
    cutoff = max(0, len(history) - 252 * 3)
    sliced = history[cutoff:]

    # snapshot summary
    recent = history[-252:] if len(history) >= 252 else history
    prices = [h["close"] for h in recent]
    low_52w = min(prices) if prices else None
    high_52w = max(prices) if prices else None

    snap = Snapshot(
        last_price=last["close"],
        change_1d_pct=perf.get("1D"),
        high_52w=high_52w,
        low_52w=low_52w,
    )

    return PerformanceResponse(
        symbol=symbol,
        last_price=last["close"],
        last_date=last["date"],
        performance=perf,
        history=sliced,
        snapshot=snap,
    )


# (Barawakar /api/* routes removed — data now served from /market/* endpoints via nsepython + yfinance)


from news_scraper import (
    get_market_feed_async, 
    get_commodities_dashboard,
    get_indices_dashboard,
    get_currencies_dashboard
)

@app.get("/commodities-dashboard")
async def commodities_dashboard(futures: bool = False):
    cache_key = f"commodities_dashboard_{'futures' if futures else 'spot'}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    payload = await get_commodities_dashboard(is_futures=futures)
    cache.set(cache_key, payload, ttl=60)
    return payload

@app.get("/indices-dashboard")
async def indices_dashboard(futures: bool = False):
    cache_key = f"indices_dashboard_{'futures' if futures else 'spot'}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    payload = await get_indices_dashboard(is_futures=futures)
    cache.set(cache_key, payload, ttl=60)
    return payload

@app.get("/currencies-dashboard")
async def currencies_dashboard(futures: bool = False):
    cache_key = f"currencies_dashboard_{'futures' if futures else 'spot'}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    payload = await get_currencies_dashboard(is_futures=futures)
    cache.set(cache_key, payload, ttl=60)
    return payload



@app.get("/market-dashboard")
async def market_dashboard():
    """
    Full market dashboard: commodities, currencies, and indices.
    Uses Twelve Data free tier with caching (5-minute TTL).
    """
    cache_key = "market_dashboard"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    from commodities import fetch_market_dashboard
    data = await fetch_market_dashboard()
    cache.set(cache_key, data, ttl=60)
    return data
