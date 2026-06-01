# backend/news_scraper.py
#
# Multi-source Indian market news aggregator.
# Sources (in priority order):
#   1. NewsAPI (if key present)
#   2. ET Markets RSS
#   3. Zee Business RSS
#   4. Moneycontrol RSS
#   5. The Hindu BusinessLine RSS
#   6. NDTV Profit RSS
#   7. LiveMint RSS
#   8. Business Standard RSS
#
# All articles are normalised to the same internal schema and
# deduplicated by headline similarity before returning.

import os
import re
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import List, Dict, Any, Optional
from xml.etree import ElementTree as ET
import asyncio

import httpx
import asyncio
import requests
from dotenv import load_dotenv

import commodities

load_dotenv()
logger = logging.getLogger("sentinews.news")

# ── Global Cache ──
_NEWS_CACHE = {
    "articles": [],
    "updated_at": None
}

NEWSAPI_KEY          = os.getenv("NEWSAPI_KEY")
NEWSAPI_URL          = "https://newsapi.org/v2/everything"

NEWSAPI_PARAMS = {
    "q": (
        "NSE OR BSE OR Nifty OR Sensex OR RBI OR SEBI "
        "OR \"stock market\" OR \"share price\" OR \"mutual fund\" "
        "OR \"FII\" OR \"DII\" OR \"earnings\" OR \"results\" OR IPO"
    ),
    "language": "en",
    "sortBy":   "publishedAt",
    "pageSize": 40,
}

# ── RSS feeds — all free, no auth needed ──────────────────────────────────

RSS_FEEDS = [
    # (source_name, url, category)
    ("ET Markets",         "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",           "markets"),
    ("ET Stocks",          "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",       "markets"),
    ("ET Industry",        "https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms",            "markets"),
    ("ET Mutual Funds",    "https://economictimes.indiatimes.com/mf/rssfeeds/837555174.cms",                 "markets"),
    ("ET Economy",         "https://economictimes.indiatimes.com/economy/rssfeeds/1373380680.cms",           "macro"),
    ("Moneycontrol Top",   "https://www.moneycontrol.com/rss/MCtopnews.xml",                                 "markets"),
    ("Moneycontrol Biz",   "https://www.moneycontrol.com/rss/business.xml",                                  "markets"),
    ("Financial Express",  "https://www.financialexpress.com/market/feed/",                                  "markets"), # Check if this works or remove
    ("Rediff Business",    "https://www.rediff.com/rss/moneyrss.xml",                                        "markets"),
    ("LiveMint Markets",   "https://www.livemint.com/rss/markets",                                           "markets"),
    ("The Hindu BL",       "https://www.thehindubusinessline.com/markets/?service=rss",                      "markets"),
    # ("Business Standard",  "https://www.business-standard.com/rss/markets-106.rss",                          "markets"), # Returning 403 Forbidden
    # ("CNBC TV18",          "https://www.cnbctv18.com/commonfeeds/v1/cne/rest/api/v10/index/rss/market",      "markets"), # Returning 404 Not Found
    ("NDTV Profit",        "https://feeds.feedburner.com/ndtvprofit-latest",                                 "markets"),
    ("ET Commodities",     "https://economictimes.indiatimes.com/markets/commodities/rssfeeds/5114120.cms",  "commodities"),
    ("ET Forex",           "https://economictimes.indiatimes.com/markets/forex/rssfeeds/1150244.cms",        "currencies"),
]

REQUEST_TIMEOUT = 12
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml,application/xml,text/xml,*/*",
}

# ── Ticker extraction ─────────────────────────────────────────────────────

# Well-known Indian stock names → NSE ticker
NAME_TO_TICKER = {
    "reliance": "RELIANCE",   "tcs": "TCS",        "infosys": "INFY",
    "wipro": "WIPRO",         "hdfc": "HDFCBANK",  "icici": "ICICIBANK",
    "sbi": "SBIN",            "axis bank": "AXISBANK", "kotak": "KOTAKBANK",
    "larsen": "LT",           "l&t": "LT",          "bajaj": "BAJFINANCE",
    "maruti": "MARUTI",       "hul": "HINDUNILVR",  "itc": "ITC",
    "adani": "ADANIENT",      "tata": "TATAMOTORS", "ongc": "ONGC",
    "ntpc": "NTPC",           "sail": "SAIL",       "bhel": "BHEL",
    "hindalco": "HINDALCO",   "jswsteel": "JSWSTEEL", "ultratech": "ULTRACEMCO",
    "sun pharma": "SUNPHARMA", "cipla": "CIPLA",    "dr reddy": "DRREDDY",
    "airtel": "BHARTIARTL",   "vodafone": "IDEA",   "paytm": "PAYTM",
    "zomato": "ZOMATO",       "nykaa": "NYKAA",     "ola": "OLAELEC",
    "nifty": None,            "sensex": None,        "nse": None, "bse": None,
    "rbi": None,              "sebi": None,          "fii": None, "dii": None,
    "gst": None,              "bank": None,          "pnb": "PNB",
}

# Standalone uppercase tickers (3-10 chars) — rough filter
_TICKER_RE = re.compile(r'\b([A-Z]{2,10})\b')

def extract_tickers(text: str) -> List[str]:
    """Extract known NSE tickers from headline/summary via name and symbol matching."""
    found = []
    lower = text.lower()

    # name-based matching
    for name, ticker in NAME_TO_TICKER.items():
        if ticker and name in lower:
            if ticker not in found:
                found.append(ticker)

    # symbol-based (all-caps words that look like tickers)
    for m in _TICKER_RE.finditer(text):
        sym = m.group(1)
        if 2 < len(sym) <= 10 and sym not in ("NSE","BSE","RBI","FII","DII","IPO",
                                                 "CEO","CFO","CTO","AGM","EGM","MF",
                                                 "ETF","GDP","CPI","WPI","EMI","FDI",
                                                 "IEA","BCCI","LIVE","PNG","KYC","EPFO",
                                                 "EPF","HDFC","GST","USA","UK","EU",
                                                 "SEBI", "CBDT", "DBS", "IT", "BIT", "NET",
                                                 "NEW", "ALL", "SET", "OUT", "FOR", "BUY", "SELL"):
            if sym not in found:
                found.append(sym)

    return found[:8]  # cap at 8 tickers per article


# ── Time parsing ──────────────────────────────────────────────────────────

def _parse_pub_date(raw: Optional[str]) -> str:
    """Parse RSS pubDate or ISO string to UTC ISO string."""
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    # Try RFC2822 (RSS standard)
    try:
        return parsedate_to_datetime(raw).astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    # Try ISO
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(raw[:25], fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


# ── Deduplication ─────────────────────────────────────────────────────────

def _headline_hash(headline: str) -> str:
    """Rough dedup key — first 80 chars, lowercased."""
    return hashlib.md5(headline.lower().strip()[:80].encode()).hexdigest()


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


# ── Internal item schema ──────────────────────────────────────────────────

def _make_item(
    idx: int,
    source: str,
    headline: str,
    url: str,
    published_at: str,
    summary: str,
    category: str = "markets",
) -> Dict[str, Any]:
    tickers = extract_tickers(f"{headline} {summary}")
    published_at_ist = format_to_ist(published_at)
    return {
        "id":           f"{source.lower().replace(' ','_')}_{idx}",
        "headline":     headline.strip(),
        "source":       source,
        "url":          url,
        "published_at": published_at,
        "published_at_ist": published_at_ist,
        "tickers":      tickers,
        "summary":      summary.strip(),
        "category":     category,
        "sentiment":    0.0,
        "action":       "neutral",
    }


# ── RSS parser ────────────────────────────────────────────────────────────

async def _fetch_rss_async(client: httpx.AsyncClient, source_name: str, url: str, category: str) -> List[Dict[str, Any]]:
    """Fetch and parse a single RSS feed asynchronously."""
    try:
        resp = await client.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as exc:
        logger.warning("RSS fetch failed [%s]: %s", source_name, exc)
        return []

    items = []
    # Support both <channel><item> and direct <item> at root
    for i, item in enumerate(root.iter("item")):
        title     = (item.findtext("title") or "").strip()
        link      = (item.findtext("link") or "").strip()
        pub_raw   = item.findtext("pubDate") or item.findtext("dc:date", namespaces={"dc": "http://purl.org/dc/elements/1.1/"})
        desc      = (item.findtext("description") or "").strip()
        # Strip HTML tags from description
        desc = re.sub(r'<[^>]+>', ' ', desc).strip()
        desc = re.sub(r'\s+', ' ', desc)[:400]

        if not title or len(title) < 10:
            continue

        items.append(_make_item(
            idx=i,
            source=source_name,
            headline=title,
            url=link,
            published_at=_parse_pub_date(pub_raw),
            summary=desc,
            category=category,
        ))

    logger.info("RSS [%s]: fetched %d articles", source_name, len(items))
    return items


# ── NewsAPI ────────────────────────────────────────────────────────────────

async def _fetch_newsapi_async(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    if not NEWSAPI_KEY:
        return []
    try:
        resp = await client.get(
            NEWSAPI_URL,
            params=NEWSAPI_PARAMS,
            headers={"X-Api-Key": NEWSAPI_KEY},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        articles = resp.json().get("articles", [])
        return [
            _make_item(
                idx=i,
                source=(a.get("source") or {}).get("name") or "NewsAPI",
                headline=a.get("title") or "",
                url=a.get("url") or "",
                published_at=_parse_pub_date(a.get("publishedAt")),
                summary=a.get("description") or "",
            )
            for i, a in enumerate(articles)
            if a.get("title") and len(a.get("title", "")) > 10
        ]
    except Exception as exc:
        logger.error("NewsAPI failed: %s", exc)
        return []


# ── FII/DII placeholder ───────────────────────────────────────────────────

def get_fii_dii_snapshot() -> Optional[Dict[str, Any]]:
    """Returns None here — FII/DII data is fetched live in app.py."""
    return None


# ── Commodities mini-snapshot ─────────────────────────────────────────────

def get_commodities_snapshot_for_feed() -> Optional[Dict[str, Any]]:
    return None


# ── Main market feed ──────────────────────────────────────────────────────

async def get_market_feed_async(force_refresh: bool = False) -> Dict[str, Any]:
    """
    Parallelized async gather for all news sources.
    If force_refresh is False, serves from memory if < 5 mins old.
    """
    global _NEWS_CACHE
    now = datetime.now()

    if not force_refresh and _NEWS_CACHE["articles"]:
        return {"headline_news": _NEWS_CACHE["articles"]}

    async with httpx.AsyncClient(headers=REQUEST_HEADERS) as client:
        tasks = [_fetch_newsapi_async(client)]
        for source_name, url, category in RSS_FEEDS:
            tasks.append(_fetch_rss_async(client, source_name, url, category))

        results = await asyncio.gather(*tasks)
        all_articles = []
        for r in results:
            all_articles.extend(r)

    # Deduplicate
    seen: set = set()
    unique: List[Dict[str, Any]] = []
    for art in all_articles:
        h = _headline_hash(art["headline"])
        if h not in seen:
            seen.add(h)
            unique.append(art)

    unique.sort(key=lambda x: x.get("published_at", ""), reverse=True)

    # Ensure sentiment/action fields present
    for item in unique:
        item.setdefault("sentiment", 0.0)
        item.setdefault("action", "neutral")

    # Update cache
    _NEWS_CACHE["articles"] = unique
    _NEWS_CACHE["updated_at"] = now
    
    return {
        "headline_news": unique,
        "fii_dii":       None,
        "commodities":   None,
        "total":         len(unique),
    }

def get_market_feed() -> Dict[str, Any]:
    """Deprecated synchronous fallback: serves only from cache."""
    return {
        "headline_news": _NEWS_CACHE["articles"],
        "fii_dii":       None,
        "total":         len(_NEWS_CACHE["articles"]),
    }


# ── Daily Stocks In News ──────────────────────────────────────────────────

async def get_daily_stock_news(date_str: str) -> List[Dict[str, Any]]:
    """
    Returns specific company news for the given date (YYYY-MM-DD).
    Formats them roughly like: {"company": "TICKER", "news": "...", "url": "..."}
    """
    # Fetch latest or use cache
    feed = await get_market_feed_async()
    articles = feed.get("headline_news", [])
    
    # Filter by date
    filtered = []
    for art in articles:
        pub = art.get("published_at", "")
        if pub.startswith(date_str):
            filtered.append(art)
            
    # If no articles match the specific date and NewsAPI is available, we could fetch from NewsAPI
    if not filtered and NEWSAPI_KEY:
        try:
            async with httpx.AsyncClient(headers=REQUEST_HEADERS) as client:
                resp = await client.get(
                    NEWSAPI_URL,
                    params={
                        "q": "NSE OR BSE OR Nifty",
                        "language": "en",
                        "sortBy": "publishedAt",
                        "pageSize": 50,
                        "from": date_str,
                        "to": date_str
                    },
                    headers={"X-Api-Key": NEWSAPI_KEY},
                    timeout=REQUEST_TIMEOUT,
                )
                if resp.status_code == 200:
                    data = resp.json().get("articles", [])
                    for i, a in enumerate(data):
                        filtered.append(_make_item(
                            idx=i, source=(a.get("source") or {}).get("name") or "NewsAPI",
                            headline=a.get("title") or "", url=a.get("url") or "",
                            published_at=_parse_pub_date(a.get("publishedAt")),
                            summary=a.get("description") or "",
                        ))
        except Exception as exc:
            logger.error(f"NewsAPI historical fetch failed: {exc}")

    # Enrich with sentiment
    from ai_processor import enrich_news_batch
    enriched_filtered = enrich_news_batch(filtered)

    # Extract single ticker mapped to news
    daily_news = []
    seen_companies = set()
    
    for art in enriched_filtered:
        tickers = art.get("tickers", [])
        news_text = art.get("headline", "")
        # Try to clean up the description
        summary = art.get("summary", "")
        if summary and not summary.isspace():
            # If summary is too long, we keep it concise
            if len(summary) > 150:
                summary = summary[:147] + "..."
            news_text = f"{news_text} - {summary}"
            
        for ticker in tickers:
            if ticker not in seen_companies:
                seen_companies.add(ticker)
                daily_news.append({
                    "company": ticker,
                    "news": news_text,
                    "url": art.get("url"),
                    "source": art.get("source"),
                    "published_at": art.get("published_at"),
                    "published_at_ist": art.get("published_at_ist"),
                    "sentiment": art.get("sentiment", 0.0),
                    "action": art.get("action", "neutral")
                })
                # Max 1 breaking news per company in this digest to keep it scannable like NDTV page
                
    return daily_news[:30] # Limit to 30 items



# ── Commodities dashboard ─────────────────────────────────────────────────

async def _fetch_category_news(query: str, category: str) -> List[Dict[str, Any]]:
    # Ensure global cache is warmed up
    global _NEWS_CACHE
    if not _NEWS_CACHE["articles"]:
        logger.info(f"Cache empty. Warming up news cache for category: {category}")
        await get_market_feed_async()

    cached_articles = _NEWS_CACHE["articles"]
    keywords = query.lower().split(" or ")
    
    # Filter articles matching category or keywords
    matched = []
    seen_urls = set()
    for art in cached_articles:
        url = art.get("url")
        if url in seen_urls:
            continue
            
        is_match = False
        if art.get("category") == category:
            is_match = True
        else:
            text = (art.get("headline", "") + " " + art.get("summary", "")).lower()
            if any(k.strip() in text for k in keywords):
                is_match = True
        
        if is_match:
            seen_urls.add(url)
            matched.append(art)
            
    # Sort by relevance to query
    def relevance_score(n):
        headline_lower = n.get("headline", "").lower()
        summary_lower = n.get("summary", "").lower()
        count = sum(1 for k in keywords if k.strip() in headline_lower)
        return count * 10 + (1 if any(k.strip() in summary_lower for k in keywords) else 0)
        
    matched.sort(key=lambda x: (relevance_score(x), x.get("published_at", "")), reverse=True)
    return matched[:40]

def _fetch_rss_sync(source, rss_url, cat):
    try:
        r = requests.get(rss_url, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS)
        r.raise_for_status()
        rt = ET.fromstring(r.content)
        res = []
        for j, item in enumerate(rt.iter("item")):
            t = (item.findtext("title") or "").strip()
            l = (item.findtext("link") or "").strip()
            p = item.findtext("pubDate") or item.findtext("dc:date", namespaces={"dc": "http://purl.org/dc/elements/1.1/"})
            d = (item.findtext("description") or "").strip()
            d = re.sub(r'<[^>]+>', ' ', d).strip()
            d = re.sub(r'\s+', ' ', d)[:400]
            if t and len(t) > 10:
                res.append(_make_item(j, source, t, l, _parse_pub_date(p), d, cat))
        return res
    except Exception as e:
        logger.warning(f"Sync RSS fetch failed [{source}]: {e}")
        return []

async def get_commodities_dashboard(is_futures: bool = False, force_refresh: bool = False) -> Dict[str, Any]:
    market_data = await commodities.fetch_market_dashboard(is_futures=is_futures, force_refresh=force_refresh)
    snapshot = {
        "date":        market_data["last_updated"],
        "commodities": market_data["commodities"],
        "is_futures":  market_data.get("is_futures", False),
    }
    news = await _fetch_category_news("crude oil OR gold OR silver OR copper OR natural gas OR wheat OR commodity", "commodities")
    return {"snapshot": snapshot, "news": news}

async def get_indices_dashboard(is_futures: bool = False, force_refresh: bool = False) -> Dict[str, Any]:
    market_data = await commodities.fetch_market_dashboard(is_futures=is_futures, force_refresh=force_refresh)
    snapshot = {
        "date":        market_data["last_updated"],
        "indices":     market_data["indices"],
        "is_futures":  market_data.get("is_futures", False),
    }
    news = await _fetch_category_news("nifty 50 OR sensex OR stock market index OR dow jones OR nasdaq OR sp500", "indices")
    return {"snapshot": snapshot, "news": news}

async def get_currencies_dashboard(is_futures: bool = False, force_refresh: bool = False) -> Dict[str, Any]:
    market_data = await commodities.fetch_market_dashboard(is_futures=is_futures, force_refresh=force_refresh)
    snapshot = {
        "date":        market_data["last_updated"],
        "currencies":  market_data["currencies"],
        "is_futures":  market_data.get("is_futures", False),
    }
    news = await _fetch_category_news("forex OR usdinr OR eurusd OR gbpusd OR currency market OR dollar index", "currencies")
    return {"snapshot": snapshot, "news": news}
