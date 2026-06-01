import os
import re
import hashlib
import logging
import asyncio
import httpx
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import List, Dict, Any, Optional
from xml.etree import ElementTree as ET
from src.core.config import settings
from src.services.ai_service import enrich_news_batch, enrich_news_item

logger = logging.getLogger("sentinews.news")

_NEWS_CACHE = {"articles": [], "updated_at": None}

RSS_FEEDS = [
    ("ET Markets", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", "markets"),
    ("ET Stocks", "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms", "markets"),
    ("Moneycontrol Top", "https://www.moneycontrol.com/rss/MCtopnews.xml", "markets"),
    ("LiveMint Markets", "https://www.livemint.com/rss/markets", "markets"),
    ("The Hindu BL", "https://www.thehindubusinessline.com/markets/?service=rss", "markets"),
    ("NDTV Profit", "https://feeds.feedburner.com/ndtvprofit-latest", "markets"),
]

NAME_TO_TICKER = {
    "reliance": "RELIANCE", "tcs": "TCS", "infosys": "INFY", "wipro": "WIPRO",
    "hdfc": "HDFCBANK", "icici": "ICICIBANK", "sbi": "SBIN", "axis bank": "AXISBANK",
}

_TICKER_RE = re.compile(r'\b([A-Z]{2,10})\b')

def extract_tickers(text: str) -> List[str]:
    found = []
    lower = text.lower()
    for name, ticker in NAME_TO_TICKER.items():
        if ticker and name in lower and ticker not in found:
            found.append(ticker)
    for m in _TICKER_RE.finditer(text):
        sym = m.group(1)
        if 2 < len(sym) <= 10 and sym not in ("NSE","BSE","RBI","FII","DII","IPO"):
            if sym not in found: found.append(sym)
    return found[:8]

def _parse_pub_date(raw: Optional[str]) -> str:
    if not raw: return datetime.now(timezone.utc).isoformat()
    try: return parsedate_to_datetime(raw).astimezone(timezone.utc).isoformat()
    except: pass
    return datetime.now(timezone.utc).isoformat()

def _headline_hash(headline: str) -> str:
    return hashlib.md5(headline.lower().strip()[:80].encode()).hexdigest()

def _make_item(idx: int, source: str, headline: str, url: str, published_at: str, summary: str, category: str = "markets") -> Dict[str, Any]:
    tickers = extract_tickers(f"{headline} {summary}")
    return {
        "id": f"{source.lower().replace(' ','_')}_{idx}",
        "headline": headline.strip(),
        "source": source,
        "url": url,
        "published_at": published_at,
        "tickers": tickers,
        "summary": summary.strip(),
        "category": category,
        "sentiment": 0.0,
        "action": "neutral",
    }

async def _fetch_rss_async(client: httpx.AsyncClient, source_name: str, url: str, category: str) -> List[Dict[str, Any]]:
    try:
        resp = await client.get(url, timeout=12)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        items = []
        for i, item in enumerate(root.iter("item")):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_raw = item.findtext("pubDate") or item.findtext("dc:date", namespaces={"dc": "http://purl.org/dc/elements/1.1/"})
            desc = (item.findtext("description") or "").strip()
            desc = re.sub(r'<[^>]+>', ' ', desc).strip()
            desc = re.sub(r'\s+', ' ', desc)[:400]
            if len(title) > 10:
                items.append(_make_item(i, source_name, title, link, _parse_pub_date(pub_raw), desc, category))
        return items
    except Exception as exc:
        logger.warning(f"RSS fetch failed [{source_name}]: {exc}")
        return []

async def _fetch_newsapi_async(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    if not settings.NEWSAPI_KEY: return []
    try:
        resp = await client.get(
            "https://newsapi.org/v2/everything",
            params={"q": "NSE OR BSE OR Nifty", "language": "en", "sortBy": "publishedAt", "pageSize": 40},
            headers={"X-Api-Key": settings.NEWSAPI_KEY}, timeout=12
        )
        resp.raise_for_status()
        articles = resp.json().get("articles", [])
        return [_make_item(i, (a.get("source") or {}).get("name") or "NewsAPI", a.get("title") or "", a.get("url") or "", _parse_pub_date(a.get("publishedAt")), a.get("description") or "") for i, a in enumerate(articles) if a.get("title") and len(a.get("title")) > 10]
    except Exception as exc:
        logger.error(f"NewsAPI failed: {exc}")
        return []

async def get_market_feed_async(force_refresh: bool = False) -> Dict[str, Any]:
    global _NEWS_CACHE
    if not force_refresh and _NEWS_CACHE["articles"]:
        return {"headline_news": _NEWS_CACHE["articles"]}
    
    async with httpx.AsyncClient() as client:
        tasks = [_fetch_newsapi_async(client)]
        for name, url, cat in RSS_FEEDS:
            tasks.append(_fetch_rss_async(client, name, url, cat))
        results = await asyncio.gather(*tasks)
        all_articles = [item for r in results for item in r]
    
    seen = set()
    unique = []
    for art in all_articles:
        h = _headline_hash(art["headline"])
        if h not in seen:
            seen.add(h)
            unique.append(art)
    
    unique.sort(key=lambda x: x.get("published_at", ""), reverse=True)
    enriched = enrich_news_batch(unique)
    _NEWS_CACHE["articles"] = enriched
    _NEWS_CACHE["updated_at"] = datetime.now()
    return {"headline_news": enriched, "total": len(enriched)}
