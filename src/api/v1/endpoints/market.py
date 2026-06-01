from typing import List
import httpx
from fastapi import APIRouter, HTTPException, Query
from src.core.config import settings
from src.schemas.schemas import SearchResult
from src.services.news_service import get_market_feed_async

router = APIRouter()

@router.get("/market-feed")
async def market_feed():
    return await get_market_feed_async()

@router.get("/search", response_model=List[SearchResult])
async def search_symbol(query: str = Query(...)):
    q = query.strip()
    if not q: return []

    if not settings.TWELVEDATA_API_KEY:
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
                results = []
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
            # Fall through to service unavailable if both fail
            pass
        raise HTTPException(status_code=500, detail="Price API key not configured and fallback failed")
    
    url = "https://api.twelvedata.com/symbol_search"
    params = {"symbol": q, "apikey": settings.TWELVEDATA_API_KEY}
    
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data") or []
        results = []
        for item in items:
            sym = item.get("symbol")
            name = item.get("instrument_name") or item.get("name") or ""
            if not sym or not name: continue
            results.append(SearchResult(symbol=sym, name=name, exchange=item.get("exchange"), type=item.get("instrument_type")))
        return results[:10]
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Search service unavailable")
