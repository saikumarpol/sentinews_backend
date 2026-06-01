# backend/routers/daily_news.py
import logging
from fastapi import APIRouter, Query, HTTPException
from typing import List, Dict, Any
from datetime import datetime

from news_scraper import get_daily_stock_news

logger = logging.getLogger("sentinews.daily_news")
router = APIRouter(prefix="/market", tags=["daily-news"])

@router.get("/daily-news", summary="Get specific Indian stock news for a given date")
async def fetch_daily_stock_news(
    date: str = Query(..., description="Date in YYYY-MM-DD format")
) -> List[Dict[str, Any]]:
    """
    Returns a list of specific company news points for the given date, formatted
    similarly to 'Company: News snippet'.
    """
    try:
        # Validate date format roughly
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    try:
        news_items = await get_daily_stock_news(date)
        return news_items
    except Exception as exc:
        logger.error(f"Error fetching daily stock news for {date}: {exc}")
        raise HTTPException(status_code=500, detail="Internal server error fetching news")
