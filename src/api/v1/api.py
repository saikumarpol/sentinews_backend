from fastapi import APIRouter
from src.api.v1.endpoints import auth, market, portfolio, stocks, market_data, reports, screener, daily_news

api_router = APIRouter()
api_router.include_router(auth.router, tags=["auth"])
api_router.include_router(market.router, tags=["market"])
api_router.include_router(portfolio.router, prefix="/portfolio", tags=["portfolio"])
api_router.include_router(stocks.router, prefix="/stocks", tags=["stocks"])
api_router.include_router(market_data.router, tags=["indian-market"])
api_router.include_router(reports.router, tags=["reports"])
api_router.include_router(screener.router, tags=["screener"])
api_router.include_router(daily_news.router, tags=["daily-news"])
