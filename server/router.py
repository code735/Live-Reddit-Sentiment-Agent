from fastapi import APIRouter
from .routes import tickers

api_router = APIRouter()

api_router.include_router(
    tickers.router,
    prefix="/ticker",
    tags=["ticker"]
)