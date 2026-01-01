from fastapi import APIRouter
from ..schemas.sentiment import SentimentRequest, SentimentResponse
from ..services.sentiment import SentimentService

router = APIRouter(prefix="/sentiment", tags=["Sentiment"])

# shared service

sentiment_service = SentimentService()


@router.post("/", response_model=SentimentResponse)
def analyze_sentiment(payload: SentimentRequest):
    return sentiment_service.analyze(payload.text)
