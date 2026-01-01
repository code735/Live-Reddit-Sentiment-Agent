from pydantic import BaseModel


class SentimentRequest(BaseModel):
    text: str


class SentimentResponse(BaseModel):
    label: str
    confidence: float
    scores: dict[str, float]
