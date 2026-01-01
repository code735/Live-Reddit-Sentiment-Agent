import pathway as pw

class RedditSentimentSchema(pw.Schema):
    event_id: str
    post_id: str
    event_type: str
    ticker: str
    text: str
    sentiment: str
    confidence: float
    observed_at: str   # ISO-8601 string (safe & simple)
