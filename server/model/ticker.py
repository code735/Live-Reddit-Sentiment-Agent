from pydantic import BaseModel

class crawlerConfig(BaseModel):
  subreddit: str
  limit: float