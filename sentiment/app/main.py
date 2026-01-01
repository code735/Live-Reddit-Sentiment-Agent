from fastapi import FastAPI
from .routes.sentiment import router as sentiment

app = FastAPI(title="reddit fin ai")

app.include_router(sentiment)


@app.get("/health")
async def health_check():
    return {"status": "ok"}
