from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
import logging
from pw import run 

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Starting Pathway in background...")
    asyncio.create_task(asyncio.to_thread(run)) 
    yield
    logging.info("Shutdown complete")

app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health():
    return {"status": "ok"}
