from fastapi import FastAPI, APIRouter
from contextlib import asynccontextmanager
import asyncio
from .pw import run

@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = asyncio.Event()

    # start once
    task = asyncio.create_task(run(stop_event))


    # @app.get("/health")
    # def health():
    #     return {"status": "ok"}
    
    yield


    # shutdown
    stop_event.set()
    await task  # clean exit

app = FastAPI(lifespan=lifespan)
api_router = APIRouter()
