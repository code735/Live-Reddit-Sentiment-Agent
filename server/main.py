from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
from pw import run

@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = asyncio.Event()

    # start once
    task = asyncio.create_task(run(stop_event))

    yield  # server is running

    # shutdown
    stop_event.set()
    await task  # clean exit

app = FastAPI(lifespan=lifespan)
