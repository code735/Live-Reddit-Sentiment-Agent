from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
from pw import run

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(run())

    yield  # 3️⃣ server is now ready and accepting requests

    # (optional) shutdown logic here

app = FastAPI(lifespan=lifespan)
