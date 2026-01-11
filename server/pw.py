import asyncio
from pathway_streams.main import start_pawthway

async def run(stop_event: asyncio.Event):
    print("pathway started")
    start_pawthway()
    await stop_event.wait()

    print("pathway stopping")
