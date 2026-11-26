import asyncio


async def fetch(queue, timeout: float = 5.0):
    """Poll a queue until a message shows up (basic.get does not block)."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        message = await queue.get(fail=False)
        if message is not None:
            return message
        await asyncio.sleep(0.05)
    return None
