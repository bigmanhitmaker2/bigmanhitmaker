import redis.asyncio as redis
from dotenv import load_dotenv
import os

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")
redis_client = redis.from_url(REDIS_URL)

async def get_credits(user_id: str) -> int:
    credits = await redis_client.get(user_id)
    return int(credits) if credits else 0

async def add_credits(user_id: str, amount: int):
    await redis_client.incrby(user_id, amount)

async def deduct_credit(user_id: str) -> bool:
    credits = await get_credits(user_id)
    if credits > 0:
        await redis_client.decr(user_id)
        return True
    return False
