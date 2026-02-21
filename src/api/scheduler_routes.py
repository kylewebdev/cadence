import logging

import redis.asyncio as aioredis
from fastapi import APIRouter

from config.settings import settings
from src.scheduler.queue import DLQ_KEY, QUEUE_KEY

logger = logging.getLogger(__name__)

router = APIRouter(tags=["scheduler"])


@router.get("/scheduler")
async def scheduler_health() -> dict:
    """Return queue depth, DLQ depth, and Redis connectivity status."""
    try:
        client = aioredis.Redis.from_url(
            settings.REDIS_URL, encoding="utf-8", decode_responses=True
        )
        queue_depth = await client.llen(QUEUE_KEY)
        dlq_depth = await client.llen(DLQ_KEY)
        await client.aclose()
        redis_ok = True
    except Exception as exc:
        logger.warning("Redis health check failed: %s", exc)
        queue_depth = -1
        dlq_depth = -1
        redis_ok = False

    return {
        "queue_depth": queue_depth,
        "dlq_depth": dlq_depth,
        "redis_ok": redis_ok,
    }
