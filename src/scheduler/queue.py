import dataclasses
import json
import logging
from datetime import datetime

import redis.asyncio as aioredis

from config.settings import settings
from src.parsers.base import RawDocument

logger = logging.getLogger(__name__)

QUEUE_KEY = "cadence:processing"
DLQ_KEY = "cadence:dlq"


def _serialize(doc: RawDocument) -> str:
    """Serialize RawDocument to JSON, converting datetime fields to ISO-8601."""
    d = dataclasses.asdict(doc)
    for key, val in d.items():
        if isinstance(val, datetime):
            d[key] = val.isoformat()
    return json.dumps(d)


def _deserialize(raw: str) -> RawDocument:
    """Deserialize JSON string back to RawDocument."""
    d = json.loads(raw)
    if d.get("published_date"):
        d["published_date"] = datetime.fromisoformat(d["published_date"])
    return RawDocument(**d)


class ProcessingQueue:
    """
    Redis LPUSH queue for raw documents awaiting Phase 3 processing.

    Phase 3 workers consume via BRPOP cadence:processing.
    Can be upgraded to full BullMQ job format when Phase 3 is built.
    """

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None

    async def _client(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = aioredis.Redis.from_url(
                settings.REDIS_URL, encoding="utf-8", decode_responses=True
            )
        return self._redis

    async def push(self, doc: RawDocument) -> None:
        client = await self._client()
        await client.lpush(QUEUE_KEY, _serialize(doc))

    async def push_dlq(self, agency_id: str, error: str) -> None:
        client = await self._client()
        payload = json.dumps({"agency_id": agency_id, "error": error, "ts": datetime.utcnow().isoformat()})
        await client.lpush(DLQ_KEY, payload)

    async def depth(self) -> int:
        client = await self._client()
        return await client.llen(QUEUE_KEY)

    async def dlq_depth(self) -> int:
        client = await self._client()
        return await client.llen(DLQ_KEY)

    async def pop_all(self) -> list[RawDocument]:
        """Non-blocking drain: RPOP until queue is empty. Returns all docs."""
        client = await self._client()
        docs = []
        while True:
            raw = await client.rpop(QUEUE_KEY)
            if raw is None:
                break
            try:
                docs.append(_deserialize(raw))
            except Exception:
                logger.warning("Failed to deserialize queue item; skipping.")
        return docs
