import asyncio
import random
from urllib.parse import urlparse

import redis.asyncio as aioredis

from config.settings import settings

_MIN_DELAY = 2
_MAX_DELAY = 5


class DomainRateLimiter:
    """
    Per-domain politeness enforcer using Redis TTL keys.

    Before each fetch, checks cadence:ratelimit:{domain}. If the key
    exists, sleeps the remaining TTL. Then sets the key with a new
    random jitter TTL (2–5 s) so the next caller waits.
    """

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None

    async def _client(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = aioredis.Redis.from_url(
                settings.REDIS_URL, encoding="utf-8", decode_responses=True
            )
        return self._redis

    async def acquire(self, url: str) -> None:
        domain = urlparse(url).netloc
        key = f"cadence:ratelimit:{domain}"
        client = await self._client()

        ttl = await client.ttl(key)
        if ttl > 0:
            await asyncio.sleep(ttl)

        delay = random.randint(_MIN_DELAY, _MAX_DELAY)
        await client.set(key, "1", ex=delay)
