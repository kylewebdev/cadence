import hashlib
import logging
from datetime import datetime, timedelta

import redis.asyncio as aioredis

from config.settings import settings
from src.parsers.base import RawDocument

logger = logging.getLogger(__name__)

_HASH_SET_KEY = "cadence:seen_hashes"
_URL_KEY_PREFIX = "cadence:url:"


class _InMemoryFallback:
    def __init__(self) -> None:
        self._hashes: set[str] = set()
        self._url_expiry: dict[str, datetime] = {}

    def hash_seen(self, hash_val: str) -> bool:
        return hash_val in self._hashes

    def add_hash(self, hash_val: str) -> None:
        self._hashes.add(hash_val)

    def url_seen(self, url_key: str) -> bool:
        expiry = self._url_expiry.get(url_key)
        if expiry is None:
            return False
        return datetime.utcnow() < expiry

    def mark_url(self, url_key: str, ttl_hours: int) -> None:
        self._url_expiry[url_key] = datetime.utcnow() + timedelta(hours=ttl_hours)


class Deduplicator:
    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        self._fallback: _InMemoryFallback | None = None
        self._initialized: bool = False

    async def _ensure_connected(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        try:
            client = aioredis.Redis.from_url(
                settings.REDIS_URL, encoding="utf-8", decode_responses=True
            )
            await client.ping()
            self._redis = client
        except Exception:
            logger.warning(
                "Redis unavailable at %s; falling back to in-memory deduplication.",
                settings.REDIS_URL,
            )
            self._fallback = _InMemoryFallback()

    @staticmethod
    def _compute_hash(doc: RawDocument) -> str:
        return hashlib.sha256(f"{doc.url}:{doc.raw_text}".encode()).hexdigest()

    @staticmethod
    def _url_key(url: str) -> str:
        return f"{_URL_KEY_PREFIX}{hashlib.sha256(url.encode()).hexdigest()}"

    async def is_duplicate(self, doc: RawDocument) -> bool:
        await self._ensure_connected()
        hash_val = self._compute_hash(doc)
        if self._redis is not None:
            return bool(await self._redis.sismember(_HASH_SET_KEY, hash_val))
        return self._fallback.hash_seen(hash_val)  # type: ignore[union-attr]

    async def mark_seen(self, doc: RawDocument) -> None:
        await self._ensure_connected()
        hash_val = self._compute_hash(doc)
        if self._redis is not None:
            await self._redis.sadd(_HASH_SET_KEY, hash_val)
        else:
            self._fallback.add_hash(hash_val)  # type: ignore[union-attr]

    async def url_recently_fetched(self, url: str, ttl_hours: int = 24) -> bool:
        await self._ensure_connected()
        key = self._url_key(url)
        if self._redis is not None:
            return bool(await self._redis.exists(key))
        return self._fallback.url_seen(key)  # type: ignore[union-attr]

    async def mark_url_fetched(self, url: str, ttl_hours: int = 24) -> None:
        await self._ensure_connected()
        key = self._url_key(url)
        if self._redis is not None:
            await self._redis.set(key, "1", ex=ttl_hours * 3600)
        else:
            self._fallback.mark_url(key, ttl_hours)  # type: ignore[union-attr]
