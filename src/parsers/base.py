import asyncio
import hashlib
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.registry.models import Agency


@dataclass
class RawDocument:
    url: str
    agency_id: str
    document_type: str  # mirrors FeedType values as strings ("rss_feed", "arrest_log", etc.)
    title: str | None
    raw_text: str
    published_date: datetime | None
    source_metadata: dict = field(default_factory=dict)


# Global semaphore: cap concurrent Playwright browser instances to avoid
# resource exhaustion when many activities run simultaneously.
PLAYWRIGHT_SEMAPHORE: asyncio.Semaphore = asyncio.Semaphore(3)


class BaseParser(ABC):
    DEFAULT_RATE_LIMIT_SECONDS: float = 1.0

    @abstractmethod
    async def fetch(self, url: str) -> list[RawDocument]:
        ...

    async def rate_limit_delay(self, seconds: float | None = None) -> None:
        await asyncio.sleep(seconds if seconds is not None else self.DEFAULT_RATE_LIMIT_SECONDS)

    def hash_document(self, doc: RawDocument) -> str:
        payload = f"{doc.url}:{doc.raw_text}"
        return hashlib.sha256(payload.encode()).hexdigest()

    def clean_whitespace(self, text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    async def get_agency(self, agency_id: str, db: AsyncSession) -> Agency | None:
        result = await db.execute(select(Agency).where(Agency.agency_id == agency_id))
        return result.scalar_one_or_none()
