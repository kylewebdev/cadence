import time
from datetime import datetime

import feedparser

from src.parsers.base import BaseParser, RawDocument


class RSSParser(BaseParser):
    def __init__(self, agency_id: str) -> None:
        self.agency_id = agency_id

    async def fetch(self, url: str) -> list[RawDocument]:
        await self.rate_limit_delay()
        feed = feedparser.parse(url)
        if not feed.entries:
            return []

        feed_title = feed.feed.get("title", "")
        document_type = self._infer_document_type(feed_title)

        docs = []
        for entry in feed.entries:
            docs.append(
                RawDocument(
                    url=entry.get("link", url),
                    agency_id=self.agency_id,
                    document_type=document_type,
                    title=entry.get("title") or None,
                    raw_text=self._extract_content(entry),
                    published_date=self._parse_date(entry),
                    source_metadata={"feed_url": url, "feed_title": feed_title},
                )
            )
        return docs

    def _infer_document_type(self, feed_title: str) -> str:
        lower = feed_title.lower()
        if "press" in lower:
            return "press_release"
        if "arrest" in lower:
            return "arrest_log"
        return "activity_feed"

    def _parse_date(self, entry) -> datetime | None:
        for attr in ("published_parsed", "updated_parsed"):
            ts = getattr(entry, attr, None)
            if ts and isinstance(ts, time.struct_time):
                try:
                    return datetime(*ts[:6])
                except (ValueError, TypeError):
                    continue
        return None

    def _extract_content(self, entry) -> str:
        try:
            value = entry.content[0]["value"]
        except (AttributeError, IndexError, KeyError, TypeError):
            value = entry.get("summary", "")
        return self.clean_whitespace(value or "")
