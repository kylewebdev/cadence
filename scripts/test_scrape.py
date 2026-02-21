#!/usr/bin/env python
"""Run a single-agency scrape synchronously — no Temporal, no worker required.

Usage:
    python scripts/test_scrape.py --agency-id alhambra-pd
    python scripts/test_scrape.py --agency-id alhambra-pd --dry-run
"""
import argparse
import asyncio
import dataclasses
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from src.api.deps import AsyncSessionLocal
from src.dedup.deduplicator import Deduplicator
from src.parsers.base import RawDocument
from src.registry.models import Agency
from src.scheduler.parser_registry import get_parser
from src.scheduler.queue import ProcessingQueue
from src.scheduler.rate_limiter import DomainRateLimiter

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")
logger = logging.getLogger("test_scrape")

_FREQUENCY_TTL: dict[str, int] = {
    "realtime": 900,
    "hourly": 3600,
    "daily": 86400,
    "weekly": 604800,
}


def _serialize_doc(doc: RawDocument) -> dict:
    d = dataclasses.asdict(doc)
    for k, v in d.items():
        if hasattr(v, "isoformat"):
            d[k] = v.isoformat()
    return d


async def run(agency_id: str, dry_run: bool) -> None:
    queue = ProcessingQueue()
    dedup = Deduplicator()
    rate_limiter = DomainRateLimiter()

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Agency)
            .where(Agency.agency_id == agency_id)
            .options(selectinload(Agency.feeds))
        )
        agency = result.scalar_one_or_none()

    if agency is None:
        logger.error("Agency not found: %s", agency_id)
        sys.exit(1)

    if not agency.platform_type:
        logger.error("Agency %s has no platform_type configured.", agency_id)
        sys.exit(1)

    parser = get_parser(agency)
    if parser is None:
        logger.error("No parser registered for platform_type=%r", agency.platform_type)
        sys.exit(1)

    logger.info(
        "Scraping agency=%s  platform=%s  feeds=%d  dry_run=%s",
        agency_id,
        agency.platform_type,
        len(agency.feeds),
        dry_run,
    )

    ttl_seconds = _FREQUENCY_TTL.get(agency.scrape_frequency, 86400)
    ttl_hours = ttl_seconds / 3600
    feeds_scraped = 0
    docs_total = 0
    docs_pushed = 0

    active_feeds = [f for f in agency.feeds if f.is_active]
    if not active_feeds:
        logger.warning("No active feeds for agency %s", agency_id)

    for feed in active_feeds:
        logger.info("Fetching feed: %s", feed.url)
        await rate_limiter.acquire(feed.url)
        try:
            docs: list[RawDocument] = await parser.fetch(feed.url)
        except Exception as exc:
            logger.error("Error fetching %s: %s", feed.url, exc)
            continue

        feeds_scraped += 1
        docs_total += len(docs)
        logger.info("  Got %d documents", len(docs))

        for doc in docs:
            if dry_run:
                print(json.dumps(_serialize_doc(doc), indent=2, default=str))
                docs_pushed += 1
            else:
                is_dup = await dedup.is_duplicate(doc)
                if not is_dup:
                    await queue.push(doc)
                    await dedup.mark_seen(doc)
                    docs_pushed += 1

    logger.info(
        "Done. feeds_scraped=%d  docs_total=%d  docs_pushed=%d",
        feeds_scraped,
        docs_total,
        docs_pushed,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Test scrape a single agency.")
    parser.add_argument("--agency-id", required=True, help="Agency ID slug (e.g. alhambra-pd)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print documents to stdout instead of pushing to Redis queue.",
    )
    args = parser.parse_args()
    asyncio.run(run(args.agency_id, args.dry_run))


if __name__ == "__main__":
    main()
