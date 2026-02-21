import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from temporalio import activity

from src.api.deps import AsyncSessionLocal
from src.dedup.deduplicator import Deduplicator
from src.parsers.health_monitor import record_parse_run
from src.registry.models import Agency, AgencyFeed
from src.scheduler.parser_registry import get_parser
from src.scheduler.queue import ProcessingQueue
from src.scheduler.rate_limiter import DomainRateLimiter

logger = logging.getLogger(__name__)

# Seconds per frequency label (used to compute dedup TTL)
_FREQUENCY_TTL: dict[str, int] = {
    "realtime": 900,
    "hourly": 3600,
    "daily": 86400,
    "weekly": 604800,
}

_queue = ProcessingQueue()
_dedup = Deduplicator()
_rate_limiter = DomainRateLimiter()


@activity.defn
async def scrape_agency_activity(agency_id: str) -> dict:
    """
    Scrape all active feeds for one agency.

    Returns {"feeds_scraped": int, "docs_pushed": int, "errors": list[str]}
    """
    feeds_scraped = 0
    docs_pushed = 0
    errors: list[str] = []

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Agency)
            .where(Agency.agency_id == agency_id)
            .options(selectinload(Agency.feeds))
        )
        agency = result.scalar_one_or_none()

        if agency is None:
            logger.warning("Agency not found: %s", agency_id)
            return {"feeds_scraped": 0, "docs_pushed": 0, "errors": [f"Agency not found: {agency_id}"]}

        if not agency.platform_type:
            logger.warning("Agency %s has no platform_type; skipping.", agency_id)
            return {"feeds_scraped": 0, "docs_pushed": 0, "errors": []}

        parser = get_parser(agency)
        if parser is None:
            logger.warning("No parser for platform_type=%r (agency=%s)", agency.platform_type, agency_id)
            return {"feeds_scraped": 0, "docs_pushed": 0, "errors": []}

        ttl_seconds = _FREQUENCY_TTL.get(agency.scrape_frequency, 86400)
        ttl_hours = ttl_seconds / 3600

        active_feeds: list[AgencyFeed] = [f for f in agency.feeds if f.is_active]

        for feed in active_feeds:
            try:
                if not feed.url.startswith(("http://", "https://")):
                    logger.debug("Skipping feed with invalid URL %r for agency %s", feed.url, agency_id)
                    continue

                if await _dedup.url_recently_fetched(feed.url, ttl_hours=int(ttl_hours)):
                    logger.debug("Skipping recently fetched feed %s", feed.url)
                    continue

                await _rate_limiter.acquire(feed.url)

                docs = await parser.fetch(feed.url)
                feeds_scraped += 1

                for doc in docs:
                    if not await _dedup.is_duplicate(doc):
                        await _queue.push(doc)
                        await _dedup.mark_seen(doc)
                        docs_pushed += 1

                await _dedup.mark_url_fetched(feed.url, ttl_hours=int(ttl_hours))

                feed.last_scraped = datetime.utcnow()
                feed.last_successful = datetime.utcnow()
                feed.last_error = None

            except Exception as exc:
                err_msg = str(exc)
                logger.error("Error scraping feed %s for agency %s: %s", feed.url, agency_id, err_msg)
                feed.last_error = err_msg
                errors.append(err_msg)

        await session.commit()

    try:
        await record_parse_run(
            agency_id=agency_id,
            docs_fetched=docs_pushed,
            feeds_scraped=feeds_scraped,
            error_count=len(errors),
            platform_type=agency.platform_type,
        )
    except Exception:
        logger.exception("Failed to record parse run for %s", agency_id)
    # Return regardless — health recording failure must not mask scrape result
    return {"feeds_scraped": feeds_scraped, "docs_pushed": docs_pushed, "errors": errors}


@activity.defn
async def query_due_agencies_activity() -> list[str]:
    """
    Return agency_ids for all agencies with at least one active feed due for scraping.
    """
    from datetime import timedelta
    from sqlalchemy import or_, and_

    _INTERVALS: dict[str, timedelta] = {
        "realtime": timedelta(minutes=15),
        "hourly": timedelta(hours=1),
        "daily": timedelta(hours=24),
        "weekly": timedelta(days=7),
    }

    now = datetime.utcnow()
    due_agency_ids: set[str] = set()

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Agency)
            .where(Agency.platform_type.isnot(None))
            .options(selectinload(Agency.feeds))
        )
        agencies = result.scalars().all()

        for agency in agencies:
            interval = _INTERVALS.get(agency.scrape_frequency, timedelta(hours=24))
            cutoff = now - interval
            for feed in agency.feeds:
                if not feed.is_active:
                    continue
                if feed.last_scraped is None or feed.last_scraped < cutoff:
                    due_agency_ids.add(agency.agency_id)
                    break

    return list(due_agency_ids)


@activity.defn
async def push_dlq_activity(agency_id: str, error: str) -> None:
    """Push a failure record to the dead-letter queue."""
    await _queue.push_dlq(agency_id, error)
