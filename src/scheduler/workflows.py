import asyncio
import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from src.scheduler.activities import (
        push_dlq_activity,
        query_due_agencies_activity,
        scrape_agency_activity,
    )

logger = logging.getLogger(__name__)

_ACTIVITY_TIMEOUT = timedelta(minutes=10)
_RETRY_POLICY = RetryPolicy(maximum_attempts=3)


@workflow.defn
class MainIngestionWorkflow:
    @workflow.run
    async def run(self) -> dict:
        # Fetch agencies due for scraping via a local activity (fast DB query)
        agency_ids: list[str] = await workflow.execute_local_activity(
            query_due_agencies_activity,
            schedule_to_close_timeout=timedelta(minutes=2),
        )

        workflow.logger.info("Agencies due for scraping: %d", len(agency_ids))

        total_feeds = 0
        total_docs = 0
        total_errors = 0

        sem = asyncio.Semaphore(10)

        async def run_one(agency_id: str) -> None:
            nonlocal total_feeds, total_docs, total_errors
            async with sem:
                try:
                    result: dict = await workflow.execute_activity(
                        scrape_agency_activity,
                        agency_id,
                        schedule_to_close_timeout=_ACTIVITY_TIMEOUT,
                        retry_policy=_RETRY_POLICY,
                    )
                    total_feeds += result.get("feeds_scraped", 0)
                    total_docs += result.get("docs_pushed", 0)
                    total_errors += len(result.get("errors", []))
                except ActivityError as exc:
                    workflow.logger.error(
                        "Activity failed for agency %s after retries: %s", agency_id, exc
                    )
                    total_errors += 1
                    await workflow.execute_local_activity(
                        push_dlq_activity,
                        args=[agency_id, str(exc)],
                        schedule_to_close_timeout=timedelta(minutes=1),
                    )

        await asyncio.gather(*[run_one(aid) for aid in agency_ids])

        return {
            "agencies_processed": len(agency_ids),
            "feeds_scraped": total_feeds,
            "docs_pushed": total_docs,
            "errors": total_errors,
        }
