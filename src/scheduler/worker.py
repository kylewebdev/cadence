import asyncio
import logging
from datetime import timedelta

from temporalio.client import Client, Schedule, ScheduleActionStartWorkflow, ScheduleIntervalSpec, ScheduleSpec
from temporalio.worker import Worker

from config.settings import settings
from src.scheduler.activities import (
    push_dlq_activity,
    query_due_agencies_activity,
    scrape_agency_activity,
)
from src.scheduler.workflows import MainIngestionWorkflow

logger = logging.getLogger(__name__)

TASK_QUEUE = "cadence-ingestion"
SCHEDULE_ID = "main-ingestion"


async def _ensure_schedule(client: Client) -> None:
    """Create the 15-minute ingestion schedule if it doesn't already exist."""
    try:
        await client.create_schedule(
            SCHEDULE_ID,
            Schedule(
                spec=ScheduleSpec(
                    intervals=[ScheduleIntervalSpec(every=timedelta(minutes=15))]
                ),
                action=ScheduleActionStartWorkflow(
                    MainIngestionWorkflow.run,
                    id=f"{SCHEDULE_ID}-scheduled",
                    task_queue=TASK_QUEUE,
                ),
            ),
        )
        logger.info("Created Temporal schedule '%s' (every 15 min).", SCHEDULE_ID)
    except Exception as exc:
        # Schedule already exists — ignore
        if "already exists" in str(exc).lower():
            logger.info("Schedule '%s' already registered.", SCHEDULE_ID)
        else:
            raise


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    client = await Client.connect(settings.TEMPORAL_HOST)
    await _ensure_schedule(client)

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[MainIngestionWorkflow],
        activities=[scrape_agency_activity, query_due_agencies_activity, push_dlq_activity],
        max_concurrent_activities=10,
    )
    logger.info("Worker started on task queue '%s'.", TASK_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
