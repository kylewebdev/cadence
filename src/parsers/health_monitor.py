"""
Health monitor: record per-run doc counts and detect unhealthy agencies.

An agency is UNHEALTHY when its 2 most recent parse runs both produced 0 docs.
An agency is MISSING when it has no parse runs in the last 48 hours.
"""
import logging
from datetime import datetime, timedelta

from sqlalchemy import select, text

from src.api.deps import AsyncSessionLocal
from src.registry.models import Agency, ParseRun

logger = logging.getLogger(__name__)

_UNHEALTHY_WINDOW_HOURS = 48
_MIN_CONSECUTIVE_ZERO_RUNS = 2


async def record_parse_run(
    agency_id: str,
    docs_fetched: int,
    feeds_scraped: int,
    error_count: int,
    platform_type: str | None = None,
) -> None:
    """Insert one ParseRun row. Opens its own session (independent commit)."""
    async with AsyncSessionLocal() as session:
        session.add(
            ParseRun(
                agency_id=agency_id,
                docs_fetched=docs_fetched,
                feeds_scraped=feeds_scraped,
                error_count=error_count,
                platform_type=platform_type,
            )
        )
        await session.commit()


async def get_unhealthy_agencies() -> list[dict]:
    """
    Return agencies that are MISSING (no run in last 48h) or UNHEALTHY
    (last 2 runs both produced 0 docs).

    Each dict has keys:
        agency_id, canonical_name, platform_type,
        last_run_at, docs_fetched_last, status
    """
    cutoff = datetime.utcnow() - timedelta(hours=_UNHEALTHY_WINDOW_HOURS)

    async with AsyncSessionLocal() as session:
        # --- Query 1: 48h summary (latest run per agency within window) ---
        summary_sql = text("""
            SELECT
                a.agency_id,
                a.canonical_name,
                a.platform_type,
                MAX(pr.run_at)       AS last_run_at,
                MAX(pr.docs_fetched) AS docs_fetched_last
            FROM agencies a
            LEFT JOIN parse_runs pr
                ON pr.agency_id = a.agency_id
               AND pr.run_at >= :cutoff
            WHERE a.platform_type IS NOT NULL
            GROUP BY a.agency_id, a.canonical_name, a.platform_type
        """)
        summary_rows = (await session.execute(summary_sql, {"cutoff": cutoff})).all()

        missing = {
            row.agency_id
            for row in summary_rows
            if row.last_run_at is None
        }

        # --- Query 2: Consecutive zeros CTE ---
        zeros_sql = text("""
            WITH ranked AS (
                SELECT
                    agency_id,
                    docs_fetched,
                    ROW_NUMBER() OVER (
                        PARTITION BY agency_id ORDER BY run_at DESC
                    ) AS rn
                FROM parse_runs
            ),
            recent AS (
                SELECT agency_id, docs_fetched
                FROM ranked
                WHERE rn <= :window
            )
            SELECT
                agency_id,
                COUNT(*)                                     AS run_count,
                SUM(CASE WHEN docs_fetched = 0 THEN 1 ELSE 0 END) AS zero_count
            FROM recent
            GROUP BY agency_id
            HAVING COUNT(*) >= :window
               AND SUM(CASE WHEN docs_fetched = 0 THEN 1 ELSE 0 END) >= :window
        """)
        zeros_rows = (
            await session.execute(zeros_sql, {"window": _MIN_CONSECUTIVE_ZERO_RUNS})
        ).all()

        unhealthy_ids = {row.agency_id for row in zeros_rows}

        results: list[dict] = []
        for row in summary_rows:
            if row.agency_id in missing:
                status = "MISSING"
            elif row.agency_id in unhealthy_ids:
                status = "UNHEALTHY"
            else:
                continue  # healthy — not included in problem list

            results.append(
                {
                    "agency_id": row.agency_id,
                    "canonical_name": row.canonical_name,
                    "platform_type": row.platform_type,
                    "last_run_at": row.last_run_at,
                    "docs_fetched_last": row.docs_fetched_last or 0,
                    "status": status,
                }
            )

    return results
