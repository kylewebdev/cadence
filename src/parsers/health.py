"""
CLI health check for the parser pipeline.

Usage:
    python -m src.parsers.health check

Classifies every parseable agency as HEALTHY, UNHEALTHY, or MISSING.
Problem rows (UNHEALTHY/MISSING) are printed first.
Exits 1 if subcommand is not 'check'.
"""
import asyncio
import sys
from datetime import datetime

from sqlalchemy import text

from src.api.deps import AsyncSessionLocal


async def _run_check() -> None:
    """Single-query health check using DISTINCT ON + consecutive-zeros CTE."""
    sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (agency_id)
                agency_id,
                run_at,
                docs_fetched,
                platform_type
            FROM parse_runs
            ORDER BY agency_id, run_at DESC
        ),
        ranked AS (
            SELECT
                agency_id,
                docs_fetched,
                ROW_NUMBER() OVER (
                    PARTITION BY agency_id ORDER BY run_at DESC
                ) AS rn
            FROM parse_runs
        ),
        recent_two AS (
            SELECT agency_id, docs_fetched
            FROM ranked
            WHERE rn <= 2
        ),
        zero_runs AS (
            SELECT
                agency_id,
                COUNT(*)                                          AS run_count,
                SUM(CASE WHEN docs_fetched = 0 THEN 1 ELSE 0 END) AS zero_count
            FROM recent_two
            GROUP BY agency_id
            HAVING COUNT(*) >= 2
               AND SUM(CASE WHEN docs_fetched = 0 THEN 1 ELSE 0 END) >= 2
        )
        SELECT
            a.agency_id,
            a.canonical_name,
            a.platform_type,
            l.run_at        AS last_run_at,
            l.docs_fetched  AS docs_fetched_last,
            CASE
                WHEN l.agency_id IS NULL         THEN 'MISSING'
                WHEN z.agency_id IS NOT NULL      THEN 'UNHEALTHY'
                ELSE                                   'HEALTHY'
            END AS status
        FROM agencies a
        LEFT JOIN latest l      ON l.agency_id = a.agency_id
        LEFT JOIN zero_runs z   ON z.agency_id = a.agency_id
        WHERE a.platform_type IS NOT NULL
        ORDER BY
            CASE
                WHEN l.agency_id IS NULL    THEN 0
                WHEN z.agency_id IS NOT NULL THEN 1
                ELSE                              2
            END,
            a.agency_id
    """)

    async with AsyncSessionLocal() as session:
        rows = (await session.execute(sql)).all()

    if not rows:
        print("No parseable agencies found.")
        return

    # Column widths
    C1, C2, C3, C4, C5, C6 = 34, 32, 14, 17, 5, 9
    sep = "-" * (C1 + C2 + C3 + C4 + C5 + C6 + 5)

    header = (
        "AGENCY_ID".ljust(C1)
        + "NAME".ljust(C2)
        + "PLATFORM".ljust(C3)
        + "LAST_RUN".ljust(C4)
        + "DOCS".rjust(C5)
        + "  STATUS"
    )

    print(sep)
    print(header)
    print(sep)

    counts = {"HEALTHY": 0, "UNHEALTHY": 0, "MISSING": 0}

    for row in rows:
        status = row.status
        counts[status] = counts.get(status, 0) + 1

        last_run = (
            row.last_run_at.strftime("%Y-%m-%d %H:%M")
            if isinstance(row.last_run_at, datetime)
            else "never"
        )
        docs = str(row.docs_fetched_last or 0)
        platform = row.platform_type or ""

        line = (
            str(row.agency_id).ljust(C1)
            + str(row.canonical_name).ljust(C2)
            + platform.ljust(C3)
            + last_run.ljust(C4)
            + docs.rjust(C5)
            + "  "
            + status
        )
        print(line)

    print(sep)
    total = sum(counts.values())
    print(
        f"Total: {total}  "
        f"Healthy: {counts.get('HEALTHY', 0)}  "
        f"Unhealthy: {counts.get('UNHEALTHY', 0)}  "
        f"Missing: {counts.get('MISSING', 0)}"
    )


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] != "check":
        print("Usage: python -m src.parsers.health check", file=sys.stderr)
        sys.exit(1)
    asyncio.run(_run_check())


if __name__ == "__main__":
    main()
