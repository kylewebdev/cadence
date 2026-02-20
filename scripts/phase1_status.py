"""
scripts/phase1_status.py - Print Phase 1 completion summary (read-only).

Usage:
    python -m scripts.phase1_status
    # or
    python scripts/phase1_status.py
"""

import asyncio
import sys
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

# Allow running as a script from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import settings
from src.registry.models import Agency, AgencyFeed


async def main() -> None:
    engine = create_async_engine(settings.DATABASE_URL, echo=False)

    async with AsyncSession(engine) as session:
        # 1. Total agencies
        total = await session.scalar(select(func.count()).select_from(Agency))

        # 2. By agency_type
        rows = await session.execute(
            select(Agency.agency_type, func.count()).group_by(Agency.agency_type)
        )
        by_type = {str(r[0].value): r[1] for r in rows}

        # 3. By platform_type (sort by count desc in Python)
        rows = await session.execute(
            select(Agency.platform_type, func.count()).group_by(Agency.platform_type)
        )
        by_platform = {(r[0] or "null"): r[1] for r in rows}

        # 4. By region
        rows = await session.execute(
            select(Agency.region, func.count()).group_by(Agency.region)
        )
        by_region = {(r[0] or "(no region)"): r[1] for r in rows}

        # 5. Feed stats
        total_feeds = await session.scalar(select(func.count()).select_from(AgencyFeed))
        agencies_with_feeds = await session.scalar(
            select(func.count(func.distinct(AgencyFeed.agency_id)))
        )

    await engine.dispose()

    # 6. Data quality — missing platform / region
    no_platform = by_platform.get("null", 0) + by_platform.get("unknown", 0)
    no_region = by_region.get("(no region)", 0)

    # --- Output ---
    col = 30

    print("=== Cadence Phase 1 Status ===\n")
    print(f"Total agencies: {total}\n")

    print("Agency type breakdown:")
    for k, v in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"  {k + ':':<{col}} {v}")

    print("\nPlatform type breakdown (build parsers top-down):")
    for k, v in sorted(by_platform.items(), key=lambda x: -x[1]):
        print(f"  {k + ':':<{col}} {v}")

    print("\nRegion breakdown:")
    # Put "(no region)" last
    region_items = sorted(
        [(k, v) for k, v in by_region.items() if k != "(no region)"],
        key=lambda x: -x[1],
    )
    if "(no region)" in by_region:
        region_items.append(("(no region)", by_region["(no region)"]))
    for k, v in region_items:
        print(f"  {k + ':':<{col}} {v}")

    print("\nFeed stats:")
    print(f"  {'Total feeds:':<{col}} {total_feeds}")
    print(f"  {'Agencies with feeds:':<{col}} {agencies_with_feeds}")

    print("\nData quality flags:")
    print(f"  {'Missing platform (unknown/null):':<{col}} {no_platform} agencies")
    print(f"  {'Missing region:':<{col}} {no_region} agencies")


if __name__ == "__main__":
    asyncio.run(main())
