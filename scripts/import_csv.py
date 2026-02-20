"""
scripts/import_csv.py - Import CA law enforcement agencies from CSV into Postgres.

Usage:
    python -m scripts.import_csv
    # or
    python scripts/import_csv.py
"""

import asyncio
import csv
import re
import sys
from pathlib import Path

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

# Allow running as a script from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import settings
from src.registry.models import Agency, AgencyFeed, AgencyType, FeedType

CSV_PATH = Path(__file__).parent.parent / "ca_law_enforcement_websites.csv"


def make_slug(name: str) -> str:
    s = name.lower()
    s = s.replace("\u2019", "").replace("'", "")
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    s = re.sub(r"-+", "-", s)
    return s


def detect_agency_type(name: str) -> AgencyType:
    if "Sheriff" in name:
        return AgencyType.sheriff
    if "District Attorney" in name or re.search(r"\bDA\b", name):
        return AgencyType.district_attorney
    if "Coroner" in name:
        return AgencyType.coroner
    if any(k in name for k in ("University", "College", "Campus", "School District")):
        return AgencyType.campus
    if any(k in name for k in ("Transit", "BART", "Amtrak")):
        return AgencyType.transit
    if "Highway Patrol" in name or "CHP" in name or name.startswith("State "):
        return AgencyType.state
    return AgencyType.municipal_pd


def detect_county(name: str) -> str | None:
    m = re.search(r"^([A-Za-z\s]+?)\s+County", name)
    return m.group(1).strip() if m else None


def detect_platform_type(url: str, notes: str) -> str:
    combined = (url + " " + notes).lower()
    if "citizenrims" in combined:
        return "citizenrims"
    if "crimegraphics" in combined:
        return "crimegraphics"
    if "crimemapping.com" in combined:
        return "crimemapping"
    if "arcgis" in combined:
        return "arcgis"
    if "nixle" in combined:
        return "nixle"
    if re.search(r"data\.[^/]*\.(gov|org)", combined):
        return "socrata"
    return "unknown"


def detect_feed_type(data_types: str) -> FeedType:
    lowered = data_types.lower()
    if "arrest" in lowered:
        return FeedType.arrest_log
    if "incident" in lowered:
        return FeedType.incident_reports
    if "crime map" in lowered:
        return FeedType.crimemapping_embed
    if "activity" in lowered:
        return FeedType.daily_activity_log
    if "press" in lowered:
        return FeedType.press_releases
    if "alert" in lowered:
        return FeedType.community_alerts
    return FeedType.rss_feed


def detect_format(url: str) -> str:
    lowered = url.lower()
    if ".pdf" in lowered:
        return "pdf"
    if "rss" in lowered or "feed" in lowered:
        return "rss"
    if "arcgis" in lowered or "data." in lowered:
        return "json_api"
    return "html"


async def upsert_agency(session: AsyncSession, agency_id: str, row: dict) -> None:
    name = row["Department Name"].strip()
    stmt = (
        insert(Agency)
        .values(
            agency_id=agency_id,
            canonical_name=name,
            county=detect_county(name),
            agency_type=detect_agency_type(name),
            homepage_url=row["Official Website"].strip() or None,
            platform_type=detect_platform_type(
                row["Activity Data URL"], row["Notes"]
            ),
            has_activity_data=row["Has Activity Data"].strip().lower() == "yes",
            notes=row["Notes"].strip() or None,
        )
        .on_conflict_do_update(
            index_elements=["agency_id"],
            set_={
                "canonical_name": name,
                "county": detect_county(name),
                "agency_type": detect_agency_type(name),
                "homepage_url": row["Official Website"].strip() or None,
                "platform_type": detect_platform_type(
                    row["Activity Data URL"], row["Notes"]
                ),
                "has_activity_data": row["Has Activity Data"].strip().lower() == "yes",
                "notes": row["Notes"].strip() or None,
            },
        )
    )
    await session.execute(stmt)


async def insert_feed(session: AsyncSession, agency_id: str, row: dict) -> None:
    url = row["Activity Data URL"].strip()
    if not url:
        return

    # Delete existing feeds for this agency before reinserting
    await session.execute(
        delete(AgencyFeed).where(AgencyFeed.agency_id == agency_id)
    )

    stmt = insert(AgencyFeed).values(
        agency_id=agency_id,
        feed_type=detect_feed_type(row["Data Types Available"]),
        url=url,
        format=detect_format(url),
        is_active=True,
    )
    await session.execute(stmt)


async def main() -> None:
    rows = []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="|")
        for row in reader:
            rows.append(row)

    engine = create_async_engine(settings.DATABASE_URL, echo=False)

    type_counts: dict[str, int] = {}
    platform_counts: dict[str, int] = {}
    with_feeds = 0
    without_feeds = 0

    # Track slugs to handle duplicates
    seen_slugs: dict[str, int] = {}

    async with AsyncSession(engine) as session:
        for row in rows:
            name = row["Department Name"].strip()
            base_slug = make_slug(name)
            if base_slug in seen_slugs:
                seen_slugs[base_slug] += 1
                agency_id = f"{base_slug}-{seen_slugs[base_slug]}"
            else:
                seen_slugs[base_slug] = 1
                agency_id = base_slug

            await upsert_agency(session, agency_id, row)

            feed_url = row["Activity Data URL"].strip()
            if feed_url:
                await insert_feed(session, agency_id, row)
                with_feeds += 1
            else:
                without_feeds += 1

            atype = detect_agency_type(name).value
            type_counts[atype] = type_counts.get(atype, 0) + 1

            ptype = detect_platform_type(row["Activity Data URL"], row["Notes"])
            platform_counts[ptype] = platform_counts.get(ptype, 0) + 1

        await session.commit()

    await engine.dispose()

    total = len(rows)
    print(f"Imported {total} agencies.\n")

    print("By agency_type:")
    for k, v in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")

    print("\nBy platform_type:")
    for k, v in sorted(platform_counts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")

    print(f"\nWith feeds: {with_feeds}")
    print(f"Without feeds: {without_feeds}")


if __name__ == "__main__":
    asyncio.run(main())
