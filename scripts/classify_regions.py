"""
scripts/classify_regions.py - Infer county and assign region for all agencies.

Usage:
    python -m scripts.classify_regions
    # or
    python scripts/classify_regions.py
"""

import asyncio
import re
import sys
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

# Allow running as a script from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import settings
from src.registry.models import Agency

COUNTY_TO_REGION: dict[str, str] = {
    "Alameda": "Bay Area", "Contra Costa": "Bay Area", "Marin": "Bay Area",
    "Napa": "Bay Area", "San Francisco": "Bay Area", "San Mateo": "Bay Area",
    "Santa Clara": "Bay Area", "Solano": "Bay Area", "Sonoma": "Bay Area",
    "Sacramento": "Sacramento", "El Dorado": "Sacramento", "Placer": "Sacramento",
    "Yolo": "Sacramento", "Sutter": "Sacramento", "Yuba": "Sacramento",
    "San Joaquin": "Central Valley", "Stanislaus": "Central Valley",
    "Merced": "Central Valley", "Madera": "Central Valley",
    "Fresno": "Central Valley", "Kings": "Central Valley",
    "Tulare": "Central Valley", "Kern": "Central Valley",
    "Del Norte": "NorCal", "Siskiyou": "NorCal", "Modoc": "NorCal",
    "Humboldt": "NorCal", "Trinity": "NorCal", "Shasta": "NorCal",
    "Lassen": "NorCal", "Tehama": "NorCal", "Glenn": "NorCal",
    "Mendocino": "NorCal", "Lake": "NorCal", "Colusa": "NorCal",
    "Butte": "NorCal", "Plumas": "NorCal", "Sierra": "NorCal",
    "Nevada": "NorCal",
    "Santa Cruz": "Central Coast", "San Benito": "Central Coast",
    "Monterey": "Central Coast", "San Luis Obispo": "Central Coast",
    "Santa Barbara": "Central Coast", "Ventura": "Central Coast",
    "Riverside": "Inland Empire", "San Bernardino": "Inland Empire",
    "Los Angeles": "SoCal", "Orange": "SoCal",
    "San Diego": "San Diego", "Imperial": "San Diego",
}

# Sorted by length descending to prefer longer matches (e.g. "San Francisco" over "San")
_COUNTIES_BY_LENGTH = sorted(COUNTY_TO_REGION.keys(), key=len, reverse=True)


def infer_county(name: str) -> str | None:
    # Strategy 1: "X County" regex
    m = re.search(r"^([A-Za-z\s]+?)\s+County", name)
    if m:
        candidate = m.group(1).strip()
        if candidate in COUNTY_TO_REGION:
            return candidate

    # Strategy 2: prefix match against known county names
    for county in _COUNTIES_BY_LENGTH:
        if name.startswith(county + " ") or name == county:
            return county

    return None


async def main() -> None:
    engine = create_async_engine(settings.DATABASE_URL, echo=False)

    county_inferred = 0
    region_assigned = 0
    no_county_found = 0
    region_counts: dict[str, int] = {}

    async with AsyncSession(engine) as session:
        result = await session.execute(
            select(Agency).order_by(Agency.canonical_name)
        )
        agencies = result.scalars().all()

        for agency in agencies:
            county = agency.county or infer_county(agency.canonical_name)

            if county != agency.county:
                agency.county = county
                county_inferred += 1

            region = COUNTY_TO_REGION.get(county) if county else None

            if region:
                agency.region = region
                region_assigned += 1
                region_counts[region] = region_counts.get(region, 0) + 1
            else:
                no_county_found += 1

        await session.commit()

    await engine.dispose()

    print(f"Inferred county for {county_inferred} agencies.")
    print(f"Assigned region for {region_assigned} agencies.")
    print(f"No county found for {no_county_found} agencies.")

    print("\nRegion distribution:")
    for region, count in sorted(region_counts.items(), key=lambda x: -x[1]):
        print(f"  {region}: {count}")
    if no_county_found:
        print(f"  (no region): {no_county_found}")


if __name__ == "__main__":
    asyncio.run(main())
