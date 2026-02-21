"""
CrimeMapping agency ID discovery.

Queries the CrimeMapping public agency search API to find the numeric agency ID
for each registered agency where crimemapping_agency_id is NULL.

The CrimeMapping search endpoint:
    GET https://www.crimemapping.com/api/agencies/search?q={name}&state=CA

Returns JSON: [{"agencyId": 123, "agencyName": "...", "state": "CA", ...}]

Usage:
    python scripts/discover_crimemapping_ids.py [--dry-run]
"""
import argparse
import asyncio
import logging
import re
import sys
from pathlib import Path

import httpx
from sqlalchemy import select, update

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.api.deps import AsyncSessionLocal
from src.registry.models import Agency

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger("discover_crimemapping_ids")

USER_AGENT = "CadenceBot/1.0 (+https://github.com/cadence)"

# CrimeMapping agency search API (discovered via browser devtools)
SEARCH_URL = "https://www.crimemapping.com/api/agencies/search"

# Fallback: the public embed iframe list endpoint (returns all CA agencies)
LIST_URL = "https://www.crimemapping.com/cap/agencies?state=CA"


def _normalize(name: str) -> str:
    """Lowercase, strip 'department'/'office'/'police'/'sheriff' suffixes for fuzzy match."""
    n = name.lower()
    n = re.sub(r"\b(police department|sheriff.s office|sheriff department|police|sheriff|department|office)\b", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _best_match(candidates: list[dict], target_name: str) -> dict | None:
    """Return the candidate whose name best matches target_name, or None if no good match."""
    target_norm = _normalize(target_name)
    best: dict | None = None
    best_score = 0
    for c in candidates:
        candidate_name = c.get("agencyName") or c.get("name") or ""
        cand_norm = _normalize(candidate_name)
        # Score: fraction of target words found in candidate
        target_words = set(target_norm.split())
        cand_words = set(cand_norm.split())
        if not target_words:
            continue
        overlap = len(target_words & cand_words) / len(target_words)
        if overlap > best_score:
            best_score = overlap
            best = c
    # Require at least 60% word overlap
    return best if best_score >= 0.6 else None


async def fetch_agency_list(client: httpx.AsyncClient) -> list[dict]:
    """Fetch the full list of CA agencies from CrimeMapping."""
    try:
        resp = await client.get(
            LIST_URL,
            headers={"User-Agent": USER_AGENT},
            timeout=30.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return data.get("agencies", data.get("Agencies", []))
    except Exception as exc:
        logger.warning("Could not fetch agency list from %s: %s", LIST_URL, exc)
    return []


async def search_agency(
    client: httpx.AsyncClient, agency_name: str
) -> list[dict]:
    """Search CrimeMapping for a specific agency name."""
    try:
        resp = await client.get(
            SEARCH_URL,
            params={"q": agency_name, "state": "CA"},
            headers={"User-Agent": USER_AGENT},
            timeout=15.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
    except Exception as exc:
        logger.debug("Search failed for %r: %s", agency_name, exc)
    return []


async def load_crimemapping_agencies() -> list[Agency]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Agency)
            .where(Agency.platform_type == "crimemapping")
            .where(Agency.crimemapping_agency_id.is_(None))
            .order_by(Agency.agency_id)
        )
        return list(result.scalars().all())


async def update_crimemapping_id(
    agency_id: str, crimemapping_id: int, dry_run: bool
) -> None:
    if dry_run:
        logger.info(
            "DRY RUN — would set %s.crimemapping_agency_id = %d",
            agency_id,
            crimemapping_id,
        )
        return
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(Agency)
            .where(Agency.agency_id == agency_id)
            .values(crimemapping_agency_id=crimemapping_id)
        )
        await session.commit()
    logger.info("Set %s.crimemapping_agency_id = %d", agency_id, crimemapping_id)


async def main(dry_run: bool) -> None:
    agencies = await load_crimemapping_agencies()
    if not agencies:
        print("No CrimeMapping agencies with missing IDs found.")
        return

    logger.info("Looking up CrimeMapping IDs for %d agencies...", len(agencies))

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # Try to fetch full list first (one request covers all agencies)
        all_agencies = await fetch_agency_list(client)
        if all_agencies:
            logger.info("Fetched %d agencies from CrimeMapping list endpoint.", len(all_agencies))

        updated = 0
        not_found = []

        for agency in agencies:
            candidates = all_agencies
            if not candidates:
                # Fall back to per-agency search
                candidates = await search_agency(client, agency.canonical_name)
                await asyncio.sleep(0.5)  # rate limit

            match = _best_match(candidates, agency.canonical_name)
            if match:
                cid = match.get("agencyId") or match.get("AgencyId") or match.get("id")
                if cid:
                    await update_crimemapping_id(agency.agency_id, int(cid), dry_run)
                    updated += 1
                    continue

            logger.warning(
                "%s (%r): no CrimeMapping match found",
                agency.agency_id,
                agency.canonical_name,
            )
            not_found.append(agency.agency_id)

    print(f"\nCrimeMapping ID discovery:")
    print(f"  Updated   : {updated}")
    print(f"  Not found : {len(not_found)}")
    if not_found:
        print("  Manual lookup needed:")
        for aid in not_found:
            print(f"    {aid}")
    if dry_run:
        print("  (dry run — no DB changes written)")
    print("\nNext: run validate_ingest.py to verify crimemapping agencies are now reachable.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discover CrimeMapping numeric agency IDs")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Find IDs but do not write to DB",
    )
    args = parser.parse_args()
    asyncio.run(main(args.dry_run))
