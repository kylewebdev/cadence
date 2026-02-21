"""
Feed URL discovery for agencies with placeholder 'N/A' feed URLs.

For each agency where the active feed URL is 'N/A', this script:
  1. Fetches the agency homepage
  2. Finds candidate news/press-release/alert links on that page
  3. Tests the agency's parser against each candidate
  4. If the parser returns ≥1 document, updates the feed URL in the DB

Supported platforms: civicplus, nixle, rss, arcgis, socrata
(CrimeMapping requires a numeric agency ID — handled by discover_crimemapping_ids.py)

Usage:
    python scripts/discover_feed_urls.py [--platform PLATFORM] [--limit N] [--dry-run]
"""
import argparse
import asyncio
import logging
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.api.deps import AsyncSessionLocal
from src.registry.models import Agency, AgencyFeed
from src.scheduler.parser_registry import get_parser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger("discover_feed_urls")

for _noisy in ("httpx", "httpcore", "playwright", "asyncio"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

USER_AGENT = "CadenceBot/1.0 (+https://github.com/cadence)"
HTTP_TIMEOUT = 15.0
PARSER_TEST_TIMEOUT = 20.0

# Keywords that indicate a press-release / activity-log page
NEWS_KEYWORDS = (
    "news", "press", "release", "alert", "blotter",
    "arrest", "log", "crime", "activity", "incident",
)

# CivicPlus-specific common paths to probe before crawling the homepage
CIVICPLUS_PROBE_PATHS = [
    "/CivicAlerts.aspx",
    "/news",
    "/News",
    "/press-releases",
    "/Press-Releases",
    "/news-releases",
    "/police-news",
    "/department-news",
]


def _candidate_urls_from_homepage(homepage_html: str, homepage_url: str) -> list[str]:
    """Extract internal links that likely lead to a news/press-release feed."""
    soup = BeautifulSoup(homepage_html, "html.parser")
    parsed = urlparse(homepage_url)
    base_netloc = parsed.netloc
    seen: set[str] = set()
    candidates: list[str] = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        full = urljoin(homepage_url, href)
        fp = urlparse(full)
        # Internal links only, HTTP/HTTPS, no fragments/anchors
        if fp.netloc != base_netloc:
            continue
        if fp.scheme not in ("http", "https"):
            continue
        if "#" in href:
            continue
        path_lower = fp.path.lower()
        if any(kw in path_lower for kw in NEWS_KEYWORDS):
            if full not in seen:
                seen.add(full)
                candidates.append(full)
    return candidates


async def _fetch_html(url: str, client: httpx.AsyncClient) -> str | None:
    try:
        resp = await client.get(url, headers={"User-Agent": USER_AGENT})
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return None


async def discover_url_for_agency(
    agency: Agency,
    client: httpx.AsyncClient,
) -> str | None:
    """
    Return the first URL that the agency's parser can successfully scrape,
    or None if no candidate works.
    """
    homepage = agency.homepage_url
    if not homepage or not homepage.startswith(("http://", "https://")):
        logger.debug("%s: no homepage URL", agency.agency_id)
        return None

    parser = get_parser(agency)
    if parser is None:
        logger.debug("%s: no parser (platform_type=%r)", agency.agency_id, agency.platform_type)
        return None

    # Build candidate list
    parsed = urlparse(homepage)
    base = f"{parsed.scheme}://{parsed.netloc}"
    candidates: list[str] = []

    # Platform-specific probes first (fast, no homepage fetch needed)
    if agency.platform_type == "civicplus":
        candidates.extend(f"{base}{p}" for p in CIVICPLUS_PROBE_PATHS)

    # Fetch homepage and extract internal news links
    homepage_html = await _fetch_html(homepage, client)
    if homepage_html:
        candidates.extend(_candidate_urls_from_homepage(homepage_html, homepage))

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_candidates: list[str] = []
    for url in candidates:
        if url not in seen:
            seen.add(url)
            unique_candidates.append(url)

    logger.debug("%s: testing %d candidates", agency.agency_id, len(unique_candidates))

    for url in unique_candidates[:15]:  # cap at 15 to avoid excessive requests
        try:
            docs = await asyncio.wait_for(
                parser.fetch(url), timeout=PARSER_TEST_TIMEOUT
            )
            if docs:
                logger.info(
                    "%s (%s): found working URL %s (%d docs)",
                    agency.agency_id, agency.platform_type, url, len(docs),
                )
                return url
        except asyncio.TimeoutError:
            logger.debug("%s: timeout on %s", agency.agency_id, url)
        except Exception as exc:
            logger.debug("%s: %s failed: %s", agency.agency_id, url, exc)

    return None


async def load_agencies_with_na_urls(platform: str | None) -> list[Agency]:
    async with AsyncSessionLocal() as session:
        stmt = (
            select(Agency)
            .join(AgencyFeed, AgencyFeed.agency_id == Agency.agency_id)
            .where(AgencyFeed.url == "N/A")
            .where(AgencyFeed.is_active == True)
            .where(Agency.platform_type.isnot(None))
            .where(Agency.platform_type != "crimemapping")  # handled separately
        )
        if platform:
            stmt = stmt.where(Agency.platform_type == platform)
        stmt = stmt.options(selectinload(Agency.feeds)).order_by(Agency.agency_id)
        result = await session.execute(stmt)
        return list(result.scalars().unique().all())


async def update_feed_url(agency_id: str, new_url: str, dry_run: bool) -> None:
    if dry_run:
        logger.info("DRY RUN — would update %s feed URL to %s", agency_id, new_url)
        return
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(AgencyFeed)
            .where(AgencyFeed.agency_id == agency_id)
            .where(AgencyFeed.url == "N/A")
            .where(AgencyFeed.is_active == True)
            .values(url=new_url)
        )
        await session.commit()
    logger.info("Updated %s feed URL → %s", agency_id, new_url)


async def main(platform: str | None, limit: int | None, dry_run: bool) -> None:
    agencies = await load_agencies_with_na_urls(platform)
    if limit:
        agencies = agencies[:limit]

    logger.info(
        "Discovering URLs for %d agencies with N/A feed URLs%s...",
        len(agencies),
        f" (platform={platform!r})" if platform else "",
    )

    updated = 0
    failed = 0

    semaphore = asyncio.Semaphore(3)

    async def process(agency: Agency) -> None:
        nonlocal updated, failed
        async with semaphore:
            async with httpx.AsyncClient(
                follow_redirects=True, timeout=HTTP_TIMEOUT
            ) as client:
                url = await discover_url_for_agency(agency, client)
        if url:
            await update_feed_url(agency.agency_id, url, dry_run)
            updated += 1
        else:
            logger.warning("%s (%s): no working URL found", agency.agency_id, agency.platform_type)
            failed += 1

    await asyncio.gather(*[process(a) for a in agencies])

    print(f"\nDiscovery complete:")
    print(f"  Updated : {updated}")
    print(f"  Not found: {failed}")
    if dry_run:
        print("  (dry run — no DB changes written)")
    print(f"\nNext step: re-run validate_ingest.py to measure updated coverage.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discover feed URLs for agencies with N/A placeholder")
    parser.add_argument(
        "--platform",
        default=None,
        help="Limit to one platform type (e.g. civicplus, nixle)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max agencies to process",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Find URLs but do not write to DB",
    )
    args = parser.parse_args()
    asyncio.run(main(args.platform, args.limit, args.dry_run))
