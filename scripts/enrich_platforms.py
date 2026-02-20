"""
scripts/enrich_platforms.py - Enrich agency platform_type by fetching and inspecting URLs.

Usage:
    python scripts/enrich_platforms.py
"""

import asyncio
import csv
import dataclasses
import datetime
import logging
import re
import ssl
import sys
import time
import urllib.parse
import urllib.robotparser
from pathlib import Path

import httpx
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import selectinload

# Allow running as a script from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import settings
from src.registry.models import Agency

PROJECT_ROOT = Path(__file__).parent.parent

USER_AGENT = "CadenceBot/1.0 (+https://github.com/cadence)"
HTTP_TIMEOUT = 10.0
RATE_LIMIT_DELAY = 2.0
REPORTS_DIR = PROJECT_ROOT / "reports"

DETECTION_RULES = [
    # (platform, url_substrings, html_substrings)
    ("citizenrims",   ["citizenrims.com"],   ["CitizenRIMS", "citizen-rims"]),
    ("crimegraphics", ["crimegraphics.com"], ["CrimeGraphics"]),
    ("crimemapping",  ["crimemapping.com"],  ["CrimeMapping", "crimemapping"]),
    ("civicplus",     [],                    ["civicplus", "CivicPlus", "civic-plus"]),
    ("nixle",         ["nixle.com"],         ["nixle", "Nixle"]),
    ("axon",          [],                    ["axon.com", "evidence.com", "Axon"]),
    ("mark43",        [],                    ["mark43", "Mark43"]),
    ("socrata",       [],                    ["socrata", "Socrata"]),
    ("arcgis",        ["arcgis"],            ["ArcGIS", "arcgis"]),
    ("rave",          [],                    ["rave", "RaveMobileSafety", "smart911"]),
]

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DetectionResult:
    platform: str
    detection_source: str  # e.g. "homepage_url:url=nixle.com" or "feed_url:html=ArcGIS"


@dataclasses.dataclass
class AgencyOutcome:
    agency_id: str
    canonical_name: str
    detected_platform: str | None  # None = still unknown
    detection_source: str
    had_error: bool


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def load_agencies(session: AsyncSession) -> list[Agency]:
    stmt = (
        select(Agency)
        .where(or_(Agency.platform_type == "unknown", Agency.platform_type.is_(None)))
        .options(selectinload(Agency.feeds))
        .order_by(Agency.canonical_name)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


def detect_platform(url: str, html: str, url_label: str) -> DetectionResult | None:
    # Socrata URL rule: check before loop
    if re.search(r"data\.[^/]+\.(gov|org)", url, re.I):
        return DetectionResult(platform="socrata", detection_source=f"{url_label}:url=data.*.gov/org")

    for platform, url_substrings, html_substrings in DETECTION_RULES:
        for token in url_substrings:
            if token in url:
                return DetectionResult(
                    platform=platform,
                    detection_source=f"{url_label}:url={token}",
                )
        for token in html_substrings:
            if token in html:
                return DetectionResult(
                    platform=platform,
                    detection_source=f"{url_label}:html={token}",
                )

    return None


def fetch_robots_sync(text: str) -> urllib.robotparser.RobotFileParser:
    parser = urllib.robotparser.RobotFileParser()
    parser.parse(text.splitlines())
    return parser


async def fetch_robots(
    client: httpx.AsyncClient, base_url: str
) -> urllib.robotparser.RobotFileParser | None:
    robots_url = f"{base_url}/robots.txt"
    try:
        response = await client.get(robots_url, headers={"User-Agent": USER_AGENT})
        return fetch_robots_sync(response.text)
    except Exception:
        return None


def is_allowed(parser: urllib.robotparser.RobotFileParser | None, url: str) -> bool:
    if parser is None:
        return True
    return parser.can_fetch(USER_AGENT, url)


async def rate_limited_get(
    client: httpx.AsyncClient,
    url: str,
    last_request_time: float,
) -> tuple[httpx.Response | None, float]:
    elapsed = time.monotonic() - last_request_time
    sleep_for = max(0.0, RATE_LIMIT_DELAY - elapsed)
    if sleep_for > 0:
        await asyncio.sleep(sleep_for)

    now = time.monotonic()
    try:
        response = await client.get(url, headers={"User-Agent": USER_AGENT})
        return response, now
    except httpx.TimeoutException:
        logger.warning("Timeout fetching %s", url)
    except httpx.ConnectError:
        logger.warning("Connection error fetching %s", url)
    except ssl.SSLError:
        logger.warning("SSL error fetching %s", url)
    except Exception as exc:
        logger.warning("Error fetching %s: %s", url, exc)

    return None, now


async def check_url(
    client: httpx.AsyncClient,
    url: str,
    url_label: str,
    robots_cache: dict[str, urllib.robotparser.RobotFileParser | None],
    last_request_time: float,
) -> tuple[DetectionResult | None, float]:
    parsed = urllib.parse.urlparse(url)
    cache_key = f"{parsed.scheme}://{parsed.netloc}"

    if cache_key not in robots_cache:
        robots_response, last_request_time = await rate_limited_get(
            client, f"{cache_key}/robots.txt", last_request_time
        )
        if robots_response is not None and not robots_response.is_error:
            robots_cache[cache_key] = fetch_robots_sync(robots_response.text)
        else:
            robots_cache[cache_key] = None

    if not is_allowed(robots_cache[cache_key], url):
        logger.info("robots.txt disallows %s, skipping", url)
        return None, last_request_time

    response, last_request_time = await rate_limited_get(client, url, last_request_time)
    if response is None or response.is_error:
        return None, last_request_time

    result = detect_platform(url, response.text, url_label)
    return result, last_request_time


async def process_agency(
    agency: Agency,
    client: httpx.AsyncClient,
    robots_cache: dict[str, urllib.robotparser.RobotFileParser | None],
    last_request_time: float,
    session: AsyncSession,
) -> tuple[AgencyOutcome, float]:
    try:
        candidates: list[tuple[str, str]] = []
        if agency.homepage_url:
            candidates.append((agency.homepage_url, "homepage_url"))
        for feed in agency.feeds:
            candidates.append((feed.url, "feed_url"))

        result: DetectionResult | None = None
        for url, url_label in candidates:
            detection, last_request_time = await check_url(
                client, url, url_label, robots_cache, last_request_time
            )
            if detection is not None:
                result = detection
                break

        if result is not None:
            agency.platform_type = result.platform
            try:
                await session.commit()
            except Exception as commit_exc:
                logger.error(
                    "Commit failed for %s: %s", agency.canonical_name, commit_exc
                )
                await session.rollback()
                return (
                    AgencyOutcome(
                        agency_id=agency.agency_id,
                        canonical_name=agency.canonical_name,
                        detected_platform=None,
                        detection_source="",
                        had_error=True,
                    ),
                    last_request_time,
                )

            logger.info(
                "%-60s -> %s  [%s]",
                agency.canonical_name,
                result.platform,
                result.detection_source,
            )
            return (
                AgencyOutcome(
                    agency_id=agency.agency_id,
                    canonical_name=agency.canonical_name,
                    detected_platform=result.platform,
                    detection_source=result.detection_source,
                    had_error=False,
                ),
                last_request_time,
            )
        else:
            logger.info("%-60s -> (no match)", agency.canonical_name)
            return (
                AgencyOutcome(
                    agency_id=agency.agency_id,
                    canonical_name=agency.canonical_name,
                    detected_platform=None,
                    detection_source="",
                    had_error=False,
                ),
                last_request_time,
            )

    except Exception as exc:
        logger.error("Error processing agency %s: %s", agency.canonical_name, exc)
        return (
            AgencyOutcome(
                agency_id=agency.agency_id,
                canonical_name=agency.canonical_name,
                detected_platform=None,
                detection_source="",
                had_error=True,
            ),
            last_request_time,
        )


def save_report(outcomes: list[AgencyOutcome], reports_dir: Path) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.date.today().strftime("%Y%m%d")
    report_path = reports_dir / f"platform_enrichment_{date_str}.csv"

    with open(report_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["agency_id", "canonical_name", "detected_platform", "detection_source"],
        )
        writer.writeheader()
        for outcome in outcomes:
            writer.writerow(
                {
                    "agency_id": outcome.agency_id,
                    "canonical_name": outcome.canonical_name,
                    "detected_platform": outcome.detected_platform or "unknown",
                    "detection_source": outcome.detection_source,
                }
            )

    return report_path


async def main() -> None:
    setup_logging()
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    robots_cache: dict[str, urllib.robotparser.RobotFileParser | None] = {}
    outcomes: list[AgencyOutcome] = []

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            agencies = await load_agencies(session)
            logger.info("Found %d agencies with unknown platform_type", len(agencies))

            # Initialize so the first real request fires immediately
            last_request_time = time.monotonic() - RATE_LIMIT_DELAY

            for agency in agencies:
                outcome, last_request_time = await process_agency(
                    agency, client, robots_cache, last_request_time, session
                )
                outcomes.append(outcome)

    await engine.dispose()

    classified = sum(1 for o in outcomes if o.detected_platform is not None)
    still_unknown = sum(
        1 for o in outcomes if o.detected_platform is None and not o.had_error
    )
    errors = sum(1 for o in outcomes if o.had_error)

    logger.info(
        "Done. %d classified, %d still unknown, %d errors (of %d total)",
        classified,
        still_unknown,
        errors,
        len(outcomes),
    )

    report_path = save_report(outcomes, REPORTS_DIR)
    logger.info("Report saved to %s", report_path)


if __name__ == "__main__":
    asyncio.run(main())
