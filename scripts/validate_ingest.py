"""
Phase 2 ingestion validation script.

Runs a sample parse across all agencies with working parsers, deduplicates,
bulk-inserts into Postgres documents table, and reports Phase 2 coverage.

Usage:
    python scripts/validate_ingest.py [--limit N] [--output PATH] [--timeout SECS]
"""
import argparse
import asyncio
import csv
import dataclasses
import hashlib
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root is on the path when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import selectinload

from src.api.deps import AsyncSessionLocal
from src.dedup.deduplicator import Deduplicator
from src.parsers import PARSER_REGISTRY
from src.registry.models import Agency, Document
from src.scheduler.parser_registry import get_parser
from src.scheduler.queue import ProcessingQueue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger("validate_ingest")

# Suppress noisy sub-loggers during bulk run
for _noisy in ("httpx", "httpcore", "playwright", "asyncio"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)


@dataclasses.dataclass
class AgencyResult:
    agency_id: str
    parser_id: str | None
    docs_fetched: int = 0      # total returned by parser (pre-dedup)
    doc_count: int = 0         # unique docs pushed to queue (post-dedup)
    docs_inserted: int = 0     # rows actually written to Postgres this run
    date_min: datetime | None = None
    date_max: datetime | None = None
    error: str | None = None


def _doc_hash(url: str, raw_text: str) -> str:
    return hashlib.sha256(f"{url}:{raw_text}".encode()).hexdigest()


async def process_agency(
    agency: Agency,
    dedup: Deduplicator,
    queue: ProcessingQueue,
    timeout: float,
    semaphore: asyncio.Semaphore,
) -> AgencyResult:
    result = AgencyResult(
        agency_id=agency.agency_id,
        parser_id=agency.platform_type,
    )

    parser = get_parser(agency)
    if parser is None:
        result.error = f"no_parser (platform_type={agency.platform_type!r})"
        return result

    active_feeds = [f for f in agency.feeds if f.is_active]
    if not active_feeds:
        result.error = "no_active_feeds"
        return result

    feed_url = active_feeds[0].url
    if feed_url == "N/A" or not feed_url.startswith(("http://", "https://")):
        result.error = "no_url"
        return result

    async with semaphore:
        try:
            docs = await asyncio.wait_for(parser.fetch(feed_url), timeout=timeout)
        except asyncio.TimeoutError:
            result.error = f"timeout after {timeout}s"
            return result
        except Exception as exc:
            result.error = str(exc)[:200]
            return result

    result.docs_fetched = len(docs)
    for doc in docs:
        if doc.published_date:
            if result.date_min is None or doc.published_date < result.date_min:
                result.date_min = doc.published_date
            if result.date_max is None or doc.published_date > result.date_max:
                result.date_max = doc.published_date
        if not await dedup.is_duplicate(doc):
            await queue.push(doc)
            await dedup.mark_seen(doc)
            result.doc_count += 1

    return result


async def bulk_insert_from_queue(
    queue: ProcessingQueue,
) -> dict[str, int]:
    """Drain queue, bulk-insert into documents, return {agency_id: inserted_count}."""
    docs = await queue.pop_all()
    if not docs:
        logger.info("Queue empty — nothing to insert.")
        return {}

    logger.info("Inserting %d documents into Postgres...", len(docs))

    rows = [
        {
            "agency_id": doc.agency_id,
            "url": doc.url,
            "doc_hash": _doc_hash(doc.url, doc.raw_text),
            "document_type": doc.document_type,
            "title": doc.title,
            "raw_text": doc.raw_text,
            "published_date": doc.published_date,
            "source_metadata": doc.source_metadata or {},
        }
        for doc in docs
    ]

    inserted_counts: dict[str, int] = {}

    async with AsyncSessionLocal() as session:
        stmt = (
            pg_insert(Document)
            .values(rows)
            .on_conflict_do_nothing(index_elements=["doc_hash"])
            .returning(Document.agency_id)
        )
        result = await session.execute(stmt)
        for (aid,) in result.fetchall():
            inserted_counts[aid] = inserted_counts.get(aid, 0) + 1
        await session.commit()

    total = sum(inserted_counts.values())
    logger.info("Inserted %d new documents (skipped %d duplicates).", total, len(rows) - total)
    return inserted_counts


async def load_agencies(limit: int | None) -> list[Agency]:
    known_platform_types = list(PARSER_REGISTRY.keys())
    async with AsyncSessionLocal() as session:
        stmt = (
            select(Agency)
            .where(Agency.platform_type.in_(known_platform_types))
            .options(selectinload(Agency.feeds))
            .order_by(Agency.agency_id)
        )
        if limit:
            stmt = stmt.limit(limit)
        result = await session.execute(stmt)
        return list(result.scalars().all())


async def print_db_summary() -> None:
    print("\n--- Documents table summary (top 20 by doc count) ---")
    async with AsyncSessionLocal() as session:
        rows = await session.execute(
            text(
                """
                SELECT agency_id, COUNT(*) AS doc_count,
                       MIN(published_date) AS oldest, MAX(published_date) AS newest
                FROM documents
                GROUP BY agency_id
                ORDER BY doc_count DESC
                LIMIT 20
                """
            )
        )
        rows = rows.fetchall()
        if not rows:
            print("  (no documents yet)")
            return
        print(f"  {'agency_id':<35} {'docs':>6}  {'oldest':<24}  newest")
        print("  " + "-" * 80)
        for agency_id, doc_count, oldest, newest in rows:
            o = oldest.isoformat() if oldest else "—"
            n = newest.isoformat() if newest else "—"
            print(f"  {agency_id:<35} {doc_count:>6}  {o:<24}  {n}")


def write_csv(results: list[AgencyResult], output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "agency_id",
                "parser_id",
                "docs_fetched",
                "doc_count",
                "docs_inserted",
                "date_range_min",
                "date_range_max",
                "error",
            ],
        )
        writer.writeheader()
        for r in results:
            writer.writerow(
                {
                    "agency_id": r.agency_id,
                    "parser_id": r.parser_id or "",
                    "docs_fetched": r.docs_fetched,
                    "doc_count": r.doc_count,
                    "docs_inserted": r.docs_inserted,
                    "date_range_min": r.date_min.isoformat() if r.date_min else "",
                    "date_range_max": r.date_max.isoformat() if r.date_max else "",
                    "error": r.error or "",
                }
            )
    logger.info("CSV written to %s", output_path)


def print_coverage_summary(results: list[AgencyResult], total_registry: int = 697) -> None:
    # total_registry param kept for the "of 697 total" context line but the
    # Phase 2 metric denominator is only agencies we have parsers for.
    # Per-platform breakdown: ok | no_url | no_parser_config | parser_error
    platform_stats: dict[str, dict[str, int]] = {}
    for r in results:
        pt = r.parser_id or "unknown"
        s = platform_stats.setdefault(pt, {"ok": 0, "no_url": 0, "no_config": 0, "error": 0})
        if r.error is None:
            s["ok"] += 1
        elif r.error == "no_url":
            s["no_url"] += 1
        elif "no_parser" in r.error:
            s["no_config"] += 1
        else:
            s["error"] += 1

    print("\nPlatform coverage:")
    header = f"  {'platform':<16} {'total':>6}  {'ok':>5}  {'no_url':>7}  {'no_cfg':>7}  {'error':>6}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for pt, s in sorted(platform_stats.items(), key=lambda x: -(sum(x[1].values()))):
        total = sum(s.values())
        print(
            f"  {pt:<16} {total:>6}  {s['ok']:>5}  {s['no_url']:>7}  {s['no_config']:>7}  {s['error']:>6}"
        )

    # Phase 2 metric: of agencies we have parsers for, how many are reachable?
    # (doc_count may be 0 on repeat runs due to Redis dedup — use error is None)
    # Excludes `unknown` platform agencies — no parser exists for them yet.
    agencies_covered = sum(1 for r in results if r.error is None)
    agencies_with_data = sum(1 for r in results if r.docs_fetched > 0)
    no_url_count = sum(1 for r in results if r.error == "no_url")
    denominator = len(results)
    pct_parseable = 100 * agencies_covered / denominator if denominator else 0
    target_pct = 60
    target_count = round(denominator * target_pct / 100)
    status = "✓ PASS" if pct_parseable >= target_pct else "✗ FAIL"
    print(
        f"\nPhase 2 metric: {agencies_covered}/{denominator} parser-covered agencies reachable"
        f" ({pct_parseable:.1f}%) — target ≥{target_pct}% {status}"
    )
    print(f"  Agencies returning data    : {agencies_with_data}/{denominator}")
    print(f"  Blocked by missing URL     : {no_url_count} (run discover_feed_urls.py)")
    print(f"  Of 697 total in registry   : {agencies_covered} reachable ({100*agencies_covered/total_registry:.1f}%)")


async def main(output_path: str, limit: int | None, timeout: float) -> None:
    logger.info("Loading agencies from registry...")
    agencies = await load_agencies(limit)
    logger.info("Found %d agencies with known platform types.", len(agencies))

    dedup = Deduplicator()
    queue = ProcessingQueue()
    semaphore = asyncio.Semaphore(5)

    tasks = [
        process_agency(agency, dedup, queue, timeout, semaphore)
        for agency in agencies
    ]

    results: list[AgencyResult] = []
    completed = 0
    total = len(tasks)

    for coro in asyncio.as_completed(tasks):
        r = await coro
        results.append(r)
        completed += 1
        status = "ok" if r.error is None else f"err: {r.error}"
        logger.info(
            "[%d/%d] %s (%s): %d fetched / %d new — %s",
            completed,
            total,
            r.agency_id,
            r.parser_id or "?",
            r.docs_fetched,
            r.doc_count,
            status,
        )

    # Drain queue and insert
    inserted_by_agency = await bulk_insert_from_queue(queue)
    for r in results:
        r.docs_inserted = inserted_by_agency.get(r.agency_id, 0)

    write_csv(results, output_path)
    print_coverage_summary(results)
    await print_db_summary()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Phase 2 ingestion validation")
    parser.add_argument(
        "--output",
        default=f"reports/ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        help="Output CSV path (default: reports/ingest_YYYYMMDD_HHMMSS.csv)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of agencies (useful for quick tests)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Per-agency fetch timeout in seconds (default: 30)",
    )
    args = parser.parse_args()

    asyncio.run(main(args.output, args.limit, args.timeout))
