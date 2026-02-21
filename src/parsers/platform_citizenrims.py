"""
CitizenRims Public Safety Portal Parser
========================================
Agency URLs follow the pattern: https://{slug}.citizenrims.com/
Some agencies use a path on their own domain (e.g. https://antiochca.gov/446/Crime-Statistics).

CitizenRims is a Records Information Management System (RIMS) portal used by ~75
CA agencies. It displays recent calls-for-service / incident logs in an HTML table.

Confirmed structure (*.citizenrims.com):
  Table          : table.table  (Bootstrap)
  Header row     : thead tr th  — columns typically: Date, Incident Type, Block Address
  Body rows      : tbody tr td  — one incident per row
  Date column    : td:first-child  (text like "02/18/2026 14:32")
  Type column    : td:nth-child(2)
  Address column : td:nth-child(3)
  Description    : td:nth-child(4) (present on some instances)

Pagination: ?page=N  (some instances; parser fetches up to max_pages)
"""
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.parsers.base import BaseParser, RawDocument

USER_AGENT = "CadenceBot/1.0 (+https://github.com/cadence)"
REQUEST_TIMEOUT = 30.0

# Try these selectors in order; first one that yields rows wins
TABLE_ROW_SELECTORS = [
    "table.table tbody tr",
    "table.incidents tbody tr",
    "table#incidentTable tbody tr",
    "table#callTable tbody tr",
    ".incident-row",
    ".call-row",
    "table tbody tr",          # generic fallback — any table body row
]

_DATE_FORMATS = (
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %I:%M:%S %p",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%m/%d/%Y",
)


def _build_page_url(base_url: str, page_num: int) -> str:
    if page_num == 1:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}page={page_num}"


class CitizenRimsParser(BaseParser):
    def __init__(self, agency_id: str, max_pages: int = 3) -> None:
        self.agency_id = agency_id
        self.max_pages = max_pages

    async def fetch(self, url: str) -> list[RawDocument]:
        await self.rate_limit_delay()
        docs: list[RawDocument] = []
        for page_num in range(1, self.max_pages + 1):
            page_url = _build_page_url(url, page_num)
            html = await self._get(page_url)
            if html is None:
                break
            page_docs = self._parse_page(html, page_url)
            if not page_docs:
                break
            docs.extend(page_docs)
            if not self._has_next_page(html):
                break
        return docs

    async def _get(self, url: str) -> str | None:
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=REQUEST_TIMEOUT
        ) as client:
            try:
                resp = await client.get(url, headers={"User-Agent": USER_AGENT})
                return resp.text if resp.status_code == 200 else None
            except Exception:
                return None

    def _parse_page(self, html: str, page_url: str) -> list[RawDocument]:
        soup = BeautifulSoup(html, "html.parser")
        rows: list = []
        for selector in TABLE_ROW_SELECTORS:
            rows = soup.select(selector)
            # Skip header rows (rows that contain th elements)
            rows = [r for r in rows if r.find("td")]
            if rows:
                break
        return [self._row_to_document(row, page_url) for row in rows]

    def _has_next_page(self, html: str) -> bool:
        soup = BeautifulSoup(html, "html.parser")
        if soup.find("a", rel="next"):
            return True
        if soup.find("a", string=re.compile(r"\bnext\b", re.I)):
            return True
        # Bootstrap pagination: li.next:not(.disabled)
        li = soup.find("li", class_="next")
        if li and "disabled" not in li.get("class", []):
            return True
        return False

    def _row_to_document(self, row, page_url: str) -> RawDocument:
        cells = row.find_all("td")
        texts = [c.get_text(" ", strip=True) for c in cells]

        # Heuristic: first cell resembling a date is the date column;
        # first cell that looks like a street address is location.
        date_str: str | None = None
        incident_type: str | None = None
        address: str | None = None
        description: str | None = None

        date_pattern = re.compile(r"\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2}")
        address_pattern = re.compile(r"\d+\s+\w+\s+(st|ave|blvd|dr|rd|ln|way|ct|pl)\b", re.I)

        for i, text in enumerate(texts):
            if date_str is None and date_pattern.search(text):
                date_str = text
            elif incident_type is None and not address_pattern.search(text):
                incident_type = text
            elif address is None and address_pattern.search(text):
                address = text
            elif description is None and len(text) > 5:
                description = text

        # Fallback: assign by position if heuristics failed
        if len(texts) >= 1 and date_str is None:
            date_str = texts[0]
        if len(texts) >= 2 and incident_type is None:
            incident_type = texts[1]
        if len(texts) >= 3 and address is None:
            address = texts[2]

        parts = [p for p in [incident_type, address, description] if p]
        raw_text = self.clean_whitespace(" at ".join(parts) if parts else " ".join(texts))

        return RawDocument(
            url=page_url,
            agency_id=self.agency_id,
            document_type="incident_log",
            title=incident_type,
            raw_text=raw_text or " ".join(texts[:3]),
            published_date=self._parse_date(date_str),
            source_metadata={
                "address": address,
                "incident_type": incident_type,
                "page_url": page_url,
            },
        )

    def _parse_date(self, date_str: str | None) -> datetime | None:
        if not date_str:
            return None
        # Strip extra whitespace and trailing junk
        cleaned = re.sub(r"\s+", " ", date_str).strip()
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(cleaned)
        except (ValueError, TypeError):
            return None
