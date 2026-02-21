"""
CrimeMapping.com Data API
=========================
Embed URL : https://www.crimemapping.com/map/agency/{crimemapping_agency_id}
Data API  : https://www.crimemapping.com/cap/agency/{crimemapping_agency_id}
            ?days={days}&DetailRecordId=0&offset={offset}&limit={limit}
Method    : GET — returns JSON
Auth      : None. Requires browser session cookies set by embed page JS.
            Direct httpx calls fail; use Playwright route interception.
Response  : {"Incidents": [...], "TotalRecords": N}
Incident  : {"TypeDescription": str, "Address": str, "DateOccurred": str,
             "CaseNumber": str|None, "Description": str|None, ...}
            (exact field names confirmed via Playwright discovery)
Limit     : 100 per page; paginate when TotalRecords > len(Incidents).
Discovered: via Playwright network interception, map/agency/{ID} embed
"""
from datetime import datetime

from playwright.async_api import async_playwright

from src.parsers.base import PLAYWRIGHT_SEMAPHORE, BaseParser, RawDocument


class CrimeMappingParser(BaseParser):
    def __init__(self, agency_id: str, crimemapping_id: int, days: int = 14) -> None:
        self.agency_id = agency_id
        self.crimemapping_id = crimemapping_id
        self.days = days

    async def fetch(self, url: str) -> list[RawDocument]:
        await self.rate_limit_delay()
        embed_url = f"https://www.crimemapping.com/map/agency/{self.crimemapping_id}"
        async with PLAYWRIGHT_SEMAPHORE:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                async with page.expect_response(
                    lambda r: "/cap/agency/" in r.url and r.status == 200
                ) as response_info:
                    await page.goto(embed_url, wait_until="domcontentloaded")
                data = await (await response_info.value).json()
                await browser.close()
        incidents = data.get("Incidents", [])
        if not incidents:
            return []
        return [self._to_raw_document(inc) for inc in incidents]

    def _to_raw_document(self, incident: dict) -> RawDocument:
        type_desc = incident.get("TypeDescription") or ""
        address = incident.get("Address") or ""
        date_str = incident.get("DateOccurred") or ""
        description = incident.get("Description") or ""
        raw_text = self.clean_whitespace(
            f"{type_desc} at {address} on {date_str}. {description}"
        )
        return RawDocument(
            url=f"https://www.crimemapping.com/map/agency/{self.crimemapping_id}",
            agency_id=self.agency_id,
            document_type="incident_log",
            title=incident.get("TypeDescription") or None,
            raw_text=raw_text,
            published_date=self._parse_date(incident.get("DateOccurred")),
            source_metadata={
                "crimemapping_id": self.crimemapping_id,
                "case_number": incident.get("CaseNumber"),
            },
        )

    def _parse_date(self, date_str: str | None) -> datetime | None:
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str)
        except ValueError:
            pass
        try:
            return datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
        except ValueError:
            return None
