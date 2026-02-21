"""
Socrata Open Data API Parser
============================
Socrata serves paginated JSON from dataset endpoints of the form:
    https://{domain}/resource/{dataset_id}.json
    ?$limit=1000&$offset=N&$order=:id&$where=date_field > 'cutoff'

Auth      : None (public datasets only)
Pagination: offset-based; stop when batch size < PAGE_SIZE
Response  : JSON array of row objects (flat dicts)
Date fmt  : ISO 8601 with optional timezone (e.g. "2024-01-15T08:30:00.000")
Nulls     : Socrata omits null fields entirely — dict.get() returns None
Computed  : Columns whose names start with ":" are Socrata internals — skip
"""
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import httpx

from src.parsers.base import BaseParser, RawDocument

PAGE_SIZE = 1000

_DATE_FORMATS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S.%f",
)

_DATE_CANDIDATES = (
    "date_occ",
    "date_rptd",
    "date",
    "datetime",
    "incident_date",
    "reported_date",
    "occurred_date",
)
_TYPE_CANDIDATES = (
    "crm_cd_desc",
    "offense_type",
    "incident_category",
    "crime_type",
    "offense_description",
    "type_desc",
)
_LOCATION_CANDIDATES = (
    "location",
    "address",
    "block_address",
    "incident_address",
    "location_1",
)
_REPORT_NUM_CANDIDATES = (
    "dr_no",
    "incident_number",
    "report_id",
    "case_number",
    "report_number",
    "id",
)

USER_AGENT = "CadenceBot/1.0 (+https://github.com/cadence)"
REQUEST_TIMEOUT = 30.0


class SocrataParser(BaseParser):
    def __init__(
        self,
        agency_id: str,
        date_field: str | None = None,
        days: int = 30,
        field_map: dict | None = None,
    ) -> None:
        self.agency_id = agency_id
        self.date_field = date_field
        self.days = days
        self.field_map = field_map or {}

    async def fetch(self, url: str) -> list[RawDocument]:
        await self.rate_limit_delay()
        domain, dataset_id = self._parse_url(url)
        cutoff = (
            datetime.now(tz=timezone.utc) - timedelta(days=self.days)
        ).strftime("%Y-%m-%dT%H:%M:%S")

        docs: list[RawDocument] = []
        offset = 0

        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            ) as client:
                while True:
                    params = self._build_params(offset, cutoff)
                    resp = await client.get(url, params=params)
                    if resp.status_code != 200:
                        break
                    try:
                        batch = resp.json()
                    except Exception:
                        break
                    if not isinstance(batch, list):
                        # Socrata error dict e.g. {"message": ..., "errorCode": 403}
                        break
                    if not batch:
                        break
                    for row in batch:
                        docs.append(
                            self._to_raw_document(row, url, domain, dataset_id, offset)
                        )
                    if len(batch) < PAGE_SIZE:
                        break
                    offset += PAGE_SIZE
                    await self.rate_limit_delay()
        except (httpx.TimeoutException, httpx.RequestError):
            return []

        return docs

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_url(self, url: str) -> tuple[str, str]:
        """Return (domain, dataset_id) from a Socrata resource URL."""
        parsed = urlparse(url)
        domain = parsed.netloc
        # path looks like /resource/xxxx-yyyy.json
        path = parsed.path
        if "/resource/" in path:
            after = path.split("/resource/", 1)[1]
            dataset_id = after.split(".")[0]
        else:
            dataset_id = path.strip("/").replace("/", "_")
        return domain, dataset_id

    def _build_params(self, offset: int, cutoff: str) -> dict:
        params: dict = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": ":id",
        }
        if self.date_field:
            params["$where"] = f"{self.date_field} > '{cutoff}'"
        return params

    def _resolve_field(self, row: dict, key: str, candidates: tuple) -> str | None:
        """Check field_map first, then try candidate column names in order."""
        # explicit override
        mapped = self.field_map.get(key)
        if mapped is not None:
            return row.get(mapped)
        # auto-detect
        for col in candidates:
            if col in row:
                val = row[col]
                if val is not None:
                    return str(val)
        return None

    def _format_raw_text(self, row: dict) -> str:
        """Sorted key:value dump; skip computed columns (start with ':')."""
        parts = []
        for k in sorted(row.keys()):
            if k.startswith(":"):
                continue
            v = row[k]
            if v is None:
                continue
            if isinstance(v, dict):
                # Flatten nested dicts; use 'human_address' if present
                v = v.get("human_address") or str(v)
            parts.append(f"{k}: {v}")
        return self.clean_whitespace("; ".join(parts))

    def _infer_document_type(self, type_val: str | None, url: str) -> str:
        lower_url = url.lower()
        if type_val:
            lower_type = type_val.lower()
            if "arrest" in lower_type or "booking" in lower_type:
                return "arrest_log"
            if "incident" in lower_type or "crime" in lower_type or "offense" in lower_type:
                return "incident_reports"
        if "arrest" in lower_url or "booking" in lower_url:
            return "arrest_log"
        if "incident" in lower_url or "crime" in lower_url:
            return "incident_reports"
        return "open_data_api"

    def _parse_date(self, date_str: str | None) -> datetime | None:
        if not date_str:
            return None
        # Strip timezone suffix before trying strptime formats
        s = date_str.strip()
        # Try fromisoformat on first 19 chars (handles "2024-01-15T08:30:00.000")
        try:
            return datetime.fromisoformat(s[:19])
        except (ValueError, TypeError):
            pass
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return None

    def _to_raw_document(
        self,
        row: dict,
        url: str,
        domain: str,
        dataset_id: str,
        offset: int,
    ) -> RawDocument:
        date_val = self._resolve_field(row, "date", _DATE_CANDIDATES)
        type_val = self._resolve_field(row, "type", _TYPE_CANDIDATES)
        location_val = self._resolve_field(row, "location", _LOCATION_CANDIDATES)
        report_num = self._resolve_field(row, "report_number", _REPORT_NUM_CANDIDATES)

        return RawDocument(
            url=url,
            agency_id=self.agency_id,
            document_type=self._infer_document_type(type_val, url),
            title=type_val,
            raw_text=self._format_raw_text(row),
            published_date=self._parse_date(date_val),
            source_metadata={
                "domain": domain,
                "dataset_id": dataset_id,
                "offset": offset,
                "report_number": report_num,
                "location": location_val,
            },
        )
