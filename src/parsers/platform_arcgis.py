"""
ArcGIS REST FeatureServer / MapServer Parser
============================================
ArcGIS REST services expose features via:
    {base_url}/query?where=...&outFields=*&f=json&resultOffset=N&resultRecordCount=1000

Auth      : None (public services only)
Pagination: Continue while response contains "exceededTransferLimit": true
Response  : {"features": [{"attributes": {...}, "geometry": {...}}, ...]}
Date fmt  : Epoch milliseconds as integer (most common); -1 is the null sentinel
Nulls     : -1 for dates; null/None for strings
Errors    : ArcGIS returns HTTP 200 even for errors; check payload["error"]
Fields    : Names can be ALL_CAPS, camelCase, or lowercase — resolve case-insensitively
"""
from datetime import datetime, timedelta, timezone, UTC

import httpx

from src.parsers.base import BaseParser, RawDocument

PAGE_SIZE = 1000

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
    "incident_type",
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
    "objectid",
    "id",
)

USER_AGENT = "CadenceBot/1.0 (+https://github.com/cadence)"
REQUEST_TIMEOUT = 30.0


class ArcGISParser(BaseParser):
    def __init__(
        self,
        agency_id: str,
        days: int = 30,
        date_field: str | None = None,
    ) -> None:
        self.agency_id = agency_id
        self.days = days
        self.date_field = date_field

    async def fetch(self, url: str) -> list[RawDocument]:
        await self.rate_limit_delay()
        cutoff_date = (datetime.now(tz=timezone.utc) - timedelta(days=self.days)).strftime(
            "%Y-%m-%d"
        )

        docs: list[RawDocument] = []
        offset = 0

        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            ) as client:
                while True:
                    params = self._build_params(offset, cutoff_date)
                    resp = await client.get(url, params=params)
                    if resp.status_code != 200:
                        break
                    try:
                        payload = resp.json()
                    except Exception:
                        break

                    # ArcGIS returns HTTP 200 with error payload on failure
                    if payload.get("error"):
                        break

                    features = payload.get("features", [])
                    if not features:
                        break

                    for feature in features:
                        docs.append(
                            self._to_raw_document(feature, url, offset)
                        )

                    # Only continue if ArcGIS says there are more records
                    if not payload.get("exceededTransferLimit", False):
                        break

                    offset += PAGE_SIZE
                    await self.rate_limit_delay()
        except (httpx.TimeoutException, httpx.RequestError):
            return []

        return docs

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_params(self, offset: int, cutoff_date: str) -> dict:
        if self.date_field:
            where = f"{self.date_field} > DATE '{cutoff_date}'"
        else:
            where = "1=1"
        return {
            "where": where,
            "outFields": "*",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
        }

    def _resolve_field(self, attrs: dict, candidates: tuple) -> str | None:
        """Case-insensitive field resolution against a tuple of candidate names."""
        # Build lowercase → original key map once
        lower_map = {k.lower(): k for k in attrs}
        for candidate in candidates:
            original_key = lower_map.get(candidate.lower())
            if original_key is not None:
                val = attrs[original_key]
                if val is not None and val != -1:
                    return str(val)
        return None

    def _format_raw_text(self, attrs: dict) -> str:
        """Sorted key:value dump; skip -1 sentinel values."""
        parts = []
        for k in sorted(attrs.keys()):
            v = attrs[k]
            if v is None or v == -1:
                continue
            parts.append(f"{k}: {v}")
        return self.clean_whitespace("; ".join(parts))

    def _parse_date(self, value) -> datetime | None:
        """Parse ArcGIS date values: epoch ms (int/str), ISO string, or None."""
        if value is None:
            return None
        # Numeric epoch milliseconds (most common ArcGIS format)
        if isinstance(value, (int, float)):
            if value < 0:
                return None
            try:
                return datetime.fromtimestamp(value / 1000, tz=UTC).replace(tzinfo=None)
            except (OSError, OverflowError, ValueError):
                return None
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            # Could be a numeric string
            try:
                epoch_ms = float(s)
                if epoch_ms < 0:
                    return None
                return datetime.fromtimestamp(epoch_ms / 1000, tz=UTC).replace(tzinfo=None)
            except (ValueError, TypeError):
                pass
            # ISO string fallback
            try:
                return datetime.fromisoformat(s[:19])
            except (ValueError, TypeError):
                pass
        return None

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

    def _to_raw_document(
        self,
        feature: dict,
        url: str,
        offset: int,
    ) -> RawDocument:
        attrs = feature.get("attributes") or {}
        geometry = feature.get("geometry")

        # Resolve date — look for date_field first, then candidates
        date_val: datetime | None = None
        if self.date_field:
            raw_date = attrs.get(self.date_field)
            if raw_date is None:
                # case-insensitive fallback
                lower_map = {k.lower(): k for k in attrs}
                key = lower_map.get(self.date_field.lower())
                raw_date = attrs.get(key) if key else None
            date_val = self._parse_date(raw_date)
        if date_val is None:
            for candidate in _DATE_CANDIDATES:
                lower_map = {k.lower(): k for k in attrs}
                key = lower_map.get(candidate.lower())
                if key:
                    date_val = self._parse_date(attrs[key])
                    if date_val:
                        break

        type_val = self._resolve_field(attrs, _TYPE_CANDIDATES)
        location_val = self._resolve_field(attrs, _LOCATION_CANDIDATES)
        report_num = self._resolve_field(attrs, _REPORT_NUM_CANDIDATES)

        return RawDocument(
            url=url,
            agency_id=self.agency_id,
            document_type=self._infer_document_type(type_val, url),
            title=type_val,
            raw_text=self._format_raw_text(attrs),
            published_date=date_val,
            source_metadata={
                "query_url": url,
                "offset": offset,
                "report_number": report_num,
                "location": location_val,
                "geometry": geometry,
            },
        )
