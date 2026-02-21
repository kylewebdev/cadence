"""PDF Parser
============
Handles PDF documents published by law enforcement agencies.

- Text-layer PDFs: extracted directly via pdfplumber
- Scanned/image PDFs: OCR fallback via pytesseract + pdf2image
- Multi-page daily logs: split at date boundaries
"""
import io
import re
from datetime import datetime
from pathlib import Path

import httpx
import pdfplumber

from src.parsers.base import BaseParser, RawDocument

# ---------------------------------------------------------------------------
# Optional OCR dependencies (names always defined so mocking always works)
# ---------------------------------------------------------------------------
pytesseract = None
convert_from_bytes = None

try:
    import pytesseract  # type: ignore[no-redef]
    from pdf2image import convert_from_bytes  # type: ignore[no-redef]

    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
OCR_DPI = 200
OCR_CHAR_THRESHOLD = 50
OCR_LOW_CONF_THRESHOLD = 60
REQUEST_TIMEOUT = 30.0
USER_AGENT = "CadenceBot/1.0 (+https://github.com/cadence)"

# ---------------------------------------------------------------------------
# Compiled regexes
# ---------------------------------------------------------------------------
_HYPHEN_RE = re.compile(r"(\w)-\n(\w)")
_PAGE_NUM_RE = re.compile(r"^\s*\d+\s*$", re.MULTILINE)
_FOOTER_RE = re.compile(r"^\s*[Pp]age\s+\d+\s+of\s+\d+\s*$", re.MULTILINE)

_WEEKDAYS = "MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY"
_MONTHS_UPPER = (
    "JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|"
    "SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER"
)
_MONTHS_TITLE = (
    "January|February|March|April|May|June|July|August|"
    "September|October|November|December"
)
_MONTHS_ABBR = "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"

# Standalone date header (full line: ^...$) — for raw multi-line text
_DATE_BOUNDARY_RE = re.compile(
    rf"""(?x)
    ^
    (?:
        (?:{_WEEKDAYS}),\s+
        (?:{_MONTHS_UPPER})\s+\d{{1,2}},\s+\d{{4}}
      | (?:{_MONTHS_TITLE})\s+\d{{1,2}},\s+\d{{4}}
      | (?:{_MONTHS_ABBR})\.?\s+\d{{1,2}},\s+\d{{4}}
      | \d{{1,2}}/\d{{1,2}}/\d{{4}}
    )
    \s*$""",
    re.MULTILINE | re.IGNORECASE,
)

# Date prefix match (start of string, no $ required) — used on cleaned text
_DATE_PREFIX_RE = re.compile(
    rf"""(?x)
    ^(?:
        (?:{_WEEKDAYS}),\s+
        (?:{_MONTHS_UPPER})\s+\d{{1,2}},\s+\d{{4}}
      | (?:{_MONTHS_TITLE})\s+\d{{1,2}},\s+\d{{4}}
      | (?:{_MONTHS_ABBR})\.?\s+\d{{1,2}},\s+\d{{4}}
      | \d{{1,2}}/\d{{1,2}}/\d{{4}}
    )""",
    re.IGNORECASE,
)

_WEEKDAY_PREFIX_RE = re.compile(rf"^(?:{_WEEKDAYS}),\s+", re.IGNORECASE)

_DATE_FORMATS = (
    "%B %d, %Y",   # January 15, 2024
    "%b %d, %Y",   # Jan 15, 2024
    "%b. %d, %Y",  # Jan. 15, 2024
    "%m/%d/%Y",    # 01/15/2024
)


class PDFParser(BaseParser):
    def __init__(self, agency_id: str) -> None:
        self.agency_id = agency_id

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def fetch(self, url: str) -> list[RawDocument]:
        try:
            await self.rate_limit_delay()
            pdf_bytes = await self._fetch_bytes(url)
            if pdf_bytes is None:
                return []
            page_texts_raw, ocr_pages = self._extract_pages_with_ocr(pdf_bytes)
            if not page_texts_raw:
                return []
            page_texts = [self._clean_page_text(t) for t in page_texts_raw]
            page_texts = [t for t in page_texts if t]
            if not page_texts:
                return []
            return self._assemble_documents(page_texts, url, ocr_pages)
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Fetch helpers
    # ------------------------------------------------------------------

    async def _fetch_bytes(self, url: str) -> bytes | None:
        if url.startswith("http://") or url.startswith("https://"):
            return await self._download(url)
        return self._read_local(url)

    async def _download(self, url: str) -> bytes | None:
        try:
            async with httpx.AsyncClient(
                follow_redirects=True, timeout=REQUEST_TIMEOUT
            ) as client:
                resp = await client.get(url, headers={"User-Agent": USER_AGENT})
            if resp.status_code != 200:
                return None
            return resp.content
        except Exception:
            return None

    def _read_local(self, path: str) -> bytes | None:
        try:
            return Path(path).read_bytes()
        except IOError:
            return None

    # ------------------------------------------------------------------
    # PDF extraction
    # ------------------------------------------------------------------

    def _extract_pages_with_ocr(
        self, pdf_bytes: bytes
    ) -> tuple[list[str], dict[int, float]]:
        """Extract text per page; fall back to OCR when text is sparse."""
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                page_texts: list[str] = []
                ocr_pages: dict[int, float] = {}
                for i, page in enumerate(pdf.pages):
                    text = page.extract_text() or ""
                    if len(text) < OCR_CHAR_THRESHOLD and OCR_AVAILABLE:
                        ocr_text, conf = self._ocr_page(pdf_bytes, i)
                        page_texts.append(ocr_text)
                        ocr_pages[i] = conf
                    else:
                        page_texts.append(text)
                return page_texts, ocr_pages
        except Exception:
            return [], {}

    def _ocr_page(
        self, pdf_bytes: bytes, page_index: int
    ) -> tuple[str, float]:
        images = convert_from_bytes(
            pdf_bytes,
            first_page=page_index + 1,
            last_page=page_index + 1,
            dpi=OCR_DPI,
        )
        if not images:
            return "", 0.0
        data = pytesseract.image_to_data(
            images[0], output_type=pytesseract.Output.DICT
        )
        text = pytesseract.image_to_string(images[0])
        confs = [c for c in data.get("conf", []) if c != -1]
        avg_conf = sum(confs) / len(confs) if confs else 0.0
        return text, avg_conf

    # ------------------------------------------------------------------
    # Text cleaning
    # ------------------------------------------------------------------

    def _clean_page_text(self, text: str) -> str:
        text = _HYPHEN_RE.sub(r"\1\2", text)
        text = _PAGE_NUM_RE.sub("", text)
        text = _FOOTER_RE.sub("", text)
        return self.clean_whitespace(text)

    # ------------------------------------------------------------------
    # Date header detection
    # ------------------------------------------------------------------

    def _find_date_header(
        self, page_text: str
    ) -> tuple[str | None, datetime | None]:
        """Check if page_text starts with a date header."""
        m = _DATE_PREFIX_RE.match(page_text)
        if m:
            header = m.group(0).strip()
            parsed = self._parse_date_from_header(header)
            return header, parsed
        return None, None

    def _parse_date_from_header(self, line: str) -> datetime | None:
        cleaned = _WEEKDAY_PREFIX_RE.sub("", line).strip()
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
        return None

    # ------------------------------------------------------------------
    # OCR metadata
    # ------------------------------------------------------------------

    def _build_ocr_metadata(
        self, ocr_pages: dict[int, float], page_indices: list[int]
    ) -> dict:
        ocr_in_range = [i for i in page_indices if i in ocr_pages]
        if not ocr_in_range:
            return {}
        avg_conf = sum(ocr_pages[i] for i in ocr_in_range) / len(ocr_in_range)
        return {
            "is_ocr": True,
            "avg_ocr_confidence": round(avg_conf, 1),
            "ocr_confidence": "low" if avg_conf < OCR_LOW_CONF_THRESHOLD else "ok",
        }

    # ------------------------------------------------------------------
    # Document assembly
    # ------------------------------------------------------------------

    def _assemble_documents(
        self,
        page_texts: list[str],
        url: str,
        ocr_pages: dict[int, float],
    ) -> list[RawDocument]:
        headers = [self._find_date_header(t) for t in page_texts]
        if page_texts and all(h is not None for h, _ in headers):
            return self._split_documents(page_texts, url, ocr_pages, headers)
        return [self._make_single_document(page_texts, url, ocr_pages)]

    def _split_documents(
        self,
        page_texts: list[str],
        url: str,
        ocr_pages: dict[int, float],
        headers: list[tuple[str | None, datetime | None]],
    ) -> list[RawDocument]:
        docs = []
        for idx, (text, (header, parsed_date)) in enumerate(
            zip(page_texts, headers)
        ):
            meta: dict = {
                "pdf_url": url,
                "section_index": idx,
                "page_count": 1,
            }
            meta.update(self._build_ocr_metadata(ocr_pages, [idx]))
            docs.append(
                RawDocument(
                    url=url,
                    agency_id=self.agency_id,
                    document_type=self._infer_document_type(url),
                    title=header,
                    raw_text=text,
                    published_date=parsed_date,
                    source_metadata=meta,
                )
            )
        return docs

    def _make_single_document(
        self,
        page_texts: list[str],
        url: str,
        ocr_pages: dict[int, float],
    ) -> RawDocument:
        combined = self.clean_whitespace("\n\n".join(page_texts))
        meta: dict = {
            "pdf_url": url,
            "page_count": len(page_texts),
        }
        meta.update(
            self._build_ocr_metadata(ocr_pages, list(range(len(page_texts))))
        )
        return RawDocument(
            url=url,
            agency_id=self.agency_id,
            document_type=self._infer_document_type(url),
            title=None,
            raw_text=combined,
            published_date=None,
            source_metadata=meta,
        )

    # ------------------------------------------------------------------
    # Document type inference
    # ------------------------------------------------------------------

    def _infer_document_type(self, url: str) -> str:
        lower = url.lower()
        if "arrest" in lower or "blotter" in lower:
            return "arrest_log"
        if "press" in lower or "release" in lower:
            return "press_release"
        if "activity" in lower or "log" in lower:
            return "daily_activity_log"
        return "pdf_library"
