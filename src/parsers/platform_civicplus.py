"""
CivicPlus CMS Parser
====================
Serves server-rendered HTML — httpx is sufficient (no Playwright needed).
Uses Bootstrap/custom CSS classes confirmed against real agency pages:

Article container : li.list-group-item
Title             : a.article-title-link  (inside h3.article-list-header)
Body preview      : div.article-preview
Date              : div.article-list-footer .fst-italic
                    text like "Posted on July 12, 2021"
Pagination        : ?Page=N  (stops when no "Next" link or page is empty)
"""
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.parsers.base import BaseParser, RawDocument

USER_AGENT = "CadenceBot/1.0 (+https://github.com/cadence)"
REQUEST_TIMEOUT = 30.0

# CSS selector cascade — tried in order, first non-empty result wins
ARTICLE_SELECTORS = [
    "li.list-group-item",           # Modern CivicPlus (Bootstrap) layout
    ".civicAlert",                  # Legacy CivicPlus layout
    ".alertItem",                   # Variant A
    "#fa_newslist .fa_rowcmp",      # Older CivicPlus layout
    "article",                      # Generic fallback
]

_DATE_FORMATS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
    "%B %d, %Y",
    "%b %d, %Y",
    "%m/%d/%Y",
)


def _build_page_url(base_url: str, page_num: int) -> str:
    if page_num == 1:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}Page={page_num}"


class CivicPlusParser(BaseParser):
    def __init__(self, agency_id: str, max_pages: int = 5) -> None:
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
            resp = await client.get(url, headers={"User-Agent": USER_AGENT})
        return resp.text if resp.status_code == 200 else None

    def _parse_page(self, html: str, page_url: str) -> list[RawDocument]:
        soup = BeautifulSoup(html, "html.parser")
        articles = []
        for selector in ARTICLE_SELECTORS:
            articles = soup.select(selector)
            if articles:
                break
        return [self._to_raw_document(tag, page_url) for tag in articles]

    def _has_next_page(self, html: str) -> bool:
        soup = BeautifulSoup(html, "html.parser")
        if soup.find("a", rel="next"):
            return True
        if soup.find("a", string=re.compile(r"\bnext\b", re.I)):
            return True
        return False

    def _to_raw_document(self, article, page_url: str) -> RawDocument:
        # URL
        link_tag = article.find("a", class_="article-title-link") or article.find(
            "a", href=True
        )
        if link_tag and link_tag.get("href"):
            doc_url = urljoin(page_url, link_tag["href"])
        else:
            doc_url = page_url

        # Title
        title_tag = (
            article.find("h2")
            or article.find("h3")
            or article.find("h4")
            or link_tag
        )
        title = title_tag.get_text(strip=True) if title_tag else None

        # Body
        body_tag = article.find(class_="article-preview") or article.find(
            class_="alertContent"
        ) or article.find("p")
        body_text = body_tag.get_text(" ", strip=True) if body_tag else ""

        # Date
        footer = article.find(class_="article-list-footer")
        date_str: str | None = None
        if footer:
            italic = footer.find(class_="fst-italic")
            if italic:
                date_str = italic.get_text(strip=True)
        if not date_str:
            time_tag = article.find("time")
            if time_tag:
                date_str = time_tag.get("datetime") or time_tag.get_text(strip=True)
        if not date_str:
            for cls in ("date", "alertDate"):
                el = article.find(class_=cls)
                if el:
                    date_str = el.get_text(strip=True)
                    break

        raw_text = self.clean_whitespace(
            f"{title}. {body_text}" if title else body_text
        )

        return RawDocument(
            url=doc_url,
            agency_id=self.agency_id,
            document_type=self._infer_document_type(page_url),
            title=title or None,
            raw_text=raw_text,
            published_date=self._parse_date(date_str),
            source_metadata={"page_url": page_url},
        )

    def _infer_document_type(self, url: str) -> str:
        lower = url.lower()
        if "blotter" in lower or "arrest" in lower:
            return "arrest_log"
        if "press" in lower or "release" in lower:
            return "press_release"
        return "activity_feed"

    def _parse_date(self, date_str: str | None) -> datetime | None:
        if not date_str:
            return None
        # Strip common prefixes like "Posted on", "Updated:", etc.
        cleaned = re.sub(
            r"^(posted on|last updated on|updated?:?)\s*",
            "",
            date_str,
            flags=re.I,
        ).strip()
        # Also strip trailing pipe-separated suffix e.g. "| Last Updated on ..."
        cleaned = re.sub(r"\s*\|.*$", "", cleaned).strip()
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(cleaned)
        except (ValueError, TypeError):
            return None
