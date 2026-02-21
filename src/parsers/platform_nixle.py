"""
Nixle / Rave Alert Parser
=========================
Covers nixle.com (~23 agencies) and Rave Mobile Safety (~25 agencies).
RSS delegation: URL containing /rss or /feed → delegates to RSSParser.
Redirect handling: page.url captured after goto() detects Nixle→Rave migration.
Alert ID: extracted from /alert/{id}/ URL path for deduplication.
"""
import re
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from src.parsers.base import PLAYWRIGHT_SEMAPHORE, BaseParser, RawDocument
from src.parsers.platform_rss import RSSParser

USER_AGENT = "CadenceBot/1.0 (+https://github.com/cadence)"
PLAYWRIGHT_TIMEOUT = 10_000  # ms

WAIT_SELECTOR = ".alert-item, .alertItem, .alert-card, article"
ARTICLE_SELECTORS = [
    ".alert-item",    # Nixle modern
    ".nixle-alert",   # Nixle legacy
    ".alertItem",     # Rave / older Nixle
    ".alert-card",    # Rave card layout
    "article.alert",
    "article",
]
_DATE_FORMATS = (
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d",
    "%B %d, %Y", "%b %d, %Y", "%m/%d/%Y",
)
_RSS_PATTERN = re.compile(r"/(rss|feed)", re.IGNORECASE)
_ALERT_ID_PATTERN = re.compile(r"/alert/(\w+)/?")


class NixleParser(BaseParser):
    def __init__(self, agency_id: str) -> None:
        self.agency_id = agency_id

    async def fetch(self, url: str) -> list[RawDocument]:
        await self.rate_limit_delay()
        if _RSS_PATTERN.search(url):
            return await RSSParser(self.agency_id).fetch(url)

        async with PLAYWRIGHT_SEMAPHORE:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.set_extra_http_headers({"User-Agent": USER_AGENT})
                await page.goto(url, wait_until="domcontentloaded")
                try:
                    await page.wait_for_selector(WAIT_SELECTOR, timeout=PLAYWRIGHT_TIMEOUT)
                except Exception:
                    pass
                final_url = page.url        # captures post-redirect URL
                html = await page.content()
                await browser.close()
        return self._parse_html(html, final_url)

    def _parse_html(self, html: str, page_url: str) -> list[RawDocument]:
        soup = BeautifulSoup(html, "html.parser")
        articles = []
        for selector in ARTICLE_SELECTORS:
            articles = soup.select(selector)
            if articles:
                break
        platform = "nixle" if "nixle.com" in page_url else "rave"
        return [self._to_raw_document(tag, page_url, platform) for tag in articles]

    def _to_raw_document(self, article, page_url: str, platform: str) -> RawDocument:
        # URL + alert ID
        link_tag = article.find("a", href=_ALERT_ID_PATTERN) or article.find("a", href=True)
        alert_url = urljoin(page_url, link_tag["href"]) if link_tag and link_tag.get("href") else page_url
        m = _ALERT_ID_PATTERN.search(alert_url)
        alert_id = m.group(1) if m else None

        # Title
        title_tag = article.find("h2") or article.find("h3") or article.find("h4") or link_tag
        title = title_tag.get_text(strip=True) if title_tag else None

        # Body
        body_tag = (
            article.find(class_="alert-body") or article.find(class_="alertBody")
            or article.find(class_="alert-content") or article.find(class_="alertContent")
            or article.find("p")
        )
        body_text = body_tag.get_text(" ", strip=True) if body_tag else ""

        # Date — <time datetime="...">, named class, or bare text
        date_str = None
        time_tag = article.find("time")
        if time_tag:
            date_str = time_tag.get("datetime") or time_tag.get_text(strip=True)
        if not date_str:
            for cls in ("alert-date", "alertDate", "date", "published"):
                el = article.find(class_=cls)
                if el:
                    date_str = el.get_text(strip=True)
                    break

        raw_text = self.clean_whitespace(f"{title}. {body_text}" if title else body_text)
        return RawDocument(
            url=alert_url,
            agency_id=self.agency_id,
            document_type="alert",
            title=title or None,
            raw_text=raw_text,
            published_date=self._parse_date(date_str),
            source_metadata={"nixle_alert_id": alert_id, "alert_url": alert_url, "platform": platform},
        )

    def _parse_date(self, date_str: str | None) -> datetime | None:
        if not date_str:
            return None
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(date_str.strip())
        except (ValueError, TypeError):
            return None
