from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.parsers.platform_nixle import NixleParser

FIXTURES = Path(__file__).parent / "fixtures"
NIXLE_HTML = (FIXTURES / "nixle" / "alerts_listing.html").read_text()
RAVE_HTML = (FIXTURES / "rave" / "alerts_listing.html").read_text()


def make_parser(agency_id="test-agency"):
    return NixleParser(agency_id)


def _make_playwright_mock(
    html,
    final_url="https://www.nixle.com/agency/alerts/",
    selector_found=True,
):
    mock_page = AsyncMock()
    mock_page.goto = AsyncMock()
    mock_page.set_extra_http_headers = AsyncMock()
    if selector_found:
        mock_page.wait_for_selector = AsyncMock(return_value=None)
    else:
        mock_page.wait_for_selector = AsyncMock(side_effect=Exception("Timeout"))
    mock_page.url = final_url          # plain string, NOT AsyncMock
    mock_page.content = AsyncMock(return_value=html)

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)
    mock_browser.close = AsyncMock()

    mock_p = MagicMock()
    mock_p.chromium.launch = AsyncMock(return_value=mock_browser)

    @asynccontextmanager
    async def _async_playwright():
        yield mock_p

    return _async_playwright


# --- fetch() tests (Playwright mocked) ---


@pytest.mark.asyncio
async def test_rss_url_delegates_to_rss_parser():
    mock_rss = AsyncMock(return_value=[])
    with patch("src.parsers.platform_nixle.RSSParser") as MockRSS, \
         patch("asyncio.sleep", new_callable=AsyncMock):
        MockRSS.return_value.fetch = mock_rss
        parser = make_parser()
        await parser.fetch("https://www.nixle.com/agency/12345/rss/")
    MockRSS.assert_called_once_with("test-agency")
    mock_rss.assert_awaited_once_with("https://www.nixle.com/agency/12345/rss/")


@pytest.mark.asyncio
async def test_feed_url_delegates_to_rss_parser():
    mock_rss = AsyncMock(return_value=[])
    with patch("src.parsers.platform_nixle.RSSParser") as MockRSS, \
         patch("asyncio.sleep", new_callable=AsyncMock):
        MockRSS.return_value.fetch = mock_rss
        parser = make_parser()
        await parser.fetch("https://www.nixle.com/agency/12345/Feed/")
    MockRSS.assert_called_once_with("test-agency")
    mock_rss.assert_awaited_once_with("https://www.nixle.com/agency/12345/Feed/")


@pytest.mark.asyncio
async def test_basic_nixle_fetch_returns_documents():
    mock_playwright = _make_playwright_mock(
        NIXLE_HTML, final_url="https://www.nixle.com/agency/alerts/"
    )
    with patch("src.parsers.platform_nixle.async_playwright", mock_playwright), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("https://www.nixle.com/agency/alerts/")
    assert len(docs) >= 2


@pytest.mark.asyncio
async def test_empty_page_returns_empty_list():
    empty_html = "<html><body><div id='no-alerts'></div></body></html>"
    mock_playwright = _make_playwright_mock(empty_html)
    with patch("src.parsers.platform_nixle.async_playwright", mock_playwright), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("https://www.nixle.com/agency/alerts/")
    assert docs == []


@pytest.mark.asyncio
async def test_rate_limit_called_once():
    mock_playwright = _make_playwright_mock(NIXLE_HTML)
    with patch("src.parsers.platform_nixle.async_playwright", mock_playwright), \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        parser = make_parser()
        await parser.fetch("https://www.nixle.com/agency/alerts/")
    mock_sleep.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_agency_id_propagated_to_all_docs():
    mock_playwright = _make_playwright_mock(NIXLE_HTML)
    with patch("src.parsers.platform_nixle.async_playwright", mock_playwright), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser(agency_id="riverside-pd")
        docs = await parser.fetch("https://www.nixle.com/agency/alerts/")
    assert len(docs) > 0
    assert all(doc.agency_id == "riverside-pd" for doc in docs)


@pytest.mark.asyncio
async def test_document_type_is_alert():
    mock_playwright = _make_playwright_mock(NIXLE_HTML)
    with patch("src.parsers.platform_nixle.async_playwright", mock_playwright), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("https://www.nixle.com/agency/alerts/")
    assert len(docs) > 0
    assert all(doc.document_type == "alert" for doc in docs)


@pytest.mark.asyncio
async def test_alert_id_extracted_from_url():
    mock_playwright = _make_playwright_mock(NIXLE_HTML)
    with patch("src.parsers.platform_nixle.async_playwright", mock_playwright), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("https://www.nixle.com/agency/alerts/")
    assert len(docs) > 0
    assert all(doc.source_metadata["nixle_alert_id"] is not None for doc in docs)


@pytest.mark.asyncio
async def test_nixle_page_sets_platform_nixle():
    mock_playwright = _make_playwright_mock(
        NIXLE_HTML, final_url="https://www.nixle.com/agency/alerts/"
    )
    with patch("src.parsers.platform_nixle.async_playwright", mock_playwright), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("https://www.nixle.com/agency/alerts/")
    assert len(docs) > 0
    assert all(doc.source_metadata["platform"] == "nixle" for doc in docs)


@pytest.mark.asyncio
async def test_rave_redirect_sets_platform_rave():
    mock_playwright = _make_playwright_mock(
        RAVE_HTML, final_url="https://alerts.ravemobilesafety.com/alerts/"
    )
    with patch("src.parsers.platform_nixle.async_playwright", mock_playwright), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("https://www.nixle.com/agency/alerts/")
    assert len(docs) > 0
    assert all(doc.source_metadata["platform"] == "rave" for doc in docs)


@pytest.mark.asyncio
async def test_selector_timeout_still_parses():
    mock_playwright = _make_playwright_mock(NIXLE_HTML, selector_found=False)
    with patch("src.parsers.platform_nixle.async_playwright", mock_playwright), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("https://www.nixle.com/agency/alerts/")
    assert len(docs) >= 2


# --- _parse_html() / _to_raw_document() unit tests (no mock) ---


def test_missing_date_is_none():
    html = """
    <div class="alert-item">
      <h2><a href="/alert/999/">Test Alert</a></h2>
      <div class="alert-body">Some content here.</div>
    </div>
    """
    parser = make_parser()
    docs = parser._parse_html(html, "https://www.nixle.com/alerts/")
    assert len(docs) == 1
    assert docs[0].published_date is None


def test_missing_title_is_none():
    html = """
    <div class="alert-item">
      <div class="alert-body">Body only, no heading.</div>
      <time datetime="2024-01-15">Jan 15, 2024</time>
    </div>
    """
    parser = make_parser()
    docs = parser._parse_html(html, "https://www.nixle.com/alerts/")
    assert len(docs) == 1
    assert docs[0].title is None
    assert "Body only" in docs[0].raw_text


def test_missing_body_graceful():
    html = """
    <div class="alert-item">
      <h2><a href="/alert/888/">Title Only Alert</a></h2>
    </div>
    """
    parser = make_parser()
    docs = parser._parse_html(html, "https://www.nixle.com/alerts/")
    assert len(docs) == 1
    assert docs[0].title == "Title Only Alert"
    assert "Title Only Alert" in docs[0].raw_text


# --- _parse_date() unit tests ---


def test_parse_date_iso():
    parser = make_parser()
    result = parser._parse_date("2024-01-15T09:30:00")
    assert result == datetime(2024, 1, 15, 9, 30, 0)


def test_parse_date_date_only():
    parser = make_parser()
    result = parser._parse_date("2024-01-15")
    assert result == datetime(2024, 1, 15)


def test_parse_date_long_month():
    parser = make_parser()
    result = parser._parse_date("January 15, 2024")
    assert result == datetime(2024, 1, 15)


def test_parse_date_short_month():
    parser = make_parser()
    result = parser._parse_date("Jan 15, 2024")
    assert result == datetime(2024, 1, 15)


def test_parse_date_mm_dd_yyyy():
    parser = make_parser()
    result = parser._parse_date("01/15/2024")
    assert result == datetime(2024, 1, 15)


def test_parse_date_invalid_returns_none():
    parser = make_parser()
    result = parser._parse_date("not-a-date")
    assert result is None


def test_parse_date_none_input():
    parser = make_parser()
    result = parser._parse_date(None)
    assert result is None


def test_parse_date_empty_string():
    parser = make_parser()
    result = parser._parse_date("")
    assert result is None
