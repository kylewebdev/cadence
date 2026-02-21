from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.parsers.platform_civicplus import CivicPlusParser, _build_page_url

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "civicplus"


def load_fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text()


def make_parser(agency_id: str = "alhambra-pd", max_pages: int = 5) -> CivicPlusParser:
    return CivicPlusParser(agency_id, max_pages)


# Minimal HTML page that contains one CivicPlus list-group-item article
_SINGLE_ARTICLE_HTML = """<!DOCTYPE html>
<html><body>
<ul>
<li class="list-group-item" id="list-articles-category-1-42">
  <div class="border-article article-link">
    <div class="d-flex flex-column">
      <div id="article-42-cat-1-main-content">
        <h3 class="article-list-header" id="article-42-cat-1-title">
          <a class="article-title-link" href="/m/newsflash/Home/Detail/42">
            Sample Press Release
          </a>
        </h3>
        <div class="article-preview">
          Police Department announces new community program.
        </div>
      </div>
      <div class="article-list-footer" id="article-42-cat-1-footer">
        <div class="fst-italic">Posted on January 15, 2024</div>
      </div>
    </div>
  </div>
</li>
</ul>
</body></html>"""

_SECOND_PAGE_HTML = """<!DOCTYPE html>
<html><body>
<ul>
<li class="list-group-item" id="list-articles-category-1-43">
  <div class="border-article article-link">
    <div class="d-flex flex-column">
      <div>
        <h3 class="article-list-header">
          <a class="article-title-link" href="/m/newsflash/Home/Detail/43">
            Second Page Article
          </a>
        </h3>
        <div class="article-preview">Second page content.</div>
      </div>
      <div class="article-list-footer">
        <div class="fst-italic">Posted on February 01, 2024</div>
      </div>
    </div>
  </div>
</li>
</ul>
</body></html>"""

# Page with a "next" pagination link
_PAGE_WITH_NEXT_HTML = _SINGLE_ARTICLE_HTML.replace(
    "</body>",
    '<a rel="next" href="?Page=2">Next</a></body>',
)

_EMPTY_HTML = """<!DOCTYPE html>
<html><body>
<div id="contentarea"><!-- no articles --></div>
</body></html>"""


# ---------------------------------------------------------------------------
# Fixture-based tests (synchronous, no mocks needed)
# ---------------------------------------------------------------------------


def test_parse_page_alhambra():
    html = load_fixture("alhambra_p1.html")
    parser = make_parser()
    docs = parser._parse_page(html, "https://www.alhambraca.gov/CivicAlerts.aspx")
    assert len(docs) >= 1
    for doc in docs:
        assert doc.title is not None
        assert doc.url is not None


def test_parse_page_modesto():
    html = load_fixture("modesto_p1.html")
    parser = make_parser(agency_id="modesto-pd")
    docs = parser._parse_page(html, "https://www.modestogov.com/CivicAlerts.aspx")
    assert len(docs) >= 1
    for doc in docs:
        assert doc.title is not None


def test_parse_page_empty_html_returns_empty_list():
    parser = make_parser()
    docs = parser._parse_page(_EMPTY_HTML, "https://example.com/CivicAlerts.aspx")
    assert docs == []


def test_agency_id_propagated():
    html = load_fixture("alhambra_p1.html")
    parser = make_parser(agency_id="alhambra-police-department")
    docs = parser._parse_page(html, "https://www.alhambraca.gov/CivicAlerts.aspx")
    assert len(docs) >= 1
    assert all(doc.agency_id == "alhambra-police-department" for doc in docs)


def test_source_metadata_has_page_url():
    html = load_fixture("alhambra_p1.html")
    parser = make_parser()
    page_url = "https://www.alhambraca.gov/CivicAlerts.aspx"
    docs = parser._parse_page(html, page_url)
    assert len(docs) >= 1
    assert all(doc.source_metadata["page_url"] == page_url for doc in docs)


def test_missing_date_is_none():
    no_date_html = """<!DOCTYPE html>
<html><body>
<ul>
<li class="list-group-item">
  <h3 class="article-list-header">
    <a class="article-title-link" href="/detail/1">No Date Article</a>
  </h3>
  <div class="article-preview">Some content.</div>
</li>
</ul>
</body></html>"""
    parser = make_parser()
    docs = parser._parse_page(no_date_html, "https://example.com/alerts")
    assert len(docs) == 1
    assert docs[0].published_date is None


# ---------------------------------------------------------------------------
# _build_page_url helper tests
# ---------------------------------------------------------------------------


def test_build_page_url_page_1_returns_base():
    assert _build_page_url("https://example.com/alerts", 1) == "https://example.com/alerts"


def test_build_page_url_page_2_adds_param():
    assert _build_page_url("https://example.com/alerts", 2) == "https://example.com/alerts?Page=2"


def test_build_page_url_page_2_with_existing_query():
    url = "https://example.com/alerts?Cat=Police"
    assert _build_page_url(url, 2) == "https://example.com/alerts?Cat=Police&Page=2"


# ---------------------------------------------------------------------------
# _infer_document_type tests
# ---------------------------------------------------------------------------


def test_document_type_press_release():
    parser = make_parser()
    assert parser._infer_document_type("https://example.com/PressReleases.aspx") == "press_release"


def test_document_type_arrest_log():
    parser = make_parser()
    assert parser._infer_document_type("https://example.com/ArrestBlotter.aspx") == "arrest_log"


def test_document_type_default():
    parser = make_parser()
    assert parser._infer_document_type("https://example.com/CivicAlerts.aspx") == "activity_feed"


# ---------------------------------------------------------------------------
# _parse_date tests
# ---------------------------------------------------------------------------


def test_date_parsing_long_format():
    parser = make_parser()
    assert parser._parse_date("January 15, 2024") == datetime(2024, 1, 15)


def test_date_parsing_short_month():
    parser = make_parser()
    assert parser._parse_date("Jan 15, 2024") == datetime(2024, 1, 15)


def test_date_parsing_iso():
    parser = make_parser()
    assert parser._parse_date("2024-01-15") == datetime(2024, 1, 15)


def test_date_parsing_posted_prefix():
    parser = make_parser()
    assert parser._parse_date("Posted on January 15, 2024") == datetime(2024, 1, 15)


def test_date_parsing_last_updated_prefix():
    parser = make_parser()
    assert parser._parse_date("Last Updated on February 01, 2024") == datetime(2024, 2, 1)


def test_date_parsing_with_trailing_pipe():
    parser = make_parser()
    result = parser._parse_date("Posted on June 25, 2025 | Last Updated on June 26, 2025")
    assert result == datetime(2025, 6, 25)


def test_date_parsing_invalid_returns_none():
    parser = make_parser()
    assert parser._parse_date("not-a-date") is None


def test_date_parsing_none_input():
    parser = make_parser()
    assert parser._parse_date(None) is None


# ---------------------------------------------------------------------------
# Fetch tests (mocked httpx)
# ---------------------------------------------------------------------------


def _make_httpx_mock(responses: list[tuple[int, str]]):
    """
    Build a mock httpx.AsyncClient that returns the given (status, text) pairs
    in sequence on each GET call.
    """
    call_count = 0

    class MockResponse:
        def __init__(self, status_code: int, text: str):
            self.status_code = status_code
            self.text = text

    class MockAsyncClient:
        def __init__(self, **kwargs):  # accept follow_redirects, timeout, etc.
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url, headers=None):
            nonlocal call_count
            if call_count < len(responses):
                status, text = responses[call_count]
            else:
                status, text = 404, ""
            call_count += 1
            return MockResponse(status, text)

    return MockAsyncClient, lambda: call_count


@pytest.mark.asyncio
async def test_fetch_paginates_two_pages():
    """Parser should concatenate docs from both pages when next link present on p1."""
    # Page 1 has a "next" rel link; page 2 does not.
    p1 = _PAGE_WITH_NEXT_HTML
    p2 = _SECOND_PAGE_HTML

    MockClient, _ = _make_httpx_mock([(200, p1), (200, p2)])

    with patch("src.parsers.platform_civicplus.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("https://example.com/alerts")

    assert len(docs) == 2
    titles = [d.title for d in docs]
    assert any("Sample" in t for t in titles)
    assert any("Second" in t for t in titles)


@pytest.mark.asyncio
async def test_fetch_stops_at_empty_page():
    """When page 2 has no articles, only page 1 docs are returned."""
    p1 = _PAGE_WITH_NEXT_HTML
    p2 = _EMPTY_HTML

    MockClient, _ = _make_httpx_mock([(200, p1), (200, p2)])

    with patch("src.parsers.platform_civicplus.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("https://example.com/alerts")

    assert len(docs) == 1
    assert docs[0].title is not None


@pytest.mark.asyncio
async def test_fetch_respects_max_pages():
    """With max_pages=2, even if page 2 has a next link, page 3 is not fetched."""
    # All three pages have articles and a next link
    page_with_next = _PAGE_WITH_NEXT_HTML

    call_count = 0

    class MockAsyncClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url, headers=None):
            nonlocal call_count
            call_count += 1

            class R:
                status_code = 200
                text = page_with_next

            return R()

    with patch("src.parsers.platform_civicplus.httpx.AsyncClient", MockAsyncClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser(max_pages=2)
        docs = await parser.fetch("https://example.com/alerts")

    assert call_count == 2
    assert len(docs) == 2


@pytest.mark.asyncio
async def test_fetch_stops_when_no_next_link():
    """Single page with no next link — only one HTTP request."""
    MockClient, get_calls = _make_httpx_mock([(200, _SINGLE_ARTICLE_HTML)])

    with patch("src.parsers.platform_civicplus.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("https://example.com/alerts")

    assert get_calls() == 1
    assert len(docs) == 1


@pytest.mark.asyncio
async def test_fetch_http_error_returns_empty():
    """Non-200 on page 1 returns empty list."""
    MockClient, _ = _make_httpx_mock([(500, "")])

    with patch("src.parsers.platform_civicplus.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("https://example.com/alerts")

    assert docs == []


@pytest.mark.asyncio
async def test_rate_limit_called():
    """asyncio.sleep(1.0) is called exactly once per fetch()."""
    MockClient, _ = _make_httpx_mock([(200, _SINGLE_ARTICLE_HTML)])

    with patch("src.parsers.platform_civicplus.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        parser = make_parser()
        await parser.fetch("https://example.com/alerts")

    mock_sleep.assert_awaited_once_with(1.0)
