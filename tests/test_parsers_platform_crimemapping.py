from contextlib import asynccontextmanager
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.parsers.platform_crimemapping import CrimeMappingParser

# --- Fixtures / helpers ---

SAMPLE_INCIDENT = {
    "TypeDescription": "Burglary",
    "Address": "100 Main St",
    "DateOccurred": "01/15/2024 09:30:00 AM",
    "CaseNumber": "24-001234",
    "Description": "Residential burglary.",
}


def make_parser(agency_id="lapd", crimemapping_id=1234, days=14):
    return CrimeMappingParser(agency_id, crimemapping_id, days)


def _make_playwright_mock(incidents: list[dict]):
    """Return a patch target and mock for async_playwright that yields given incidents."""
    mock_response = AsyncMock()
    mock_response.json = AsyncMock(
        return_value={"Incidents": incidents, "TotalRecords": len(incidents)}
    )

    async def _response_value():
        return mock_response

    mock_response_info = MagicMock()
    mock_response_info.value = _response_value()

    mock_page = AsyncMock()

    @asynccontextmanager
    async def _expect_response(predicate):
        yield mock_response_info

    mock_page.expect_response = _expect_response
    mock_page.goto = AsyncMock()

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)
    mock_browser.close = AsyncMock()

    mock_p = MagicMock()
    mock_p.chromium.launch = AsyncMock(return_value=mock_browser)

    @asynccontextmanager
    async def _async_playwright():
        yield mock_p

    return _async_playwright


# --- fetch() tests (mocked Playwright) ---


@pytest.mark.asyncio
async def test_basic_fetch():
    incidents = [
        SAMPLE_INCIDENT,
        {
            "TypeDescription": "Theft",
            "Address": "200 Oak Ave",
            "DateOccurred": "01/16/2024 02:00:00 PM",
            "CaseNumber": "24-001235",
            "Description": "Vehicle theft.",
        },
    ]
    mock_playwright = _make_playwright_mock(incidents)

    with patch(
        "src.parsers.platform_crimemapping.async_playwright", mock_playwright
    ), patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("")

    assert len(docs) == 2
    assert docs[0].title == "Burglary"
    assert docs[0].agency_id == "lapd"
    assert docs[0].document_type == "incident_log"
    assert docs[1].title == "Theft"


@pytest.mark.asyncio
async def test_empty_incidents_returns_empty_list():
    mock_playwright = _make_playwright_mock([])

    with patch(
        "src.parsers.platform_crimemapping.async_playwright", mock_playwright
    ), patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser()
        docs = await parser.fetch("")

    assert docs == []


@pytest.mark.asyncio
async def test_rate_limit_called():
    mock_playwright = _make_playwright_mock([SAMPLE_INCIDENT])

    with patch(
        "src.parsers.platform_crimemapping.async_playwright", mock_playwright
    ), patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        parser = make_parser()
        await parser.fetch("")

    mock_sleep.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_agency_id_propagated():
    incidents = [SAMPLE_INCIDENT, {**SAMPLE_INCIDENT, "TypeDescription": "Assault"}]
    mock_playwright = _make_playwright_mock(incidents)

    with patch(
        "src.parsers.platform_crimemapping.async_playwright", mock_playwright
    ), patch("asyncio.sleep", new_callable=AsyncMock):
        parser = make_parser(agency_id="test-agency-456")
        docs = await parser.fetch("")

    assert all(doc.agency_id == "test-agency-456" for doc in docs)


# --- _to_raw_document() tests (no mock needed) ---


def test_document_type_is_incident_log():
    parser = make_parser()
    doc = parser._to_raw_document(SAMPLE_INCIDENT)
    assert doc.document_type == "incident_log"


def test_title_is_type_description():
    parser = make_parser()
    doc = parser._to_raw_document(SAMPLE_INCIDENT)
    assert doc.title == "Burglary"


def test_raw_text_contains_type_address_date():
    parser = make_parser()
    doc = parser._to_raw_document(SAMPLE_INCIDENT)
    assert "Burglary" in doc.raw_text
    assert "100 Main St" in doc.raw_text
    assert "01/15/2024" in doc.raw_text


def test_missing_description_still_returns_doc():
    incident = {k: v for k, v in SAMPLE_INCIDENT.items() if k != "Description"}
    parser = make_parser()
    doc = parser._to_raw_document(incident)
    assert doc is not None
    assert doc.document_type == "incident_log"


def test_missing_date_is_none():
    incident = {k: v for k, v in SAMPLE_INCIDENT.items() if k != "DateOccurred"}
    parser = make_parser()
    doc = parser._to_raw_document(incident)
    assert doc.published_date is None


def test_source_metadata_has_crimemapping_id():
    parser = make_parser(crimemapping_id=1234)
    doc = parser._to_raw_document(SAMPLE_INCIDENT)
    assert doc.source_metadata["crimemapping_id"] == 1234


def test_case_number_in_source_metadata():
    parser = make_parser()
    doc = parser._to_raw_document(SAMPLE_INCIDENT)
    assert doc.source_metadata["case_number"] == "24-001234"


# --- _parse_date() tests (no mock needed) ---


def test_date_parsing_isoformat():
    parser = make_parser()
    result = parser._parse_date("2024-01-15T09:30:00")
    assert result == datetime(2024, 1, 15, 9, 30, 0)


def test_date_parsing_mm_dd_yyyy():
    parser = make_parser()
    result = parser._parse_date("01/15/2024 09:30:00 AM")
    assert result == datetime(2024, 1, 15, 9, 30, 0)


def test_date_parsing_invalid_returns_none():
    parser = make_parser()
    result = parser._parse_date("not-a-date")
    assert result is None


def test_date_parsing_none_input():
    parser = make_parser()
    result = parser._parse_date(None)
    assert result is None
