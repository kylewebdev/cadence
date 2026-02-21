from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from src.parsers.platform_socrata import PAGE_SIZE, SocrataParser


# ---------------------------------------------------------------------------
# Mock httpx infrastructure
# ---------------------------------------------------------------------------


def _make_mock_client(responses: list):
    """
    responses: list of (status_code, body) where body is a dict/list (JSON)
    or an Exception to raise.
    """
    call_count = 0

    class MockResponse:
        def __init__(self, status_code: int, body):
            self.status_code = status_code
            self._body = body

        def json(self):
            return self._body

    class MockAsyncClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url, params=None, headers=None):
            nonlocal call_count
            if call_count < len(responses):
                item = responses[call_count]
            else:
                item = (404, [])
            call_count += 1
            if isinstance(item, Exception):
                raise item
            status, body = item
            return MockResponse(status, body)

    return MockAsyncClient, lambda: call_count


def _make_rows(n: int, offset: int = 0) -> list[dict]:
    return [
        {
            "dr_no": f"{offset + i}",
            "date_occ": "2024-01-15T08:00:00",
            "crm_cd_desc": "BURGLARY",
            "location": "100 MAIN ST",
        }
        for i in range(n)
    ]


def _make_parser(**kwargs) -> SocrataParser:
    return SocrataParser(agency_id="test-pd", **kwargs)


# ---------------------------------------------------------------------------
# Unit tests — single page
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_single_page_returns_correct_count():
    rows = _make_rows(5)
    MockClient, get_calls = _make_mock_client([(200, rows)])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://data.example.com/resource/abcd-1234.json")
    assert len(docs) == 5
    assert get_calls() == 1


@pytest.mark.asyncio
async def test_single_page_no_second_request():
    rows = _make_rows(3)
    MockClient, get_calls = _make_mock_client([(200, rows)])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        await _make_parser().fetch("https://data.example.com/resource/abcd-1234.json")
    assert get_calls() == 1


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pagination_full_first_batch_triggers_second_request():
    page1 = _make_rows(PAGE_SIZE)
    page2 = _make_rows(3, offset=PAGE_SIZE)
    MockClient, get_calls = _make_mock_client([(200, page1), (200, page2)])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://data.example.com/resource/abcd-1234.json")
    assert get_calls() == 2
    assert len(docs) == PAGE_SIZE + 3


@pytest.mark.asyncio
async def test_pagination_stops_on_partial_batch():
    page1 = _make_rows(PAGE_SIZE)
    page2 = _make_rows(50, offset=PAGE_SIZE)
    MockClient, get_calls = _make_mock_client([(200, page1), (200, page2)])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://data.example.com/resource/abcd-1234.json")
    assert get_calls() == 2
    assert len(docs) == PAGE_SIZE + 50


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_dataset_returns_empty_list():
    MockClient, _ = _make_mock_client([(200, [])])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://data.example.com/resource/abcd-1234.json")
    assert docs == []


@pytest.mark.asyncio
async def test_http_500_returns_empty_list():
    MockClient, _ = _make_mock_client([(500, [])])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://data.example.com/resource/abcd-1234.json")
    assert docs == []


@pytest.mark.asyncio
async def test_socrata_error_dict_returns_empty_list():
    error_body = {"message": "Forbidden", "errorCode": 403}
    MockClient, _ = _make_mock_client([(200, error_body)])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://data.example.com/resource/abcd-1234.json")
    assert docs == []


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rate_limit_single_page_sleeps_once():
    MockClient, _ = _make_mock_client([(200, _make_rows(5))])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await _make_parser().fetch("https://data.example.com/resource/abcd-1234.json")
    mock_sleep.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_rate_limit_two_pages_sleeps_twice():
    page1 = _make_rows(PAGE_SIZE)
    page2 = _make_rows(3)
    MockClient, _ = _make_mock_client([(200, page1), (200, page2)])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await _make_parser().fetch("https://data.example.com/resource/abcd-1234.json")
    assert mock_sleep.await_count == 2


# ---------------------------------------------------------------------------
# agency_id propagation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agency_id_propagated_to_all_docs():
    rows = _make_rows(3)
    MockClient, _ = _make_mock_client([(200, rows)])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = SocrataParser(agency_id="lapd")
        docs = await parser.fetch("https://data.example.com/resource/abcd-1234.json")
    assert all(d.agency_id == "lapd" for d in docs)


# ---------------------------------------------------------------------------
# $where param
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_where_param_present_when_date_field_set():
    rows = _make_rows(1)
    captured_params = {}

    class CapturingClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url, params=None, headers=None):
            captured_params.update(params or {})

            class R:
                status_code = 200

                def json(self_):
                    return rows

            return R()

    with patch("src.parsers.platform_socrata.httpx.AsyncClient", CapturingClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = SocrataParser(agency_id="test", date_field="date_occ")
        await parser.fetch("https://data.example.com/resource/abcd-1234.json")
    assert "$where" in captured_params
    assert "date_occ" in captured_params["$where"]


@pytest.mark.asyncio
async def test_where_param_absent_when_date_field_none():
    rows = _make_rows(1)
    captured_params = {}

    class CapturingClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url, params=None, headers=None):
            captured_params.update(params or {})

            class R:
                status_code = 200

                def json(self_):
                    return rows

            return R()

    with patch("src.parsers.platform_socrata.httpx.AsyncClient", CapturingClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = SocrataParser(agency_id="test", date_field=None)
        await parser.fetch("https://data.example.com/resource/abcd-1234.json")
    assert "$where" not in captured_params


# ---------------------------------------------------------------------------
# source_metadata
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_source_metadata_has_required_keys():
    rows = _make_rows(1)
    MockClient, _ = _make_mock_client([(200, rows)])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch(
            "https://data.lacity.org/resource/2nrs-mtv8.json"
        )
    assert len(docs) == 1
    meta = docs[0].source_metadata
    for key in ("domain", "dataset_id", "offset", "report_number", "location"):
        assert key in meta, f"Missing source_metadata key: {key}"


# ---------------------------------------------------------------------------
# raw_text
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_raw_text_is_nonempty():
    rows = _make_rows(1)
    MockClient, _ = _make_mock_client([(200, rows)])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://data.example.com/resource/abcd-1234.json")
    assert docs[0].raw_text != ""


@pytest.mark.asyncio
async def test_raw_text_excludes_computed_columns():
    rows = [
        {
            "dr_no": "001",
            "date_occ": "2024-01-15T08:00:00",
            ":@computed_region_x": "some_value",
            ":id": "99",
        }
    ]
    MockClient, _ = _make_mock_client([(200, rows)])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://data.example.com/resource/abcd-1234.json")
    assert ":@computed_region_x" not in docs[0].raw_text
    assert ":id" not in docs[0].raw_text


# ---------------------------------------------------------------------------
# field_map override
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_field_map_override_picks_correct_column():
    rows = [{"custom_date_col": "2024-03-10T00:00:00", "custom_type_col": "ROBBERY"}]
    MockClient, _ = _make_mock_client([(200, rows)])
    with patch("src.parsers.platform_socrata.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = SocrataParser(
            agency_id="test",
            field_map={"date": "custom_date_col", "type": "custom_type_col"},
        )
        docs = await parser.fetch("https://data.example.com/resource/abcd-1234.json")
    assert len(docs) == 1
    assert docs[0].title == "ROBBERY"
    assert docs[0].published_date == datetime(2024, 3, 10, 0, 0, 0)


# ---------------------------------------------------------------------------
# _parse_url unit tests
# ---------------------------------------------------------------------------


def test_parse_url_standard():
    p = _make_parser()
    domain, dataset_id = p._parse_url("https://data.lacity.org/resource/2nrs-mtv8.json")
    assert domain == "data.lacity.org"
    assert dataset_id == "2nrs-mtv8"


def test_parse_url_with_query_string():
    p = _make_parser()
    domain, dataset_id = p._parse_url(
        "https://data.sfgov.org/resource/wg3w-h783.json?$limit=10"
    )
    assert domain == "data.sfgov.org"
    assert dataset_id == "wg3w-h783"


# ---------------------------------------------------------------------------
# _parse_date unit tests
# ---------------------------------------------------------------------------


def test_parse_date_iso_with_time():
    p = _make_parser()
    assert p._parse_date("2024-01-15T08:30:00") == datetime(2024, 1, 15, 8, 30, 0)


def test_parse_date_iso_date_only():
    p = _make_parser()
    assert p._parse_date("2024-01-15") == datetime(2024, 1, 15)


def test_parse_date_iso_with_millis():
    p = _make_parser()
    # fromisoformat on first 19 chars strips milliseconds
    assert p._parse_date("2024-01-15T08:30:00.000") == datetime(2024, 1, 15, 8, 30, 0)


def test_parse_date_iso_with_tz_suffix():
    p = _make_parser()
    # strip tz suffix via [:19]
    result = p._parse_date("2024-01-15T08:30:00+00:00")
    assert result == datetime(2024, 1, 15, 8, 30, 0)


def test_parse_date_none_returns_none():
    p = _make_parser()
    assert p._parse_date(None) is None


def test_parse_date_empty_string_returns_none():
    p = _make_parser()
    assert p._parse_date("") is None


def test_parse_date_invalid_returns_none():
    p = _make_parser()
    assert p._parse_date("not-a-date") is None


# ---------------------------------------------------------------------------
# Integration tests (real network — excluded from default pytest run)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_lapd_crime_data():
    """LAPD crime data — small 2-day window."""
    parser = SocrataParser("lapd", date_field="date_occ", days=2)
    docs = await parser.fetch("https://data.lacity.org/resource/2nrs-mtv8.json")
    assert len(docs) >= 1
    for doc in docs:
        assert doc.agency_id == "lapd"
        assert doc.raw_text != ""
    assert docs[0].source_metadata["dataset_id"] == "2nrs-mtv8"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_sfpd_incidents():
    """SFPD incident reports — small 2-day window."""
    parser = SocrataParser("sfpd", date_field="date", days=2)
    docs = await parser.fetch("https://data.sfgov.org/resource/wg3w-h783.json")
    assert len(docs) >= 1
    for doc in docs:
        assert doc.agency_id == "sfpd"
        assert doc.raw_text != ""
    assert docs[0].source_metadata["dataset_id"] == "wg3w-h783"
