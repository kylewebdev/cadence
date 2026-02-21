from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from src.parsers.platform_arcgis import PAGE_SIZE, ArcGISParser

# ---------------------------------------------------------------------------
# Sample fixture data
# ---------------------------------------------------------------------------

SAMPLE_FEATURE = {
    "attributes": {
        "OBJECTID": 1,
        "INCIDENT_TYPE": "BURGLARY",
        "ADDRESS": "100 MAIN ST",
        "INCIDENT_DATE": 1705309200000,  # 2024-01-15T07:00:00 UTC
        "CASE_NUMBER": "24-001234",
    },
    "geometry": {"x": -118.2437, "y": 34.0522},
}

SAMPLE_FEATURE_NO_GEOMETRY = {
    "attributes": {
        "OBJECTID": 2,
        "INCIDENT_TYPE": "THEFT",
        "ADDRESS": "200 ELM ST",
        "INCIDENT_DATE": 1705395600000,
        "CASE_NUMBER": "24-001235",
    },
    "geometry": None,
}


# ---------------------------------------------------------------------------
# Mock httpx infrastructure
# ---------------------------------------------------------------------------


def _make_mock_client(responses: list):
    """
    responses: list of (status_code, payload_dict) pairs.
    """
    call_count = 0

    class MockResponse:
        def __init__(self, status_code: int, body: dict):
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
                status, body = responses[call_count]
            else:
                status, body = 404, {"features": []}
            call_count += 1
            return MockResponse(status, body)

    return MockAsyncClient, lambda: call_count


def _make_payload(features: list, exceeded: bool = False) -> dict:
    payload = {"features": features}
    if exceeded:
        payload["exceededTransferLimit"] = True
    return payload


def _make_features(n: int) -> list:
    return [
        {
            "attributes": {
                "OBJECTID": i,
                "INCIDENT_TYPE": "BURGLARY",
                "ADDRESS": f"{100 + i} MAIN ST",
                "INCIDENT_DATE": 1705309200000,
                "CASE_NUMBER": f"24-{i:06d}",
            },
            "geometry": {"x": -118.2437, "y": 34.0522},
        }
        for i in range(n)
    ]


def _make_parser(**kwargs) -> ArcGISParser:
    return ArcGISParser(agency_id="test-pd", **kwargs)


# ---------------------------------------------------------------------------
# Pagination — exceededTransferLimit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_exceeded_transfer_limit_stops_after_one_page():
    payload = _make_payload(_make_features(5), exceeded=False)
    MockClient, get_calls = _make_mock_client([(200, payload)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://example.com/arcgis/rest/services/Crimes/FeatureServer/0/query")
    assert get_calls() == 1
    assert len(docs) == 5


@pytest.mark.asyncio
async def test_exceeded_transfer_limit_true_triggers_second_request():
    page1 = _make_payload(_make_features(PAGE_SIZE), exceeded=True)
    page2 = _make_payload(_make_features(3), exceeded=False)
    MockClient, get_calls = _make_mock_client([(200, page1), (200, page2)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://example.com/arcgis/rest/services/Crimes/FeatureServer/0/query")
    assert get_calls() == 2
    assert len(docs) == PAGE_SIZE + 3


@pytest.mark.asyncio
async def test_exceeded_transfer_limit_absent_treated_as_false():
    # No "exceededTransferLimit" key at all → stop
    payload = {"features": _make_features(5)}
    MockClient, get_calls = _make_mock_client([(200, payload)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://example.com/arcgis/rest/services/Crimes/FeatureServer/0/query")
    assert get_calls() == 1
    assert len(docs) == 5


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_features_returns_empty_list():
    MockClient, _ = _make_mock_client([(200, {"features": []})])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://example.com/arcgis/query")
    assert docs == []


@pytest.mark.asyncio
async def test_error_key_in_payload_returns_empty_list():
    error_payload = {
        "error": {"code": 400, "message": "Invalid or missing input parameters."}
    }
    MockClient, _ = _make_mock_client([(200, error_payload)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://example.com/arcgis/query")
    assert docs == []


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rate_limit_single_page_sleeps_once():
    payload = _make_payload(_make_features(5))
    MockClient, _ = _make_mock_client([(200, payload)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await _make_parser().fetch("https://example.com/arcgis/query")
    mock_sleep.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_rate_limit_two_pages_sleeps_twice():
    page1 = _make_payload(_make_features(PAGE_SIZE), exceeded=True)
    page2 = _make_payload(_make_features(3))
    MockClient, _ = _make_mock_client([(200, page1), (200, page2)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await _make_parser().fetch("https://example.com/arcgis/query")
    assert mock_sleep.await_count == 2


# ---------------------------------------------------------------------------
# agency_id propagation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agency_id_propagated_to_all_docs():
    payload = _make_payload([SAMPLE_FEATURE])
    MockClient, _ = _make_mock_client([(200, payload)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = ArcGISParser(agency_id="riverside-pd")
        docs = await parser.fetch("https://example.com/arcgis/query")
    assert all(d.agency_id == "riverside-pd" for d in docs)


# ---------------------------------------------------------------------------
# Date parsing — epoch milliseconds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_epoch_ms_date_parsed_correctly():
    payload = _make_payload([SAMPLE_FEATURE])
    MockClient, _ = _make_mock_client([(200, payload)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = ArcGISParser(agency_id="test", date_field="INCIDENT_DATE")
        docs = await parser.fetch("https://example.com/arcgis/query")
    assert docs[0].published_date is not None
    assert docs[0].published_date == datetime.fromtimestamp(1705309200000 / 1000, tz=UTC).replace(tzinfo=None)


@pytest.mark.asyncio
async def test_negative_one_epoch_sentinel_returns_none_date():
    feature = {
        "attributes": {
            "OBJECTID": 1,
            "INCIDENT_DATE": -1,
            "INCIDENT_TYPE": "THEFT",
        },
        "geometry": None,
    }
    payload = _make_payload([feature])
    MockClient, _ = _make_mock_client([(200, payload)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = ArcGISParser(agency_id="test", date_field="INCIDENT_DATE")
        docs = await parser.fetch("https://example.com/arcgis/query")
    assert docs[0].published_date is None


# ---------------------------------------------------------------------------
# where param
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_date_field_none_produces_1_equals_1_where():
    captured = {}

    class CapturingClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url, params=None, headers=None):
            captured.update(params or {})

            class R:
                status_code = 200

                def json(self_):
                    return {"features": []}

            return R()

    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", CapturingClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        await ArcGISParser(agency_id="test", date_field=None).fetch(
            "https://example.com/arcgis/query"
        )
    assert captured.get("where") == "1=1"


@pytest.mark.asyncio
async def test_date_field_set_produces_date_where_clause():
    captured = {}

    class CapturingClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url, params=None, headers=None):
            captured.update(params or {})

            class R:
                status_code = 200

                def json(self_):
                    return {"features": []}

            return R()

    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", CapturingClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        await ArcGISParser(agency_id="test", date_field="INCIDENT_DATE").fetch(
            "https://example.com/arcgis/query"
        )
    assert "INCIDENT_DATE" in captured.get("where", "")
    assert "DATE" in captured.get("where", "")


# ---------------------------------------------------------------------------
# source_metadata
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_source_metadata_has_required_keys():
    payload = _make_payload([SAMPLE_FEATURE])
    MockClient, _ = _make_mock_client([(200, payload)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://example.com/arcgis/query")
    meta = docs[0].source_metadata
    for key in ("query_url", "offset", "report_number", "location", "geometry"):
        assert key in meta, f"Missing key: {key}"


@pytest.mark.asyncio
async def test_geometry_stored_in_metadata():
    payload = _make_payload([SAMPLE_FEATURE])
    MockClient, _ = _make_mock_client([(200, payload)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://example.com/arcgis/query")
    assert docs[0].source_metadata["geometry"] == {"x": -118.2437, "y": 34.0522}


@pytest.mark.asyncio
async def test_missing_geometry_is_none_in_metadata():
    payload = _make_payload([SAMPLE_FEATURE_NO_GEOMETRY])
    MockClient, _ = _make_mock_client([(200, payload)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://example.com/arcgis/query")
    assert docs[0].source_metadata["geometry"] is None


# ---------------------------------------------------------------------------
# Case-insensitive field resolution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_case_insensitive_field_resolution():
    """INCIDENT_TYPE (uppercase) should resolve via lowercase candidate 'incident_type'."""
    feature = {
        "attributes": {
            "OBJECTID": 1,
            "INCIDENT_TYPE": "ROBBERY",
            "ADDRESS": "300 OAK AVE",
            "INCIDENT_DATE": 1705309200000,
        },
        "geometry": None,
    }
    payload = _make_payload([feature])
    MockClient, _ = _make_mock_client([(200, payload)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://example.com/arcgis/query")
    assert docs[0].title == "ROBBERY"


# ---------------------------------------------------------------------------
# raw_text — -1 sentinel excluded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_raw_text_excludes_negative_one_sentinels():
    feature = {
        "attributes": {
            "OBJECTID": 1,
            "INCIDENT_TYPE": "THEFT",
            "NULL_DATE_FIELD": -1,
            "NULL_STR_FIELD": None,
        },
        "geometry": None,
    }
    payload = _make_payload([feature])
    MockClient, _ = _make_mock_client([(200, payload)])
    with patch("src.parsers.platform_arcgis.httpx.AsyncClient", MockClient), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await _make_parser().fetch("https://example.com/arcgis/query")
    assert "-1" not in docs[0].raw_text
    assert "NULL_DATE_FIELD" not in docs[0].raw_text


# ---------------------------------------------------------------------------
# _parse_date unit tests
# ---------------------------------------------------------------------------


def test_parse_date_epoch_ms_int():
    p = _make_parser()
    result = p._parse_date(1705309200000)
    assert result == datetime.fromtimestamp(1705309200000 / 1000, tz=UTC).replace(tzinfo=None)


def test_parse_date_epoch_ms_string():
    p = _make_parser()
    result = p._parse_date("1705309200000")
    assert result == datetime.fromtimestamp(1705309200000 / 1000, tz=UTC).replace(tzinfo=None)


def test_parse_date_negative_epoch_returns_none():
    p = _make_parser()
    assert p._parse_date(-1) is None


def test_parse_date_negative_epoch_string_returns_none():
    p = _make_parser()
    assert p._parse_date("-1") is None


def test_parse_date_iso_string():
    p = _make_parser()
    result = p._parse_date("2024-01-15T08:00:00")
    assert result == datetime(2024, 1, 15, 8, 0, 0)


def test_parse_date_none_returns_none():
    p = _make_parser()
    assert p._parse_date(None) is None


def test_parse_date_invalid_string_returns_none():
    p = _make_parser()
    assert p._parse_date("not-a-date") is None


# ---------------------------------------------------------------------------
# Integration test (real network)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_arcgis_public_service():
    """
    TODO: Identify a stable public CA ArcGIS FeatureServer.
    Skipped until a reliable URL is confirmed during implementation review.
    """
    pytest.skip("No stable public CA ArcGIS URL confirmed yet — add one when identified.")
