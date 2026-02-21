import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.parsers.base import BaseParser, RawDocument
from src.registry.models import Agency


# --- Fixtures ---

def make_doc(**kwargs) -> RawDocument:
    defaults = dict(
        url="https://example.com/press-release/1",
        agency_id="lapd",
        document_type="press_releases",
        title="Test Title",
        raw_text="Officer responded to a call.",
        published_date=datetime(2024, 1, 15, 9, 0, 0),
    )
    defaults.update(kwargs)
    return RawDocument(**defaults)


class ConcreteParser(BaseParser):
    async def fetch(self, url: str) -> list[RawDocument]:
        return [make_doc(url=url)]


# --- Tests ---

def test_raw_document_creation():
    doc = make_doc()
    assert doc.url == "https://example.com/press-release/1"
    assert doc.agency_id == "lapd"
    assert doc.document_type == "press_releases"
    assert doc.title == "Test Title"
    assert doc.raw_text == "Officer responded to a call."
    assert doc.published_date == datetime(2024, 1, 15, 9, 0, 0)
    assert doc.source_metadata == {}


def test_raw_document_defaults():
    doc = RawDocument(
        url="https://example.com/1",
        agency_id="sfpd",
        document_type="rss_feed",
        title=None,
        raw_text="Some text.",
        published_date=None,
    )
    assert doc.title is None
    assert doc.published_date is None
    assert doc.source_metadata == {}


def test_source_metadata_not_shared():
    doc1 = RawDocument(
        url="https://a.com", agency_id="a", document_type="rss_feed",
        title=None, raw_text="text", published_date=None,
    )
    doc2 = RawDocument(
        url="https://b.com", agency_id="b", document_type="rss_feed",
        title=None, raw_text="text", published_date=None,
    )
    doc1.source_metadata["key"] = "value"
    assert "key" not in doc2.source_metadata


def test_base_parser_is_abstract():
    with pytest.raises(TypeError):
        BaseParser()  # type: ignore[abstract]


@pytest.mark.asyncio
async def test_concrete_subclass_fetch():
    parser = ConcreteParser()
    results = await parser.fetch("https://example.com/feed")
    assert len(results) == 1
    assert results[0].url == "https://example.com/feed"


def test_hash_document_deterministic():
    parser = ConcreteParser()
    doc = make_doc()
    assert parser.hash_document(doc) == parser.hash_document(doc)


def test_hash_document_differs_on_change():
    parser = ConcreteParser()
    doc1 = make_doc(raw_text="Original text.")
    doc2 = make_doc(raw_text="Different text.")
    assert parser.hash_document(doc1) != parser.hash_document(doc2)


def test_hash_document_is_hex_string():
    parser = ConcreteParser()
    doc = make_doc()
    h = parser.hash_document(doc)
    assert len(h) == 64
    int(h, 16)  # raises ValueError if not valid hex


def test_clean_whitespace_collapses_spaces():
    parser = ConcreteParser()
    assert parser.clean_whitespace("  hello   world  ") == "hello world"


def test_clean_whitespace_collapses_tabs_and_newlines():
    parser = ConcreteParser()
    assert parser.clean_whitespace("line1\n\tline2\r\nline3") == "line1 line2 line3"


def test_clean_whitespace_empty_string():
    parser = ConcreteParser()
    assert parser.clean_whitespace("   ") == ""


@pytest.mark.asyncio
async def test_rate_limit_delay_default():
    parser = ConcreteParser()
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await parser.rate_limit_delay()
        mock_sleep.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_rate_limit_delay_custom():
    parser = ConcreteParser()
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await parser.rate_limit_delay(seconds=2.5)
        mock_sleep.assert_awaited_once_with(2.5)


@pytest.mark.asyncio
async def test_get_agency_returns_agency():
    agency = Agency(
        agency_id="lapd",
        canonical_name="Los Angeles Police Department",
    )

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = agency

    mock_db = AsyncMock()
    mock_db.execute.return_value = mock_result

    parser = ConcreteParser()
    result = await parser.get_agency("lapd", mock_db)

    assert result is agency
    mock_db.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_agency_returns_none_when_missing():
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None

    mock_db = AsyncMock()
    mock_db.execute.return_value = mock_result

    parser = ConcreteParser()
    result = await parser.get_agency("nonexistent", mock_db)

    assert result is None
