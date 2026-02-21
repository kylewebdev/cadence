import time
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.parsers.platform_rss import RSSParser


# --- Helpers ---

def make_entry(
    title="Test Entry",
    link="https://example.com/entry/1",
    summary="Summary text.",
    published_parsed=None,
    updated_parsed=None,
    content=None,
) -> MagicMock:
    entry = MagicMock(spec=[])
    entry.get = lambda key, default=None: {
        "title": title,
        "link": link,
        "summary": summary,
    }.get(key, default)
    entry.published_parsed = published_parsed
    entry.updated_parsed = updated_parsed
    if content is not None:
        entry.content = content
    else:
        del entry.content  # AttributeError when accessed
    return entry


def make_feed(entries, feed_title="Agency News Feed") -> MagicMock:
    feed = MagicMock()
    feed.entries = entries
    feed.feed = MagicMock()
    feed.feed.get = lambda key, default=None: {"title": feed_title}.get(key, default)
    return feed


SAMPLE_DATE = time.struct_time((2024, 3, 15, 10, 30, 0, 4, 75, 0))
SAMPLE_DATETIME = datetime(2024, 3, 15, 10, 30, 0)


# --- Tests ---

@pytest.mark.asyncio
async def test_rss2_feed_basic():
    entry1 = make_entry(
        title="Entry One",
        link="https://example.com/1",
        summary="First item.",
        published_parsed=SAMPLE_DATE,
    )
    entry2 = make_entry(
        title="Entry Two",
        link="https://example.com/2",
        summary="Second item.",
        published_parsed=SAMPLE_DATE,
    )
    feed = make_feed([entry1, entry2])

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = RSSParser("lapd")
        docs = await parser.fetch("https://example.com/feed.rss")

    assert len(docs) == 2
    assert docs[0].url == "https://example.com/1"
    assert docs[0].title == "Entry One"
    assert docs[0].raw_text == "First item."
    assert docs[0].published_date == SAMPLE_DATETIME
    assert docs[1].url == "https://example.com/2"


@pytest.mark.asyncio
async def test_atom_feed_uses_content():
    entry = make_entry(summary="Short summary.", content=[{"value": "Full content text."}])
    feed = make_feed([entry])

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = RSSParser("sfpd")
        docs = await parser.fetch("https://example.com/atom.xml")

    assert docs[0].raw_text == "Full content text."


@pytest.mark.asyncio
async def test_document_type_press_release():
    feed = make_feed([make_entry()], feed_title="Agency Press Releases")

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = RSSParser("lapd")
        docs = await parser.fetch("https://example.com/feed")

    assert docs[0].document_type == "press_release"


@pytest.mark.asyncio
async def test_document_type_arrest_log():
    feed = make_feed([make_entry()], feed_title="Daily Arrest Log")

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = RSSParser("lapd")
        docs = await parser.fetch("https://example.com/feed")

    assert docs[0].document_type == "arrest_log"


@pytest.mark.asyncio
async def test_document_type_activity_feed_default():
    feed = make_feed([make_entry()], feed_title="Latest Updates")

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = RSSParser("lapd")
        docs = await parser.fetch("https://example.com/feed")

    assert docs[0].document_type == "activity_feed"


@pytest.mark.asyncio
async def test_missing_title_is_none():
    entry = make_entry(title=None)
    # Override get to return None for title
    entry.get = lambda key, default=None: {
        "link": "https://example.com/1",
        "summary": "Some text.",
    }.get(key, default)
    feed = make_feed([entry])

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = RSSParser("lapd")
        docs = await parser.fetch("https://example.com/feed")

    assert docs[0].title is None


@pytest.mark.asyncio
async def test_missing_date_is_none():
    entry = make_entry(published_parsed=None, updated_parsed=None)
    feed = make_feed([entry])

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = RSSParser("lapd")
        docs = await parser.fetch("https://example.com/feed")

    assert docs[0].published_date is None


@pytest.mark.asyncio
async def test_missing_link_falls_back_to_feed_url():
    entry = make_entry()
    entry.get = lambda key, default=None: {
        "title": "No Link Entry",
        "summary": "Some text.",
    }.get(key, default)
    feed = make_feed([entry])
    feed_url = "https://example.com/feed.rss"

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = RSSParser("lapd")
        docs = await parser.fetch(feed_url)

    assert docs[0].url == feed_url


@pytest.mark.asyncio
async def test_missing_content_empty_raw_text():
    entry = make_entry(summary=None)
    entry.get = lambda key, default=None: {
        "title": "Entry",
        "link": "https://example.com/1",
    }.get(key, default)
    feed = make_feed([entry])

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = RSSParser("lapd")
        docs = await parser.fetch("https://example.com/feed")

    assert docs[0].raw_text == ""


@pytest.mark.asyncio
async def test_malformed_feed_returns_empty_list():
    feed = MagicMock()
    feed.entries = []
    feed.bozo = True

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = RSSParser("lapd")
        docs = await parser.fetch("https://example.com/broken.rss")

    assert docs == []


@pytest.mark.asyncio
async def test_rate_limit_called():
    feed = make_feed([make_entry()])

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        parser = RSSParser("lapd")
        await parser.fetch("https://example.com/feed")

    mock_sleep.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_source_metadata_contains_feed_url():
    feed = make_feed([make_entry()], feed_title="Test Feed")
    feed_url = "https://example.com/feed.rss"

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = RSSParser("lapd")
        docs = await parser.fetch(feed_url)

    assert docs[0].source_metadata["feed_url"] == feed_url


@pytest.mark.asyncio
async def test_agency_id_propagated():
    entries = [make_entry(link=f"https://example.com/{i}") for i in range(3)]
    # Override get for each entry to include unique links
    for i, entry in enumerate(entries):
        link = f"https://example.com/{i}"
        entry.get = lambda key, default=None, _link=link: {
            "title": "Entry",
            "link": _link,
            "summary": "Text.",
        }.get(key, default)
    feed = make_feed(entries)

    with patch("feedparser.parse", return_value=feed), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        parser = RSSParser("test-agency-123")
        docs = await parser.fetch("https://example.com/feed")

    assert all(doc.agency_id == "test-agency-123" for doc in docs)
