from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.dedup.deduplicator import Deduplicator, _InMemoryFallback, _URL_KEY_PREFIX
from src.parsers.base import RawDocument


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_doc(**kwargs) -> RawDocument:
    defaults = dict(
        url="https://example.com/press-release/1",
        agency_id="lapd",
        document_type="press_release",
        title="Test Title",
        raw_text="Officer responded to a call.",
        published_date=datetime(2024, 1, 15, 9, 0, 0),
    )
    defaults.update(kwargs)
    return RawDocument(**defaults)


def _make_dedup_with_fallback() -> Deduplicator:
    """Return a Deduplicator pre-configured to use the in-memory fallback."""
    d = Deduplicator()
    d._initialized = True
    d._fallback = _InMemoryFallback()
    return d


def _make_dedup_with_redis(mock_redis) -> Deduplicator:
    """Return a Deduplicator pre-configured with a mock Redis client."""
    d = Deduplicator()
    d._initialized = True
    d._redis = mock_redis
    return d


# ---------------------------------------------------------------------------
# Group 1: Hash deduplication (in-memory path)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_is_duplicate_returns_true_after_mark_seen():
    d = _make_dedup_with_fallback()
    doc = make_doc()
    assert await d.is_duplicate(doc) is False
    await d.mark_seen(doc)
    assert await d.is_duplicate(doc) is True


@pytest.mark.asyncio
async def test_different_content_same_url_not_duplicate():
    d = _make_dedup_with_fallback()
    doc1 = make_doc(raw_text="First content")
    doc2 = make_doc(raw_text="Second content")
    await d.mark_seen(doc1)
    assert await d.is_duplicate(doc2) is False


# ---------------------------------------------------------------------------
# Group 2: URL TTL (in-memory path)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_url_not_fetched_returns_false():
    d = _make_dedup_with_fallback()
    assert await d.url_recently_fetched("https://example.com/feed") is False


@pytest.mark.asyncio
async def test_url_recently_fetched_within_ttl_returns_true():
    d = _make_dedup_with_fallback()
    url = "https://example.com/feed"
    await d.mark_url_fetched(url, ttl_hours=24)
    assert await d.url_recently_fetched(url) is True


@pytest.mark.asyncio
async def test_url_recently_fetched_expired_returns_false():
    d = _make_dedup_with_fallback()
    url = "https://example.com/feed"
    await d.mark_url_fetched(url, ttl_hours=24)

    # Backdate expiry to simulate TTL expiry
    key = Deduplicator._url_key(url)
    d._fallback._url_expiry[key] = datetime.utcnow() - timedelta(hours=1)

    assert await d.url_recently_fetched(url) is False


# ---------------------------------------------------------------------------
# Group 3: Redis fallback on unavailability
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_redis_unavailable_falls_back_to_in_memory():
    d = Deduplicator()

    mock_client = AsyncMock()
    mock_client.ping.side_effect = ConnectionError("Redis down")

    with patch("redis.asyncio.Redis.from_url", return_value=mock_client):
        await d._ensure_connected()

    assert d._redis is None
    assert d._fallback is not None
    assert isinstance(d._fallback, _InMemoryFallback)


@pytest.mark.asyncio
async def test_fallback_persists_state_across_calls():
    d = Deduplicator()

    mock_client = AsyncMock()
    mock_client.ping.side_effect = ConnectionError("Redis down")

    with patch("redis.asyncio.Redis.from_url", return_value=mock_client):
        doc = make_doc()
        assert await d.is_duplicate(doc) is False
        await d.mark_seen(doc)
        # from_url was only called once (lazy init with _initialized guard)
        assert d._initialized is True
        assert await d.is_duplicate(doc) is True


# ---------------------------------------------------------------------------
# Group 4: Redis path (mock Redis client)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_is_duplicate_calls_sismember():
    mock_redis = AsyncMock()
    mock_redis.sismember = AsyncMock(return_value=1)
    d = _make_dedup_with_redis(mock_redis)

    doc = make_doc()
    result = await d.is_duplicate(doc)

    expected_hash = Deduplicator._compute_hash(doc)
    mock_redis.sismember.assert_awaited_once_with("cadence:seen_hashes", expected_hash)
    assert result is True


@pytest.mark.asyncio
async def test_mark_seen_calls_sadd():
    mock_redis = AsyncMock()
    d = _make_dedup_with_redis(mock_redis)

    doc = make_doc()
    await d.mark_seen(doc)

    expected_hash = Deduplicator._compute_hash(doc)
    mock_redis.sadd.assert_awaited_once_with("cadence:seen_hashes", expected_hash)


@pytest.mark.asyncio
async def test_url_recently_fetched_calls_exists():
    mock_redis = AsyncMock()
    mock_redis.exists = AsyncMock(return_value=1)
    d = _make_dedup_with_redis(mock_redis)

    url = "https://example.com/feed"
    result = await d.url_recently_fetched(url)

    expected_key = Deduplicator._url_key(url)
    mock_redis.exists.assert_awaited_once_with(expected_key)
    assert result is True


@pytest.mark.asyncio
async def test_mark_url_fetched_passes_correct_ex_to_redis():
    mock_redis = AsyncMock()
    d = _make_dedup_with_redis(mock_redis)

    url = "https://example.com/feed"
    ttl_hours = 48
    await d.mark_url_fetched(url, ttl_hours=ttl_hours)

    expected_key = Deduplicator._url_key(url)
    mock_redis.set.assert_awaited_once_with(expected_key, "1", ex=ttl_hours * 3600)


# ---------------------------------------------------------------------------
# Group 5: Static helpers
# ---------------------------------------------------------------------------

def test_compute_hash_is_deterministic():
    doc = make_doc()
    assert Deduplicator._compute_hash(doc) == Deduplicator._compute_hash(doc)


def test_compute_hash_differs_on_raw_text_change():
    doc1 = make_doc(raw_text="First")
    doc2 = make_doc(raw_text="Second")
    assert Deduplicator._compute_hash(doc1) != Deduplicator._compute_hash(doc2)


def test_compute_hash_differs_on_url_change():
    doc1 = make_doc(url="https://example.com/1")
    doc2 = make_doc(url="https://example.com/2")
    assert Deduplicator._compute_hash(doc1) != Deduplicator._compute_hash(doc2)


def test_compute_hash_is_64_char_hex():
    doc = make_doc()
    h = Deduplicator._compute_hash(doc)
    assert len(h) == 64
    int(h, 16)  # raises ValueError if not valid hex


def test_url_key_starts_with_prefix():
    key = Deduplicator._url_key("https://example.com/feed")
    assert key.startswith(_URL_KEY_PREFIX)


def test_url_key_is_deterministic():
    url = "https://example.com/feed"
    assert Deduplicator._url_key(url) == Deduplicator._url_key(url)


# ---------------------------------------------------------------------------
# Group 6: _InMemoryFallback in isolation
# ---------------------------------------------------------------------------

def test_fallback_hash_seen_false_initially():
    fb = _InMemoryFallback()
    assert fb.hash_seen("abc123") is False


def test_fallback_hash_seen_true_after_add():
    fb = _InMemoryFallback()
    fb.add_hash("abc123")
    assert fb.hash_seen("abc123") is True


def test_fallback_url_seen_false_when_not_marked():
    fb = _InMemoryFallback()
    assert fb.url_seen("cadence:url:somekey") is False


def test_fallback_url_seen_true_within_ttl():
    fb = _InMemoryFallback()
    fb.mark_url("cadence:url:somekey", ttl_hours=1)
    assert fb.url_seen("cadence:url:somekey") is True


def test_fallback_url_seen_false_when_expired():
    fb = _InMemoryFallback()
    fb.mark_url("cadence:url:somekey", ttl_hours=1)
    fb._url_expiry["cadence:url:somekey"] = datetime.utcnow() - timedelta(seconds=1)
    assert fb.url_seen("cadence:url:somekey") is False


def test_fallback_instances_are_independent():
    fb1 = _InMemoryFallback()
    fb2 = _InMemoryFallback()
    fb1.add_hash("shared_hash")
    assert fb2.hash_seen("shared_hash") is False
