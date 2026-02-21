"""
Unit tests for src/processing/classify_document.py.
No database required.
"""
from datetime import datetime

import pytest

from src.parsers.base import RawDocument
from src.processing.classify_document import ClassificationResult, classify_document


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_doc(
    *,
    url: str = "https://example.com/news/",
    agency_id: str = "test-agency",
    document_type: str = "press_release",
    title: str | None = "Test Document",
    raw_text: str = "",
    published_date: datetime | None = None,
) -> RawDocument:
    return RawDocument(
        url=url,
        agency_id=agency_id,
        document_type=document_type,
        title=title,
        raw_text=raw_text,
        published_date=published_date or datetime(2024, 1, 1),
        source_metadata={},
    )


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

def test_crimemapping_platform_returns_high_confidence():
    """platform_type='crimemapping' → crimemapping_incident, confidence=1.0"""
    doc = make_doc(url="https://crimemapping.com/data/123")
    result = classify_document(doc, platform_type="crimemapping")
    assert result.document_type == "crimemapping_incident"
    assert result.confidence == 1.0


def test_nixle_platform_returns_community_alert():
    """platform_type='nixle' → community_alert, confidence=0.9"""
    doc = make_doc(url="https://nixle.com/alert/456")
    result = classify_document(doc, platform_type="nixle")
    assert result.document_type == "community_alert"
    assert result.confidence == 0.9


def test_citizenrims_platform_returns_incident_report():
    """platform_type='citizenrims' → incident_report, confidence=0.9"""
    doc = make_doc(url="https://pd.citizenrims.com/incidents/")
    result = classify_document(doc, platform_type="citizenrims")
    assert result.document_type == "incident_report"
    assert result.confidence == 0.9


def test_civicplus_arrest_url_overrides_press_release():
    """platform_type='civicplus', URL contains /arrests/ → arrest_log, confidence >= 0.8"""
    doc = make_doc(url="https://cityof.com/pd/arrests/2024-01")
    result = classify_document(doc, platform_type="civicplus")
    assert result.document_type == "arrest_log"
    assert result.confidence >= 0.8


def test_pdf_daily_activity_log_keyword():
    """platform_type='pdf', text starts with 'DAILY ACTIVITY LOG' → daily_activity_log, confidence >= 0.8"""
    doc = make_doc(
        url="https://pd.example.com/docs/report.pdf",
        raw_text="DAILY ACTIVITY LOG - Patrol Division\nCalls for service: 47\nShift summary follows.",
    )
    result = classify_document(doc, platform_type="pdf")
    assert result.document_type == "daily_activity_log"
    assert result.confidence >= 0.8


def test_socrata_platform_returns_open_data_record():
    """platform_type='socrata' → open_data_record, confidence >= 0.8"""
    doc = make_doc(url="https://data.cityofla.gov/dataset/crimes")
    result = classify_document(doc, platform_type="socrata")
    assert result.document_type == "open_data_record"
    assert result.confidence >= 0.8


def test_rss_bolo_keyword_overrides_rss_item():
    """platform_type='rss', text contains BOLO keywords → community_alert"""
    doc = make_doc(
        url="https://pd.example.com/feed/rss",
        title="BOLO Notice",
        raw_text="Be on the lookout for a white male suspect wanted for armed robbery.",
    )
    result = classify_document(doc, platform_type="rss")
    assert result.document_type == "community_alert"


def test_no_platform_generic_text_falls_back():
    """No platform, no signals → press_release, confidence < 0.6"""
    doc = make_doc(
        url="https://pd.example.com/page/",
        title="Department Update",
        raw_text="The department would like to inform the community of upcoming events.",
    )
    result = classify_document(doc, platform_type=None)
    assert result.document_type == "press_release"
    assert result.confidence < 0.6


def test_legacy_activity_feed_normalization():
    """Legacy doc.document_type='activity_feed' with no platform → daily_activity_log"""
    doc = make_doc(
        url="https://pd.example.com/generic/",
        document_type="activity_feed",
        raw_text="",
    )
    result = classify_document(doc, platform_type=None)
    assert result.document_type == "daily_activity_log"


def test_legacy_alert_with_nixle_platform():
    """Legacy doc.document_type='alert' with nixle platform → community_alert, confidence=0.9"""
    doc = make_doc(
        url="https://nixle.com/alert/789",
        document_type="alert",
    )
    result = classify_document(doc, platform_type="nixle")
    assert result.document_type == "community_alert"
    assert result.confidence == 0.9
