from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.parsers.platform_pdf import PDFParser

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "pdf"


def load_pdf_fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def _make_mock_client(status_code: int, content: bytes):
    """Return a mock httpx.AsyncClient class yielding the given response."""

    class MockResponse:
        def __init__(self):
            self.status_code = status_code
            self.content = content

    class MockAsyncClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url, headers=None):
            return MockResponse()

    return MockAsyncClient


def _make_mock_pdf(page_texts: list[str]):
    """Return a mock pdfplumber PDF context manager with the given page texts."""
    pages = []
    for text in page_texts:
        page = MagicMock()
        page.extract_text.return_value = text
        pages.append(page)

    mock_pdf = MagicMock()
    mock_pdf.pages = pages
    mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
    mock_pdf.__exit__ = MagicMock(return_value=False)
    return mock_pdf


# ---------------------------------------------------------------------------
# Basic text-layer PDF tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clean_text_pdf_returns_one_document():
    """Real fixture PDF bytes parsed by pdfplumber — should extract 'Hello World'."""
    pdf_bytes = load_pdf_fixture("text_pdf.pdf")
    mock_client = _make_mock_client(200, pdf_bytes)

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/report.pdf")

    assert len(docs) == 1
    assert "Hello World" in docs[0].raw_text
    assert not docs[0].source_metadata.get("is_ocr")


@pytest.mark.asyncio
async def test_text_pdf_document_type_arrest_log():
    long_text = "A" * 60  # >= OCR_CHAR_THRESHOLD
    mock_client = _make_mock_client(200, b"fake")
    mock_pdf = _make_mock_pdf([long_text])

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("src.parsers.platform_pdf.pdfplumber.open", return_value=mock_pdf), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch(
            "https://example.com/arrest_records/2024.pdf"
        )

    assert len(docs) == 1
    assert docs[0].document_type == "arrest_log"


@pytest.mark.asyncio
async def test_text_pdf_document_type_press_release():
    long_text = "A" * 60
    mock_client = _make_mock_client(200, b"fake")
    mock_pdf = _make_mock_pdf([long_text])

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("src.parsers.platform_pdf.pdfplumber.open", return_value=mock_pdf), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch(
            "https://example.com/press_releases/jan2024.pdf"
        )

    assert len(docs) == 1
    assert docs[0].document_type == "press_release"


@pytest.mark.asyncio
async def test_text_pdf_rate_limit_called():
    pdf_bytes = load_pdf_fixture("text_pdf.pdf")
    mock_client = _make_mock_client(200, pdf_bytes)

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await PDFParser("test-pd").fetch("https://example.com/report.pdf")

    mock_sleep.assert_awaited_once_with(1.0)


# ---------------------------------------------------------------------------
# OCR tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ocr_fallback_triggered_when_page_text_sparse():
    """When pdfplumber returns < 50 chars and OCR is available, OCR text is used."""
    mock_pdf = _make_mock_pdf([""])  # empty → triggers OCR
    mock_client = _make_mock_client(200, b"fake-pdf-bytes")
    mock_image = MagicMock()
    mock_pytesseract = MagicMock()
    mock_pytesseract.Output.DICT = "dict"
    mock_pytesseract.image_to_data.return_value = {"conf": [85, 90, 78]}
    mock_pytesseract.image_to_string.return_value = "Officer responded to scene"
    mock_convert = MagicMock(return_value=[mock_image])

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("src.parsers.platform_pdf.pdfplumber.open", return_value=mock_pdf), \
         patch("src.parsers.platform_pdf.OCR_AVAILABLE", True), \
         patch("src.parsers.platform_pdf.pytesseract", mock_pytesseract), \
         patch("src.parsers.platform_pdf.convert_from_bytes", mock_convert), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/report.pdf")

    assert len(docs) == 1
    assert "Officer responded" in docs[0].raw_text
    assert docs[0].source_metadata.get("is_ocr") is True


@pytest.mark.asyncio
async def test_ocr_low_confidence_flagged():
    mock_pdf = _make_mock_pdf([""])
    mock_client = _make_mock_client(200, b"fake")
    mock_image = MagicMock()
    mock_pytesseract = MagicMock()
    mock_pytesseract.Output.DICT = "dict"
    mock_pytesseract.image_to_data.return_value = {"conf": [25, 35, 40]}
    mock_pytesseract.image_to_string.return_value = "low quality scan text"
    mock_convert = MagicMock(return_value=[mock_image])

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("src.parsers.platform_pdf.pdfplumber.open", return_value=mock_pdf), \
         patch("src.parsers.platform_pdf.OCR_AVAILABLE", True), \
         patch("src.parsers.platform_pdf.pytesseract", mock_pytesseract), \
         patch("src.parsers.platform_pdf.convert_from_bytes", mock_convert), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/report.pdf")

    assert len(docs) == 1
    assert docs[0].source_metadata.get("ocr_confidence") == "low"


@pytest.mark.asyncio
async def test_ocr_high_confidence_not_flagged_low():
    mock_pdf = _make_mock_pdf([""])
    mock_client = _make_mock_client(200, b"fake")
    mock_image = MagicMock()
    mock_pytesseract = MagicMock()
    mock_pytesseract.Output.DICT = "dict"
    mock_pytesseract.image_to_data.return_value = {"conf": [85, 90, 80]}
    mock_pytesseract.image_to_string.return_value = "high quality text here"
    mock_convert = MagicMock(return_value=[mock_image])

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("src.parsers.platform_pdf.pdfplumber.open", return_value=mock_pdf), \
         patch("src.parsers.platform_pdf.OCR_AVAILABLE", True), \
         patch("src.parsers.platform_pdf.pytesseract", mock_pytesseract), \
         patch("src.parsers.platform_pdf.convert_from_bytes", mock_convert), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/report.pdf")

    assert len(docs) == 1
    assert docs[0].source_metadata.get("ocr_confidence") == "ok"


@pytest.mark.asyncio
async def test_ocr_skipped_silently_when_unavailable():
    """Sparse page with OCR disabled: no exception, empty page filtered out."""
    mock_pdf = _make_mock_pdf([""])  # sparse, but OCR unavailable
    mock_client = _make_mock_client(200, b"fake")

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("src.parsers.platform_pdf.pdfplumber.open", return_value=mock_pdf), \
         patch("src.parsers.platform_pdf.OCR_AVAILABLE", False), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/report.pdf")

    assert isinstance(docs, list)


# ---------------------------------------------------------------------------
# Error / edge-case tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_corrupt_pdf_returns_empty_list():
    mock_client = _make_mock_client(200, b"not a pdf")

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/report.pdf")

    assert docs == []


@pytest.mark.asyncio
async def test_zero_byte_pdf_returns_empty_list():
    mock_client = _make_mock_client(200, b"")

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/report.pdf")

    assert docs == []


@pytest.mark.asyncio
async def test_http_404_returns_empty_list():
    mock_client = _make_mock_client(404, b"")

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/report.pdf")

    assert docs == []


@pytest.mark.asyncio
async def test_http_500_returns_empty_list():
    mock_client = _make_mock_client(500, b"")

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/report.pdf")

    assert docs == []


# ---------------------------------------------------------------------------
# Multi-page / date-boundary tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multipage_split_on_date_headers():
    """Two pages each starting with a date header → two documents."""
    long_suffix = " " + "content " * 10  # ensure >= OCR_CHAR_THRESHOLD
    mock_pdf = _make_mock_pdf([
        "January 15, 2024" + long_suffix,
        "January 16, 2024" + long_suffix,
    ])
    mock_client = _make_mock_client(200, b"fake")

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("src.parsers.platform_pdf.pdfplumber.open", return_value=mock_pdf), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/daily_log.pdf")

    assert len(docs) == 2
    assert docs[0].published_date == datetime(2024, 1, 15)
    assert docs[1].published_date == datetime(2024, 1, 16)
    assert docs[0].source_metadata["section_index"] == 0
    assert docs[1].source_metadata["section_index"] == 1


@pytest.mark.asyncio
async def test_multipage_no_split_when_no_date_headers():
    """Two pages without date headers → one combined document."""
    mock_pdf = _make_mock_pdf([
        "At 0830 hours officers responded to a disturbance at Main Street.",
        "Suspect was identified and detained without incident at 0915 hours.",
    ])
    mock_client = _make_mock_client(200, b"fake")

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("src.parsers.platform_pdf.pdfplumber.open", return_value=mock_pdf), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/log.pdf")

    assert len(docs) == 1
    assert "0830 hours" in docs[0].raw_text
    assert "0915 hours" in docs[0].raw_text
    assert docs[0].source_metadata["page_count"] == 2


@pytest.mark.asyncio
async def test_multipage_weekday_date_header_triggers_split():
    """Weekday + full-month format triggers split."""
    long_suffix = " " + "content " * 10
    mock_pdf = _make_mock_pdf([
        "MONDAY, JANUARY 15, 2024" + long_suffix,
        "TUESDAY, JANUARY 16, 2024" + long_suffix,
    ])
    mock_client = _make_mock_client(200, b"fake")

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("src.parsers.platform_pdf.pdfplumber.open", return_value=mock_pdf), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/log.pdf")

    assert len(docs) == 2
    assert docs[0].title is not None and "JANUARY 15" in docs[0].title
    assert docs[1].title is not None and "JANUARY 16" in docs[1].title


# ---------------------------------------------------------------------------
# Local file path test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_file_path_input(tmp_path):
    """Parser reads local file directly without HTTP client."""
    pdf_bytes = load_pdf_fixture("text_pdf.pdf")
    pdf_file = tmp_path / "report.pdf"
    pdf_file.write_bytes(pdf_bytes)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("riverside-pd").fetch(str(pdf_file))

    assert len(docs) == 1
    assert docs[0].agency_id == "riverside-pd"


# ---------------------------------------------------------------------------
# _clean_page_text unit tests
# ---------------------------------------------------------------------------


def test_clean_page_text_fixes_hyphenation():
    parser = PDFParser("test-pd")
    result = parser._clean_page_text("The of-\nficer responded.")
    assert "officer" in result


def test_clean_page_text_strips_standalone_page_numbers():
    parser = PDFParser("test-pd")
    result = parser._clean_page_text("content\n42\nmore content here")
    parts = result.split()
    assert "42" not in parts


def test_clean_page_text_strips_page_n_of_m_footer():
    parser = PDFParser("test-pd")
    result = parser._clean_page_text("Important content here.\nPage 3 of 10\nMore content.")
    assert "Page 3 of 10" not in result
    assert "Page" not in result or "Important" in result


def test_clean_page_text_preserves_numbers_in_sentences():
    parser = PDFParser("test-pd")
    result = parser._clean_page_text("badge 42 responded to the call")
    assert "42" in result


# ---------------------------------------------------------------------------
# _infer_document_type tests
# ---------------------------------------------------------------------------


def test_infer_document_type_arrest_log_keyword():
    parser = PDFParser("test-pd")
    assert parser._infer_document_type("https://example.com/arrest_log_2024.pdf") == "arrest_log"


def test_infer_document_type_blotter_keyword():
    parser = PDFParser("test-pd")
    assert parser._infer_document_type("https://example.com/blotter/jan.pdf") == "arrest_log"


def test_infer_document_type_press_release():
    parser = PDFParser("test-pd")
    assert parser._infer_document_type("https://example.com/press_releases/jan.pdf") == "press_release"


def test_infer_document_type_daily_activity_log():
    parser = PDFParser("test-pd")
    assert parser._infer_document_type("https://example.com/activity_report.pdf") == "daily_activity_log"


def test_infer_document_type_default():
    parser = PDFParser("test-pd")
    assert parser._infer_document_type("https://example.com/documents/notice.pdf") == "pdf_library"


# ---------------------------------------------------------------------------
# agency_id and metadata propagation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agency_id_propagated():
    long_suffix = " " + "content " * 10
    mock_pdf = _make_mock_pdf([
        "January 15, 2024" + long_suffix,
        "January 16, 2024" + long_suffix,
    ])
    mock_client = _make_mock_client(200, b"fake")

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("src.parsers.platform_pdf.pdfplumber.open", return_value=mock_pdf), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("riverside-pd").fetch("https://example.com/log.pdf")

    assert all(d.agency_id == "riverside-pd" for d in docs)


@pytest.mark.asyncio
async def test_source_metadata_has_page_count():
    mock_pdf = _make_mock_pdf(["Hello from page one content here enough chars."])
    mock_client = _make_mock_client(200, b"fake")

    with patch("src.parsers.platform_pdf.httpx.AsyncClient", mock_client), \
         patch("src.parsers.platform_pdf.pdfplumber.open", return_value=mock_pdf), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        docs = await PDFParser("test-pd").fetch("https://example.com/report.pdf")

    assert len(docs) == 1
    assert docs[0].source_metadata.get("page_count") >= 1
