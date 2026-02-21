"""Tests for src/processing/clean_document.py"""
import pytest

from src.processing.clean_document import CleaningResult, clean_document


# ---------------------------------------------------------------------------
# Test 1: CivicPlus HTML — nav/widget boilerplate removed, content preserved
# ---------------------------------------------------------------------------

def test_civicplus_boilerplate_removed():
    html = """
    <html><body>
      <nav>Home / Residents / Government</nav>
      <h1>Officer Saves Child from Drowning</h1>
      <div class="article-preview">
        <p>On July 4, 2024, Officer Jane Smith of the Fresno PD responded to a
        distress call at 1234 N Palm Ave, Fresno, CA.</p>
      </div>
      <div class="share-widget">Share Print Email this page</div>
      <p>Sign up for news alerts from the City of Fresno.</p>
      <footer>Powered by CivicPlus</footer>
    </body></html>
    """
    result = clean_document(html, platform_type="civicplus")

    assert isinstance(result, CleaningResult)
    # Navigation / widget text gone
    assert "Powered by CivicPlus" not in result.cleaned_text
    assert "Sign up for news" not in result.cleaned_text.lower()
    # Core article content preserved
    assert "Officer Jane Smith" in result.cleaned_text
    assert "July 4, 2024" in result.cleaned_text
    assert "1234 N Palm Ave" in result.cleaned_text


# ---------------------------------------------------------------------------
# Test 2: Nixle alert — footer boilerplate removed, BOLO content preserved
# ---------------------------------------------------------------------------

def test_nixle_footer_removed():
    text = """BOLO Alert — Wanted Suspect

    On 03/15/2024, officers responded to 456 Main St regarding an armed robbery.
    Suspect is described as a male, approx. 30 years old.

    Sent via Nixle. Standard message and data rates may apply.
    To manage your notifications, visit nixle.com.
    Reply STOP to opt out.
    """
    result = clean_document(text, platform_type="nixle")

    # Footer lines gone
    assert "Sent via Nixle" not in result.cleaned_text
    assert "data rates may apply" not in result.cleaned_text.lower()
    assert "Reply STOP" not in result.cleaned_text

    # BOLO content preserved
    assert "BOLO" in result.cleaned_text
    assert "03/15/2024" in result.cleaned_text
    assert "456 Main St" in result.cleaned_text


# ---------------------------------------------------------------------------
# Test 3: PDF — "Page N of M" markers removed, case number preserved
# ---------------------------------------------------------------------------

def test_pdf_page_markers_removed():
    text = """Arrest Report
Case No. 24-001234

On January 10, 2024, at approximately 1400 hours, officers arrested John Doe.
Page 1 of 12

Charges include Penal Code 459 - Burglary.
page 2 of 12

The suspect was booked into Fresno County Jail.
"""
    result = clean_document(text, platform_type="pdf")

    # Page markers gone
    assert "Page 1 of 12" not in result.cleaned_text
    assert "page 2 of 12" not in result.cleaned_text

    # Content and identifiers preserved
    assert "Case No. 24-001234" in result.cleaned_text
    assert "John Doe" in result.cleaned_text


# ---------------------------------------------------------------------------
# Test 4: Duplicate sentence deduplication
# ---------------------------------------------------------------------------

def test_duplicate_sentence_deduplicated():
    text = (
        "Officers responded to a report of vandalism at Elm Street. "
        "The suspect fled the scene before officers arrived. "
        "Officers responded to a report of vandalism at Elm Street. "
        "No arrests were made."
    )
    result = clean_document(text, platform_type="default")

    # The duplicated sentence appears exactly once
    count = result.cleaned_text.lower().count("officers responded to a report of vandalism")
    assert count == 1

    # Other content still present
    assert "No arrests were made" in result.cleaned_text


# ---------------------------------------------------------------------------
# Test 5: HTML entity decoding
# ---------------------------------------------------------------------------

def test_html_entities_decoded():
    html = "<p>Smith &amp; Jones responded to the call. A&nbsp;suspect was detained.</p>"
    result = clean_document(html, platform_type="default")

    # No raw entities in output
    assert "&amp;" not in result.cleaned_text
    assert "&nbsp;" not in result.cleaned_text

    # Decoded text present
    assert "Smith & Jones" in result.cleaned_text


# ---------------------------------------------------------------------------
# Test 6: Rich document — identifiers preserved, quality_score >= 70
# ---------------------------------------------------------------------------

def test_rich_document_quality_score():
    html = """
    <html><body>
      <nav>Home / News</nav>
      <h1>Arrest Made in Connection with Bank Robbery</h1>
      <div class="article-preview">
        <p>For Immediate Release — February 14, 2024</p>
        <p>CAD #2024-00456: On February 14, 2024, detectives arrested a suspect
        at 789 W Olive Ave, Fresno, CA 93728, in connection with the robbery of
        First National Bank. Case No. 24-CR-7891.</p>
        <p>Contact: Public Information Officer, Fresno PD, (559) 621-7000.</p>
      </div>
      <footer>Powered by CivicPlus &copy; 2024</footer>
    </body></html>
    """
    result = clean_document(html, platform_type="civicplus")

    # Core identifiers preserved
    assert "CAD #2024-00456" in result.cleaned_text
    assert "February 14, 2024" in result.cleaned_text
    assert "789 W Olive Ave" in result.cleaned_text

    # Boilerplate stripped
    assert "Powered by CivicPlus" not in result.cleaned_text

    # Quality score meets threshold
    assert result.quality_score >= 70
