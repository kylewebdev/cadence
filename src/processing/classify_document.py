import re
from dataclasses import dataclass

from src.parsers.base import RawDocument

# ---------------------------------------------------------------------------
# Output type
# ---------------------------------------------------------------------------

@dataclass
class ClassificationResult:
    document_type: str
    confidence: float


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maps legacy parser-emitted document_type strings to Phase 3 enum values.
LEGACY_NORMALIZATION: dict[str, str] = {
    "activity_feed": "daily_activity_log",
    "alert": "community_alert",
    "incident_log": "incident_report",
    "incident_reports": "incident_report",
    "open_data_api": "open_data_record",
    "pdf_library": "pdf_document",
}

# Valid Phase 3 document_type enum strings.
VALID_TYPES: frozenset[str] = frozenset(
    [
        "press_release",
        "arrest_log",
        "daily_activity_log",
        "community_alert",
        "incident_report",
        "crimemapping_incident",
        "open_data_record",
        "pdf_document",
        "rss_item",
    ]
)

# Platform priors: (document_type, confidence).
# Platforms with confidence >= 0.9 short-circuit — no further signals checked.
PLATFORM_TYPE_DEFAULTS: dict[str, tuple[str, float]] = {
    "crimemapping": ("crimemapping_incident", 1.0),
    "citizenrims": ("incident_report", 0.9),
    "nixle": ("community_alert", 0.9),
    "rave": ("community_alert", 0.9),
    "socrata": ("open_data_record", 0.85),
    "arcgis": ("open_data_record", 0.85),
    "pdf": ("pdf_document", 0.75),
    "civicplus": ("press_release", 0.7),
    "rss": ("rss_item", 0.7),
}

# URL path patterns → (document_type, confidence_boost).
_URL_PATTERNS: list[tuple[re.Pattern[str], str, float]] = [
    (re.compile(r"/press.?release|/news/|/media-release"), "press_release", 0.2),
    (re.compile(r"/arrest|/booking|/jail|/inmate"), "arrest_log", 0.25),
    (re.compile(r"/activity.?log|/daily.?log|/blotter|/patrol-log"), "daily_activity_log", 0.25),
    (re.compile(r"/alert|/warn|/emergency|/bolo"), "community_alert", 0.2),
    (re.compile(r"/incident|/crime.?report"), "incident_report", 0.2),
]

# Keyword signals → matched keywords list.
KEYWORD_SIGNALS: dict[str, list[str]] = {
    "press_release": [
        "press release",
        "for immediate release",
        "media contact",
        "public information officer",
        "pio",
    ],
    "arrest_log": [
        "arrested",
        "booking",
        "bail",
        "arraignment",
        "charges filed",
        "booked into",
        "remanded",
    ],
    "daily_activity_log": [
        "daily activity",
        "patrol log",
        "calls for service",
        "cad report",
        "shift summary",
        "service calls",
    ],
    "community_alert": [
        "bolo",
        "be on the lookout",
        "wanted",
        "missing person",
        "amber alert",
        "silver alert",
        "shelter in place",
        "advisory",
    ],
    "incident_report": [
        "case number",
        "report number",
        "occurred at",
        "victim reported",
        "suspect fled",
        "investigation ongoing",
    ],
}

_FALLBACK: ClassificationResult = ClassificationResult(document_type="press_release", confidence=0.4)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _url_signal(url: str) -> tuple[str, float] | None:
    """Return (document_type, boost) for the first matching URL pattern, or None."""
    lowered = url.lower()
    for pattern, doc_type, boost in _URL_PATTERNS:
        if pattern.search(lowered):
            return doc_type, boost
    return None


def _keyword_signal(text: str) -> tuple[str, float] | None:
    """
    Score keyword signals against combined title+text excerpt.
    Returns (document_type, boost) for the highest-scoring type, or None.
    Boost = min(match_count * 0.1, 0.3).
    """
    lowered = text.lower()
    best_type: str | None = None
    best_boost: float = 0.0

    for doc_type, keywords in KEYWORD_SIGNALS.items():
        matches = sum(1 for kw in keywords if kw in lowered)
        if matches:
            boost = min(matches * 0.1, 0.3)
            if boost > best_boost:
                best_boost = boost
                best_type = doc_type

    if best_type is None:
        return None
    return best_type, best_boost


def _content_text(doc: RawDocument) -> str:
    """Return title + first 300 chars of raw_text combined."""
    title_part = (doc.title or "")
    text_part = (doc.raw_text or "")[:300]
    return f"{title_part} {text_part}"


# ---------------------------------------------------------------------------
# Main classifier
# ---------------------------------------------------------------------------

def classify_document(
    doc: RawDocument,
    platform_type: str | None = None,
) -> ClassificationResult:
    """
    Classify a RawDocument into a Phase 3 document_type enum value.

    Priority order:
      1. Legacy normalization of doc.document_type
      2. Platform-type strong priors (confidence >= 0.9 → immediate return)
      3. URL path heuristics (refines platform prior when confidence < 0.9)
      4. Keyword signals from title + text excerpt
      5. Fallback: ("press_release", 0.4)
    """

    # ------------------------------------------------------------------
    # Step 1: Legacy normalization
    # ------------------------------------------------------------------
    normalized = LEGACY_NORMALIZATION.get(doc.document_type, doc.document_type)

    # If the normalized type is already a valid enum value, use it as a weak prior.
    # We still let platform/URL/keyword signals override or confirm it.
    legacy_hit = normalized in VALID_TYPES

    # ------------------------------------------------------------------
    # Step 2: Platform-type strong prior
    # ------------------------------------------------------------------
    if platform_type:
        platform_key = platform_type.lower()
        if platform_key in PLATFORM_TYPE_DEFAULTS:
            doc_type, confidence = PLATFORM_TYPE_DEFAULTS[platform_key]

            # High-confidence platforms return immediately.
            if confidence >= 0.9:
                return ClassificationResult(document_type=doc_type, confidence=confidence)

            # Low-confidence platforms: continue refining with URL + keyword signals.
            url_sig = _url_signal(doc.url)
            kw_sig = _keyword_signal(_content_text(doc))

            # Determine best refinement.
            if url_sig and kw_sig:
                url_type, url_boost = url_sig
                kw_type, kw_boost = kw_sig
                if url_type == kw_type:
                    # Both signals agree — strong override.
                    return ClassificationResult(
                        document_type=url_type,
                        confidence=min(confidence + url_boost + kw_boost, 1.0),
                    )
                # Signals disagree — pick higher boost signal.
                if kw_boost >= url_boost:
                    return ClassificationResult(
                        document_type=kw_type,
                        confidence=min(confidence + kw_boost, 1.0),
                    )
                return ClassificationResult(
                    document_type=url_type,
                    confidence=min(confidence + url_boost, 1.0),
                )
            elif url_sig:
                url_type, url_boost = url_sig
                return ClassificationResult(
                    document_type=url_type,
                    confidence=min(confidence + url_boost, 1.0),
                )
            elif kw_sig:
                kw_type, kw_boost = kw_sig
                return ClassificationResult(
                    document_type=kw_type,
                    confidence=min(confidence + kw_boost, 1.0),
                )
            else:
                # No refinement — return platform default.
                return ClassificationResult(document_type=doc_type, confidence=confidence)

    # ------------------------------------------------------------------
    # Step 3 + 4: No platform (or unrecognized platform). Use URL + keyword signals.
    # ------------------------------------------------------------------
    url_sig = _url_signal(doc.url)
    kw_sig = _keyword_signal(_content_text(doc))

    if url_sig and kw_sig:
        url_type, url_boost = url_sig
        kw_type, kw_boost = kw_sig
        base = 0.5
        if url_type == kw_type:
            return ClassificationResult(
                document_type=url_type,
                confidence=min(base + url_boost + kw_boost, 1.0),
            )
        if kw_boost >= url_boost:
            return ClassificationResult(
                document_type=kw_type,
                confidence=min(base + kw_boost, 1.0),
            )
        return ClassificationResult(
            document_type=url_type,
            confidence=min(base + url_boost, 1.0),
        )
    elif url_sig:
        url_type, url_boost = url_sig
        return ClassificationResult(
            document_type=url_type,
            confidence=min(0.5 + url_boost, 1.0),
        )
    elif kw_sig:
        kw_type, kw_boost = kw_sig
        return ClassificationResult(
            document_type=kw_type,
            confidence=min(0.5 + kw_boost, 1.0),
        )

    # ------------------------------------------------------------------
    # Legacy normalization fallback (step 1 result used as weak signal)
    # ------------------------------------------------------------------
    if legacy_hit:
        return ClassificationResult(document_type=normalized, confidence=0.55)

    # ------------------------------------------------------------------
    # Step 5: Fallback
    # ------------------------------------------------------------------
    return _FALLBACK
