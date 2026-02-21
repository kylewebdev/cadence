"""clean_document.py
====================
Platform-aware text cleaner for Phase 3 document processing.

Normalizes raw HTML/text scraped from CA law enforcement agencies into
clean text suitable for NLP and vector embedding. Returns a CleaningResult
with ``cleaned_text`` and ``quality_score`` (0–100), mapping directly to
``parse_quality SMALLINT`` in the documents schema.

Pipeline (in order):
  a. Strip HTML tags (BeautifulSoup)
  b. Remove platform-specific boilerplate
  c. Deduplicate near-identical sentences
  d. HTML entity decode
  e. Remove universal PDF pagination artifacts
  f. Remove non-printable characters
  g. Normalize whitespace
"""

import html
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Final

from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Output type
# ---------------------------------------------------------------------------

@dataclass
class CleaningResult:
    cleaned_text: str
    quality_score: int  # 0–100; maps to parse_quality SMALLINT


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Platform-specific boilerplate patterns.
# Line-anchored patterns (containing ^) also use re.MULTILINE.
BOILERPLATE_PATTERNS: dict[str, list[re.Pattern]] = {
    "civicplus": [
        # Share/Print/Email widgets
        re.compile(r"\bShare\b.*?\bPrint\b.*?\bEmail\b", re.IGNORECASE),
        re.compile(r"\b(Share|Print|Email)\s+(this\s+)?(page|article|post)\b", re.IGNORECASE),
        # Nav breadcrumbs
        re.compile(r"^\s*(Home|Residents|Government|Services|Departments)\s*[/|>]\s*", re.IGNORECASE | re.MULTILINE),
        re.compile(r"\bHome\s*[/|>]\s*(Residents|Government|Services|Departments)\b", re.IGNORECASE),
        # Sign up for news/alerts
        re.compile(r"sign\s+up\s+for\s+(news|alerts?|notifications?|updates?)", re.IGNORECASE),
        re.compile(r"subscribe\s+to\s+(news|alerts?|notifications?|email\s+updates?)", re.IGNORECASE),
        # Cookie banners
        re.compile(r"(this\s+site\s+uses?\s+cookies?|we\s+use\s+cookies?)[^.]*\.", re.IGNORECASE),
        re.compile(r"(accept\s+all\s+cookies?|cookie\s+policy|cookie\s+settings?)", re.IGNORECASE),
        # Powered by CivicPlus
        re.compile(r"powered\s+by\s+civicplus", re.IGNORECASE),
        re.compile(r"civicplus\s+(cms|platform|technology)", re.IGNORECASE),
    ],
    "crimemapping": [
        re.compile(r"powered\s+by\s+crime\s*mapping", re.IGNORECASE),
        re.compile(r"\bview\s+map\b", re.IGNORECASE),
        re.compile(r"\b(download|export)\s+(report|data|csv|pdf)\b", re.IGNORECASE),
        re.compile(r"\bfilter\s+(by|results?|incidents?)\b", re.IGNORECASE),
        # Bare label lines (single word or label: only)
        re.compile(r"^\s*(Category|Type|Status|Zone|Beat|District|Address):\s*$", re.IGNORECASE | re.MULTILINE),
        re.compile(r"^\s*(Show|Hide)\s+(all|filters?|map|legend)\s*$", re.IGNORECASE | re.MULTILINE),
    ],
    "nixle": [
        re.compile(r"sent\s+via\s+(nixle|rave)", re.IGNORECASE),
        re.compile(r"to\s+manage\s+your\s+notifications?", re.IGNORECASE),
        re.compile(r"\bunsubscribe\b[^.]*", re.IGNORECASE),
        re.compile(r"(standard\s+)?((sms|message|data)\s+rates?\s+(may\s+)?apply)", re.IGNORECASE),
        re.compile(r"reply\s+stop\s+to\s+(opt.?out|unsubscribe|cancel)", re.IGNORECASE),
        re.compile(r"reply\s+(stop|help|info)\b[^.]*", re.IGNORECASE),
        re.compile(r"you\s+(are\s+)?receiving\s+this\s+(message|alert|notification)", re.IGNORECASE),
    ],
    "pdf": [
        re.compile(r"^\s*CONFIDENTIAL\s*$", re.MULTILINE),
        re.compile(r"^\s*FOR\s+OFFICIAL\s+USE\s+ONLY\s*$", re.MULTILINE),
        re.compile(r"^\s*LAW\s+ENFORCEMENT\s+SENSITIVE\s*$", re.MULTILINE),
        re.compile(r"^\s*THIS\s+PAGE\s+(INTENTIONALLY\s+)?LEFT\s+BLANK\s*$", re.MULTILINE),
        # Bare department letterhead (e.g. "FRESNO POLICE DEPARTMENT" as standalone line)
        re.compile(r"^\s*[A-Z\s]{10,}\s+(?:POLICE|SHERIFF|DEPARTMENT|DEPT\.?)\s*$", re.MULTILINE),
    ],
    "default": [
        re.compile(r"(this\s+site\s+uses?\s+cookies?|we\s+use\s+cookies?)[^.]*\.", re.IGNORECASE),
        re.compile(r"(accept\s+all\s+cookies?|cookie\s+policy)", re.IGNORECASE),
        # Generic nav words on standalone lines
        re.compile(r"^\s*(Home|About|Contact|Search|Menu|Navigation|Accessibility)\s*$", re.IGNORECASE | re.MULTILINE),
        # Social media
        re.compile(r"follow\s+us\s+on\s+(facebook|twitter|instagram|x|youtube|linkedin)", re.IGNORECASE),
        re.compile(r"like\s+us\s+on\s+facebook", re.IGNORECASE),
        # Copyright lines
        re.compile(r"©\s*\d{4}[^.\n]*", re.IGNORECASE),
        re.compile(r"copyright\s+\d{4}[^.\n]*", re.IGNORECASE),
        # Back to top
        re.compile(r"^\s*back\s+to\s+top\s*$", re.IGNORECASE | re.MULTILINE),
    ],
}

# rave is an alias for nixle (same parser, same boilerplate)
BOILERPLATE_PATTERNS["rave"] = BOILERPLATE_PATTERNS["nixle"]

# PDF hyphenated line-break joiner (word-\nword → wordword)
_PDF_HYPHEN_RE = re.compile(r"(\w)-\n(\w)")

# Universal "Page N of M" removal (catches what platform parsers miss)
_PDF_PAGE_RE = re.compile(r"^\s*[Pp]age\s+\d+\s+of\s+\d+\s*$", re.MULTILINE)

_DEDUP_THRESHOLD: Final[float] = 0.85

# Lines matching any of these are never removed by boilerplate removal.
_PROTECTED_PATTERNS: list[re.Pattern] = [
    # CAD / DR# / Case# / Report No. / Badge No. / ORI
    re.compile(
        r"\b(CAD|DR|case|report\s+no\.?|badge|ORI)[#:\s]+[\w-]+",
        re.IGNORECASE,
    ),
    # Street address: digit + (optional directional) + street name + street type
    re.compile(
        r"\d+\s+(?:[NSEW]\.?\s+)?[A-Za-z][\w\s]+\s+"
        r"(?:St|Ave|Blvd|Dr|Rd|Ln|Way|Ct|Pl|Hwy|Fwy|Pkwy|Cir|Ter|Loop)\.?\b",
        re.IGNORECASE,
    ),
    # MM/DD/YYYY
    re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b"),
    # YYYY-MM-DD
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    # "Month DD, YYYY" (long and abbreviated month names)
    re.compile(
        r"\b(?:January|February|March|April|May|June|July|August|"
        r"September|October|November|December|"
        r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2},\s+\d{4}\b",
        re.IGNORECASE,
    ),
    # Weekday + month + date combos
    re.compile(
        r"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
        r"(?:January|February|March|April|May|June|July|August|"
        r"September|October|November|December|"
        r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2}",
        re.IGNORECASE,
    ),
    # Phone: XXX-XXX-XXXX
    re.compile(r"\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b"),
    # Phone: (XXX) XXX-XXXX
    re.compile(r"\(\d{3}\)\s*\d{3}[-.\s]\d{4}"),
]

# Quality score signal regexes (compiled once at module load)
_DATE_QUALITY_RE = re.compile(
    r"\b(?:\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2}|"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},\s+\d{4})\b",
    re.IGNORECASE,
)
_CAD_QUALITY_RE = re.compile(
    r"\b(CAD|DR|case|report\s+no\.?|incident)[#:\s]+[\w-]+",
    re.IGNORECASE,
)
_ADDRESS_QUALITY_RE = re.compile(
    r"\b\d+\s+(?:[NSEW]\.?\s+)?[A-Za-z][\w\s]+\s+"
    r"(?:St|Ave|Blvd|Dr|Rd|Ln|Way|Ct|Pl|Hwy|Fwy|Pkwy|Cir|Ter|Loop)\.?\b",
    re.IGNORECASE,
)

# HTML block tags: insert paragraph breaks around them
_BLOCK_TAGS = {"p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip_html(text: str) -> str:
    """Parse HTML with BeautifulSoup and extract plain text.

    Inserts paragraph breaks around block-level tags so content from
    adjacent divs/paragraphs doesn't run together.
    """
    soup = BeautifulSoup(text, "html.parser")

    # Replace <br> tags with newlines
    for tag in soup.find_all("br"):
        tag.replace_with("\n")

    # Insert blank lines around block tags so get_text() separates them
    for tag in soup.find_all(_BLOCK_TAGS):
        tag.insert_before("\n\n")
        tag.insert_after("\n\n")

    return soup.get_text()


def _is_protected_line(line: str) -> bool:
    """Return True if the line contains a protected identifier (date, address, CAD#, phone)."""
    for pattern in _PROTECTED_PATTERNS:
        if pattern.search(line):
            return True
    return False


def _remove_boilerplate(text: str, platform_type: str) -> str:
    """Remove platform-specific boilerplate from text.

    For PDF content, first joins hyphenated line-breaks to avoid
    splitting words that wrap across lines in the source.
    """
    pt = platform_type.lower() if platform_type else "default"

    if pt == "pdf":
        text = _PDF_HYPHEN_RE.sub(r"\1\2", text)

    patterns = BOILERPLATE_PATTERNS.get(pt) or BOILERPLATE_PATTERNS["default"]

    lines = text.split("\n")
    result: list[str] = []
    for line in lines:
        if _is_protected_line(line):
            result.append(line)
            continue

        cleaned_line = line
        blanked = False
        for pattern in patterns:
            # Line-anchored patterns: blank the entire line
            if pattern.pattern.startswith("^"):
                if pattern.search(cleaned_line):
                    cleaned_line = ""
                    blanked = True
                    break
            else:
                cleaned_line = pattern.sub(" ", cleaned_line)

        if not blanked:
            result.append(cleaned_line)
        else:
            result.append("")

    return "\n".join(result)


def _deduplicate_sentences(text: str) -> str:
    """Remove near-duplicate sentences (similarity >= _DEDUP_THRESHOLD).

    Splits on sentence boundaries and double newlines, keeping the first
    occurrence of each sentence.
    """
    segments = re.split(r"(?<=[.!?])\s+(?=[A-Z])|\n{2,}", text)
    seen: list[str] = []
    for segment in segments:
        stripped = segment.strip()
        if not stripped:
            continue
        is_dup = False
        for prior in seen:
            ratio = SequenceMatcher(None, stripped.lower(), prior.lower()).ratio()
            if ratio >= _DEDUP_THRESHOLD:
                is_dup = True
                break
        if not is_dup:
            seen.append(stripped)
    return "\n\n".join(seen)


def _remove_nonprintable(text: str) -> str:
    """Remove non-printable Unicode characters, preserving \\n and \\t."""
    result: list[str] = []
    for ch in text:
        if ch in ("\n", "\t"):
            result.append(ch)
        elif unicodedata.category(ch).startswith("C"):
            continue
        else:
            result.append(ch)
    return "".join(result)


def _normalize_whitespace(text: str) -> str:
    """Collapse runs of spaces/tabs within lines and excess blank lines."""
    lines = text.split("\n")
    normalized = [re.sub(r"[ \t]+", " ", line).strip() for line in lines]
    joined = "\n".join(normalized)
    # Collapse 3+ consecutive newlines → 2
    joined = re.sub(r"\n{3,}", "\n\n", joined)
    return joined.strip()


def _compute_quality_score(original: str, cleaned: str) -> int:
    """Compute a 0–100 quality score for the cleaned document.

    Base score penalizes excessive removal. Bonuses reward the presence of
    identifiers (dates, CAD/case numbers, addresses) that signal real content.
    """
    if not original:
        return 0

    removal_pct = (len(original) - len(cleaned)) / len(original)
    base = max(0, 100 - int(removal_pct * 80))

    bonus = 0
    if _DATE_QUALITY_RE.search(cleaned):
        bonus += 10
    if _CAD_QUALITY_RE.search(cleaned):
        bonus += 10
    if _ADDRESS_QUALITY_RE.search(cleaned):
        bonus += 5

    score = min(100, base + bonus)

    if len(cleaned.strip()) < 50:
        score = min(score, 30)

    return score


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def clean_document(raw_text: str, platform_type: str = "default") -> CleaningResult:
    """Clean raw HTML/text from a law enforcement document.

    Args:
        raw_text: Raw text or HTML scraped from the source.
        platform_type: Parser platform key (``civicplus``, ``nixle``, ``rave``,
            ``crimemapping``, ``pdf``, or ``default``).

    Returns:
        CleaningResult with ``cleaned_text`` (normalized plain text) and
        ``quality_score`` (0–100 integer mapping to ``parse_quality SMALLINT``).
    """
    if not raw_text or not raw_text.strip():
        return CleaningResult(cleaned_text="", quality_score=0)

    # a. Strip HTML
    text = _strip_html(raw_text)

    # b. Remove platform-specific boilerplate
    text = _remove_boilerplate(text, platform_type)

    # c. Deduplicate near-identical sentences
    text = _deduplicate_sentences(text)

    # d. HTML entity decode (after BS4 so entities remain ASCII for BS4)
    text = html.unescape(text)

    # e. Remove universal PDF pagination artifacts
    text = _PDF_PAGE_RE.sub("", text)

    # f. Remove non-printable characters (after entity decode: &shy; → U+00AD → dropped)
    text = _remove_nonprintable(text)

    # g. Normalize whitespace
    text = _normalize_whitespace(text)

    score = _compute_quality_score(raw_text, text)
    return CleaningResult(cleaned_text=text, quality_score=score)
