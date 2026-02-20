# Phase 2: Scraping Parsers

Modular parsers keyed by `platform_type` from the agency registry.

## Platform families

- **CivicPlus** (~100 agencies)
- **CrimeMapping** (~150+ agencies)
- **Nixle/Rave** (~60+ agencies)
- **Socrata/ArcGIS** open data portals
- **PDF-only** agencies (pdfplumber + Tesseract OCR)
- **Custom HTML** one-offs

## Scraping stack

- Playwright for JS-heavy sites
- httpx for static pages
- feedparser for RSS feeds
