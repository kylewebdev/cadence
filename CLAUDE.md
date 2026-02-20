# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Cadence is a California law enforcement intelligence aggregation platform. It scrapes, normalizes, and semantically searches public-facing data from ~697 CA law enforcement agencies to extract CAD/case numbers, power FOIA requests under CPRA, and detect trends across agencies.

## Architecture (6 Layers)

### 1. Source Registry (`registry/`)
Postgres table of all 697 agencies. Key fields: `agency_id`, `canonical_name`, `county`, `region`, `agency_type`, `homepage_url`, `feed_urls` (by content type), `platform_type`, `parser_id`, `scrape_frequency`, `foia_contact`. Source of truth is a CSV of 697 agencies.

### 2. Ingestion Pipeline (`ingestion/`)
Task queue (Temporal or n8n) dispatching per-agency scrape jobs. Parsers are modular, keyed by `platform_type`. Major platform families:
- **CivicPlus** (~100 agencies)
- **CrimeMapping** (~150+ agencies)
- **Nixle/Rave** (~60+ agencies)
- **Socrata/ArcGIS** open data portals
- PDF-only agencies
- Custom HTML one-offs

Scraping: Playwright (JS-heavy sites) + httpx (static). PDF: pdfplumber + Tesseract OCR.

### 3. Document Processing (`processing/`)
Normalize to common schema, strip boilerplate, extract CAD/case numbers via a regex library (30–50 patterns) with LLM fallback (Claude Haiku). Sets `foia_eligible` flag on documents.

### 4. Vector Store (`vector/`)
**Qdrant** as primary vector DB. Every chunk must carry full metadata: `agency_id`, `county`, `region`, `document_type`, `published_date`, `cad_numbers`, `foia_eligible`. Separate Postgres table holds full document records. Embedding model: `text-embedding-3-large`.

### 5. FOIA Pipeline (`foia/`)
Queue of `foia_eligible` documents. CPRA request template engine (Jinja2 + LLM fill-in). Status tracking and deadline reminders.

### 6. Frontend (`frontend/`)
Next.js + Tailwind + shadcn/ui. Features: semantic search with metadata filters, FOIA badge on results, trend dashboard (BERTopic clustering), agency registry management.

## Tech Stack

| Layer | Tech |
|---|---|
| API | Python, FastAPI |
| Scraping | Playwright, httpx |
| PDF | pdfplumber, Tesseract |
| Queue | Redis + BullMQ (or Temporal/n8n) |
| Vector DB | Qdrant |
| Relational DB | Postgres |
| Frontend | Next.js, Tailwind, shadcn/ui |
| Embeddings | OpenAI `text-embedding-3-large` |
| LLM fallback | Claude Haiku (`claude-haiku-4-5-20251001`) |

## MVP Build Sequence

1. Registry setup — load CSV into Postgres, validate schema
2. RSS/platform parsers — CivicPlus, CrimeMapping, Nixle first (highest agency coverage)
3. Embedding pipeline — normalize → embed → upsert to Qdrant with full metadata
4. FOIA queue — flag eligible docs, render CPRA templates
5. Frontend — search UI, filters, FOIA badge, trend dashboard

## Critical Invariant

Every document chunk upserted to Qdrant **must** carry the full metadata payload (`agency_id`, `published_date`, `document_type`, `cad_numbers`, `foia_eligible`). Filtered semantic search correctness depends entirely on this — a chunk without metadata cannot be filtered and is effectively invisible to CPRA workflows.

## CAD/Case Number Extraction

Regex library covers 30–50 patterns (agency-specific formats). When regex yields no match, fall back to Claude Haiku with a structured extraction prompt. The `foia_eligible` flag is set when a valid CAD or case number is present and the document is from a public-facing feed.
