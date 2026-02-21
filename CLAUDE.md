# CLAUDE.md

CA law enforcement intelligence aggregation platform. 697 agencies. Python + FastAPI + Postgres + Redis + Qdrant.

## Phase Status

| Phase | Description | Status |
|---|---|---|
| 1 | Source Registry | ✅ Complete — 697 agencies in Postgres, platform_type enriched, FastAPI CRUD live |
| 2 | Ingestion Pipeline | ✅ Complete — parsers for CivicPlus/CitizenRims/Nixle/ArcGIS/CrimeMapping/Socrata/RSS/PDF, Temporal scheduler, Redis dedup, 204/266 agencies reachable (76.7%) |
| 3 | Document Processing | 🔄 In Progress |
| 4 | FOIA Pipeline | ⬜ Not started |
| 5 | Frontend | ⬜ Not started |

## Phase 3 — Document Processing (Active)

### Build Order
1. `schema/phase3_tables.sql` — documents, chunks, foia_queue, review_queue tables
2. `classify_document.py` — rule-based doc type classifier
3. `clean_document.py` — platform-aware text cleaner
4. `extract_identifiers.py` — regex CAD/case number library (25+ patterns)
5. `extract_identifiers_llm.py` — Claude Haiku fallback for press releases
6. `process_document.py` — orchestrator wiring all steps together
7. `foia_queue.py` — FOIA queue population and priority scoring
8. `health_check.py` + `test_phase3_pipeline.py` — monitoring and integration tests

### Schema Conventions
- Every document: `doc_id` (UUID4), `agency_id` (FK), `document_type` enum, `published_date` (TIMESTAMPTZ, never null — fallback to `scraped_at`)
- Every chunk inherits full parent doc metadata — non-negotiable
- `foia_eligible = true` when `cad_numbers[]` OR `case_numbers[]` is non-empty (Postgres trigger)
- `parse_quality < 50` → insert into `review_queue`, do not skip

### CAD Extraction Rules
- Regex first (25+ patterns covering agency-specific formats)
- LLM fallback only if: regex empty AND `document_type = press_release` AND doc > 100 tokens
- LLM target: < 10% of total docs. Alert if > 15%
- LLM model: `claude-haiku-4-5-20251001`

### Phase 3 → Phase 4 Handoff Signal
Redis embedding queue receiving `chunk_id`s AND `foia_queue` has entries after first full ingest run.

## Critical Invariant

Every chunk upserted to Qdrant **must** carry: `agency_id`, `published_date`, `document_type`, `cad_numbers`, `foia_eligible`. A chunk without full metadata is invisible to CPRA workflows.

## Key Files

| File | Purpose |
|---|---|
| `src/registry/models.py` | Agency, AgencyFeed, ParseRun, Document SQLAlchemy models |
| `src/parsers/__init__.py` | PARSER_REGISTRY, get_parser(parser_id, agency_id) |
| `src/parsers/health_monitor.py` | record_parse_run(), get_unhealthy_agencies() |
| `src/scheduler/activities.py` | Temporal activities |
| `src/scheduler/parser_registry.py` | get_parser(agency: Agency) — takes Agency object |
| `src/dedup/deduplicator.py` | Redis dedup with in-memory fallback |
| `src/api/deps.py` | AsyncSessionLocal |

## Tech Stack

| Layer | Tech |
|---|---|
| API | Python, FastAPI |
| Scraping | Playwright, httpx |
| PDF | pdfplumber, Tesseract |
| Queue | Redis + Temporal |
| Vector DB | Qdrant |
| Relational DB | Postgres |
| Embeddings | OpenAI `text-embedding-3-large` |
| LLM fallback | Claude Haiku `claude-haiku-4-5-20251001` |
| Frontend | Next.js, Tailwind, shadcn/ui |
