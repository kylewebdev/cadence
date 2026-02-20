# Cadence

California law enforcement intelligence aggregation platform. Scrapes, normalizes,
and semantically searches public-facing data from ~697 CA law enforcement agencies
to extract CAD/case numbers, power FOIA requests under CPRA, and detect trends.

## Architecture

| Phase | Module | Description |
|---|---|---|
| 1 | `src/registry` | Agency registry -- CRUD and CSV import |
| 2 | `src/parsers` | Scraping parsers by platform type |
| 3 | `src/processing` | Document normalization and CAD extraction |
| 4 | `src/embedding` | Qdrant vector store integration |
| 5 | `src/foia` | CPRA request pipeline |
| 6 | `src/api` | FastAPI application |

## Quickstart

### 1. Start infrastructure

```bash
docker-compose up -d
```

### 2. Install dependencies

```bash
pip install -e ".[dev]"
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env with your OPENAI_API_KEY
```

### 4. Run migrations

```bash
alembic upgrade head
```

### 5. Start the API

```bash
uvicorn src.api.main:app --reload
```

Health check: `curl http://localhost:8000/health`

## Tech Stack

- **API**: FastAPI + uvicorn
- **DB**: Postgres 16 (SQLAlchemy 2.0 async + Alembic)
- **Cache/Queue**: Redis 7
- **Vector DB**: Qdrant
- **Scraping**: Playwright + httpx
- **PDF**: pdfplumber
- **Embeddings**: OpenAI text-embedding-3-large
- **LLM fallback**: Claude Haiku
