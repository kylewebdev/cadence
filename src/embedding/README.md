# Phase 4: Vector Store Integration

Embed normalized documents and upsert to Qdrant with full metadata.

## Requirements

Every chunk upserted to Qdrant must carry the full metadata payload:

- `agency_id`
- `county`
- `region`
- `document_type`
- `published_date`
- `cad_numbers`
- `foia_eligible`

A chunk without metadata cannot be filtered and is invisible to CPRA workflows.

## Stack

- Embedding model: OpenAI `text-embedding-3-large`
- Vector DB: Qdrant (gRPC port 6334, HTTP port 6333)
- Full document records stored in Postgres alongside chunk references
