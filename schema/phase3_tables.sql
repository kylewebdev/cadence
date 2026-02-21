-- Phase 3 Document Processing Schema
-- Migrates existing documents table and creates chunks, foia_queue, review_queue.
-- Apply: psql postgresql://cadence:cadence@localhost:5433/cadence_platform -f schema/phase3_tables.sql
-- Idempotent: safe to re-run.

-- ─────────────────────────────────────────────
-- Enums
-- ─────────────────────────────────────────────

DO $$ BEGIN
    CREATE TYPE document_type_enum AS ENUM (
        'press_release',
        'arrest_log',
        'daily_activity_log',
        'incident_report',
        'community_alert',
        'rss_item',
        'open_data_record',
        'pdf_document',
        'crimemapping_incident',
        'transparency_portal_entry'
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE extraction_method_enum AS ENUM (
        'none',
        'regex',
        'llm_haiku'
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE foia_status_enum AS ENUM (
        'pending',
        'submitted',
        'acknowledged',
        'completed',
        'dismissed'
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE review_reason_enum AS ENUM (
        'low_parse_quality',
        'failed_extraction',
        'manual_flag'
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE review_status_enum AS ENUM (
        'pending',
        'reviewed',
        'dismissed'
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

-- ─────────────────────────────────────────────
-- documents: migrate existing Phase 2 table to Phase 3 spec
-- ─────────────────────────────────────────────

-- 1. Rename id → doc_id
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'documents' AND column_name = 'id'
    ) THEN
        ALTER TABLE documents RENAME COLUMN id TO doc_id;
    END IF;
END $$;

-- 2. Rename url → source_url
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'documents' AND column_name = 'url'
    ) THEN
        ALTER TABLE documents RENAME COLUMN url TO source_url;
    END IF;
END $$;

-- 3. Rename ingested_at → scraped_at
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'documents' AND column_name = 'ingested_at'
    ) THEN
        ALTER TABLE documents RENAME COLUMN ingested_at TO scraped_at;
    END IF;
END $$;

-- 4. Remap Phase 2 document_type strings to enum values, then cast.
--    Phase 2 produced: activity_feed, alert, incident_log
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'documents' AND column_name = 'document_type'
          AND data_type = 'character varying'
    ) THEN
        UPDATE documents SET document_type = 'daily_activity_log' WHERE document_type = 'activity_feed';
        UPDATE documents SET document_type = 'community_alert'    WHERE document_type = 'alert';
        UPDATE documents SET document_type = 'incident_report'    WHERE document_type = 'incident_log';
        ALTER TABLE documents
            ALTER COLUMN document_type TYPE document_type_enum
            USING document_type::document_type_enum;
    END IF;
END $$;

-- 5. Convert cad_numbers from JSONB → TEXT[].
--    ALTER COLUMN TYPE ... USING doesn't allow subqueries, so use add/update/drop/rename.
--    Must also drop the trigger that depends on the column before dropping it.
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'documents' AND column_name = 'cad_numbers'
          AND data_type = 'jsonb'
    ) THEN
        -- Clean up any leftover temp column from a previous failed run
        ALTER TABLE documents DROP COLUMN IF EXISTS cad_numbers_new;
        -- Trigger depends on cad_numbers; drop before column removal
        DROP TRIGGER IF EXISTS trg_documents_foia_eligible ON documents;
        -- New TEXT[] column alongside the old JSONB one
        ALTER TABLE documents ADD COLUMN cad_numbers_new TEXT[] NOT NULL DEFAULT '{}';
        -- Subqueries are valid in UPDATE
        UPDATE documents
            SET cad_numbers_new = ARRAY(SELECT jsonb_array_elements_text(cad_numbers))
            WHERE cad_numbers IS NOT NULL;
        -- Dropping the JSONB column also drops gin_documents_cad_numbers automatically
        ALTER TABLE documents DROP COLUMN cad_numbers;
        ALTER TABLE documents RENAME COLUMN cad_numbers_new TO cad_numbers;
    END IF;
END $$;

-- 6. Fix foia_eligible: make NOT NULL DEFAULT false
UPDATE documents SET foia_eligible = false WHERE foia_eligible IS NULL;
ALTER TABLE documents ALTER COLUMN foia_eligible SET NOT NULL;
ALTER TABLE documents ALTER COLUMN foia_eligible SET DEFAULT false;

-- 7. Fix published_date: make NOT NULL, fallback to scraped_at
UPDATE documents SET published_date = scraped_at WHERE published_date IS NULL;
ALTER TABLE documents ALTER COLUMN published_date SET NOT NULL;

-- 8. Ensure scraped_at has a server default
ALTER TABLE documents ALTER COLUMN scraped_at SET DEFAULT now();

-- 9. Drop source_metadata (not in Phase 3 spec)
ALTER TABLE documents DROP COLUMN IF EXISTS source_metadata;

-- 10. Add Phase 3 columns (all idempotent via IF NOT EXISTS)
ALTER TABLE documents ADD COLUMN IF NOT EXISTS cleaned_text       TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS location_raw       TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS location_geo       JSONB;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS case_numbers       TEXT[]               NOT NULL DEFAULT '{}';
ALTER TABLE documents ADD COLUMN IF NOT EXISTS incident_types     TEXT[]               NOT NULL DEFAULT '{}';
ALTER TABLE documents ADD COLUMN IF NOT EXISTS people_mentioned   TEXT[]               NOT NULL DEFAULT '{}';
ALTER TABLE documents ADD COLUMN IF NOT EXISTS chunk_ids          UUID[]               NOT NULL DEFAULT '{}';
ALTER TABLE documents ADD COLUMN IF NOT EXISTS embedding_model    TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS parse_quality      SMALLINT             CHECK (parse_quality BETWEEN 0 AND 100);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS extraction_method  extraction_method_enum NOT NULL DEFAULT 'none';

-- 11. Indexes (ix_documents_agency_id and ix_documents_published_date already exist)
CREATE INDEX IF NOT EXISTS ix_documents_foia_eligible
    ON documents (doc_id)
    WHERE foia_eligible = true;

CREATE INDEX IF NOT EXISTS ix_documents_document_type
    ON documents (document_type);

CREATE INDEX IF NOT EXISTS ix_documents_agency_published
    ON documents (agency_id, published_date DESC);

CREATE INDEX IF NOT EXISTS gin_documents_cad_numbers
    ON documents USING GIN (cad_numbers);

CREATE INDEX IF NOT EXISTS gin_documents_case_numbers
    ON documents USING GIN (case_numbers);

-- ─────────────────────────────────────────────
-- Trigger: auto-set foia_eligible
-- ─────────────────────────────────────────────

CREATE OR REPLACE FUNCTION set_foia_eligible()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.foia_eligible := (
        array_length(NEW.cad_numbers, 1) > 0
        OR array_length(NEW.case_numbers, 1) > 0
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_documents_foia_eligible ON documents;

CREATE TRIGGER trg_documents_foia_eligible
    BEFORE INSERT OR UPDATE OF cad_numbers, case_numbers
    ON documents
    FOR EACH ROW EXECUTE FUNCTION set_foia_eligible();

-- ─────────────────────────────────────────────
-- chunks
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    doc_id           UUID        NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
    -- Denormalized parent metadata (critical invariant: every chunk carries full set)
    agency_id        TEXT        NOT NULL,
    published_date   TIMESTAMPTZ NOT NULL,
    document_type    document_type_enum NOT NULL,
    cad_numbers      TEXT[]      NOT NULL DEFAULT '{}',
    case_numbers     TEXT[]      NOT NULL DEFAULT '{}',
    foia_eligible    BOOLEAN     NOT NULL DEFAULT false,
    -- Chunk-specific
    chunk_index      SMALLINT    NOT NULL,
    chunk_text       TEXT        NOT NULL,
    token_count      SMALLINT,
    qdrant_point_id  UUID        UNIQUE,          -- set after embedding
    embedding_model  TEXT,
    embedded_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_chunks_doc_id
    ON chunks (doc_id);

CREATE INDEX IF NOT EXISTS ix_chunks_agency_id
    ON chunks (agency_id);

CREATE INDEX IF NOT EXISTS ix_chunks_foia_eligible
    ON chunks (chunk_id)
    WHERE foia_eligible = true;

CREATE INDEX IF NOT EXISTS ix_chunks_qdrant_point_id
    ON chunks (qdrant_point_id);

CREATE UNIQUE INDEX IF NOT EXISTS ix_chunks_doc_chunk
    ON chunks (doc_id, chunk_index);

-- ─────────────────────────────────────────────
-- foia_queue
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS foia_queue (
    queue_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    doc_id           UUID        NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
    agency_id        TEXT        NOT NULL,
    cad_numbers      TEXT[]      NOT NULL DEFAULT '{}',  -- snapshot at enqueue time
    case_numbers     TEXT[]      NOT NULL DEFAULT '{}',  -- snapshot at enqueue time
    priority_score   SMALLINT    NOT NULL DEFAULT 0 CHECK (priority_score BETWEEN 0 AND 100),
    status           foia_status_enum NOT NULL DEFAULT 'pending',
    submitted_at     TIMESTAMPTZ,
    acknowledged_at  TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    notes            TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_foia_queue_doc_id UNIQUE (doc_id)
);

CREATE INDEX IF NOT EXISTS ix_foia_queue_agency_id
    ON foia_queue (agency_id);

CREATE INDEX IF NOT EXISTS ix_foia_queue_status
    ON foia_queue (status);

CREATE INDEX IF NOT EXISTS ix_foia_queue_status_priority
    ON foia_queue (status, priority_score DESC);

-- ─────────────────────────────────────────────
-- review_queue
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS review_queue (
    review_id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    doc_id           UUID        NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
    agency_id        TEXT        NOT NULL,
    parse_quality    SMALLINT,                    -- snapshot at flag time
    reason           review_reason_enum NOT NULL DEFAULT 'low_parse_quality',
    status           review_status_enum NOT NULL DEFAULT 'pending',
    reviewer_notes   TEXT,
    reviewed_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_review_queue_doc_id UNIQUE (doc_id)
);

CREATE INDEX IF NOT EXISTS ix_review_queue_agency_id
    ON review_queue (agency_id);

CREATE INDEX IF NOT EXISTS ix_review_queue_status
    ON review_queue (status);
