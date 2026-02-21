# Phase 2: Registry Enrichment & Feed URL Discovery

## Context

After the initial Phase 2 ingestion run, 121 of 165 agencies with known platform types failed to scrape. All failures fell into exactly two categories — **not parser bugs**:

| Issue | Count | Root cause |
|---|---|---|
| `no_url` | 110 | Feed URL stored as `"N/A"` placeholder in registry |
| `no_parser_config` | 11 | CrimeMapping agencies missing `crimemapping_agency_id` |

Parsers themselves have a **100% success rate** on agencies with valid URLs.

---

## Discovery Scripts

### `scripts/discover_feed_urls.py`

Finds real feed URLs for agencies where the active feed URL is `"N/A"`.

**How it works:**
1. Fetches the agency's `homepage_url`
2. Probes platform-specific common paths (e.g. `/CivicAlerts.aspx`, `/News` for CivicPlus)
3. Crawls the homepage for internal links matching keywords: `news`, `press`, `release`, `alert`, `blotter`, `arrest`, `log`, `crime`, `activity`, `incident`
4. Tests each candidate URL against the agency's real parser
5. Writes the first URL that returns ≥1 document back to the `agency_feeds` table

**Does not handle CrimeMapping** — those need a numeric ID, not a URL discovery (see below).

**Usage:**
```bash
# Preview without writing to DB
python scripts/discover_feed_urls.py --dry-run

# Run for one platform
python scripts/discover_feed_urls.py --platform civicplus
python scripts/discover_feed_urls.py --platform nixle

# Run for all platforms (except crimemapping)
python scripts/discover_feed_urls.py

# Limit for testing
python scripts/discover_feed_urls.py --platform civicplus --limit 5 --dry-run
```

**When to run again:** Any time new agencies are added to the registry with `"N/A"` feed URLs, or when an agency migrates CMS platforms and their feed URL breaks.

---

### `scripts/discover_crimemapping_ids.py`

Finds the numeric `crimemapping_agency_id` for agencies registered as `platform_type = "crimemapping"` but missing the ID.

**How it works:**
1. Fetches CrimeMapping's public CA agency list from `https://www.crimemapping.com/cap/agencies?state=CA`
2. Fuzzy-matches each agency by name (requires ≥60% word overlap after stripping common suffixes)
3. Falls back to per-agency search via `https://www.crimemapping.com/api/agencies/search` if the list endpoint fails
4. Writes the numeric ID to `agencies.crimemapping_agency_id`

Without `crimemapping_agency_id`, the CrimeMapping parser returns `None` and the agency is skipped entirely.

**Usage:**
```bash
python scripts/discover_crimemapping_ids.py --dry-run  # preview
python scripts/discover_crimemapping_ids.py            # write IDs
```

**Manual fallback:** If the script can't match an agency (name too different), look up the ID directly:
1. Go to `https://www.crimemapping.com/map/agency/{id}` — try incrementing IDs, or
2. Find the agency's CrimeMapping embed URL on their homepage (usually `?agency=NNN` or `/map/agency/NNN`)
3. Set manually: `UPDATE agencies SET crimemapping_agency_id = NNN WHERE agency_id = 'slug';`

---

## Recommended Run Order

Run these before a full ingestion pass when adding new agencies or after the registry is bulk-updated:

```bash
# 1. Populate CrimeMapping numeric IDs
python scripts/discover_crimemapping_ids.py --dry-run
python scripts/discover_crimemapping_ids.py

# 2. Discover feed URLs for remaining N/A agencies
python scripts/discover_feed_urls.py --dry-run
python scripts/discover_feed_urls.py

# 3. Validate coverage
python scripts/validate_ingest.py --output reports/phase2_after_discovery.csv
```

---

## Validation Script: `scripts/validate_ingest.py`

Runs a full parse pass and reports Phase 2 coverage metrics.

**Phase 2 metric definition:** An agency is "covered" when `error is None` — i.e., the parser ran without exception. This is intentionally independent of dedup state: on repeat runs, Redis may have already seen all hashes, making `doc_count = 0` even for working agencies.

**Coverage output columns:**
- `ok` — parser ran successfully
- `no_url` — feed URL is `"N/A"` (registry incomplete)
- `no_cfg` — parser exists but required config is missing (e.g. `crimemapping_agency_id`)
- `error` — parser threw an exception

**Target:** ≥60% of 697 agencies (≥418) reachable.

---

## Notes for Future Automation

- The discovery scripts are **one-time enrichment tools**, not part of the regular scrape loop. Run them when the registry gains new agencies or after bulk imports.
- The Temporal scheduler (`src/scheduler/`) reads feed URLs directly from `agency_feeds` — once a URL is written by a discovery script, the scheduler picks it up automatically on the next run with no other changes needed.
- CivicPlus URL patterns vary per agency (numbered paths like `/602/Arrest-Logs` are common but not predictable). The discovery script tests live — don't try to construct URLs from templates.
- Nixle agencies may have migrated to Rave Mobile Safety. The Nixle parser handles both; the discovery script just needs to find any URL that the parser can scrape.
