# Phase 3: Document Processing

Normalize raw scraped content to a common schema, extract identifiers, and flag
FOIA-eligible documents.

## Steps

1. Strip boilerplate and normalize to common schema
2. Extract CAD/case numbers via regex library (30-50 patterns)
3. Fall back to Claude Haiku structured extraction when regex yields no match
4. Set `foia_eligible = True` when a valid CAD or case number is present and the
   document comes from a public-facing feed
