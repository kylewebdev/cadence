# Phase 5: FOIA Pipeline

Queue and render CPRA public records requests for foia_eligible documents.

## Steps

1. Poll for documents where `foia_eligible = True` and no request has been sent
2. Render CPRA request template (Jinja2 base + Claude Haiku fill-in for
   agency-specific details)
3. Track request status and deadline (10 business days under CPRA)
4. Send deadline reminders and log responses
