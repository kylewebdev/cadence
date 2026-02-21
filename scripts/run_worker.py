#!/usr/bin/env python
"""Start the Temporal worker and register the ingestion schedule.

Usage:
    python scripts/run_worker.py
"""
import asyncio
import sys
from pathlib import Path

# Make project root importable when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scheduler.worker import main

if __name__ == "__main__":
    asyncio.run(main())
