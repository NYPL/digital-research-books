"""Count chunks where start_page == end_page in a turbopuffer index.

Usage:
    uv run python dev_scripts/count_single_page_chunks.py
    uv run python dev_scripts/count_single_page_chunks.py --index vra-prod
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.load_env import load_env
from vector_indexing.components.backends.turbopuffer import TurbopufferBackend

DEFAULT_INDEX = "vra-dev"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", default=DEFAULT_INDEX)
    parser.add_argument("--env", default="production")
    args = parser.parse_args()

    load_env(f"config/.env.{args.env}")

    backend = TurbopufferBackend(index_name=args.index)

    print(f"Scanning index: {args.index}")

    total = 0
    matched = 0

    for chunk in backend.scan():
        total += 1
        if chunk.start_page == chunk.end_page:
            matched += 1
        if total % 10_000 == 0:
            print(f"  scanned {total:,} docs, matched {matched:,} so far...")

    print(f"\nDone. Total docs: {total:,}")
    print(
        f"start_page == end_page: {matched:,} ({matched / total * 100:.1f}%)"
        if total
        else "No documents found."
    )


if __name__ == "__main__":
    main()
