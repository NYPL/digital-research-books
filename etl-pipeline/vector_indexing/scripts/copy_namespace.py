#!/usr/bin/env python
"""Copy data between TurboPuffer namespaces with optional filtering.
If doing a full copy to a new namespace, use turbopuffers built in copy_from_namespace for cost and performance savings.
Usage:
    # Copy with a JSON filter:
    python -m vector_indexing.scripts.copy_namespace \
        --src vra-dev \
        --dest vra-dev2 \
        --filter '["barcode", "In", ["12345", "67890"]]'

    # Copy documents matching barcodes in a file:
    python -m vector_indexing.scripts.copy_namespace \
        --src vra-dev \
        --dest vra_test-eval300-gemini_001 \
        --barcode-file dev_scripts/eval300/barcodes_300.txt
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Add project root to path if running directly
if __name__ == "__main__":
    from dotenv import find_dotenv

    project_root = Path(
        find_dotenv("requirements.txt", raise_error_if_not_found=True)
    ).parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

import turbopuffer as tpuf
from vector_indexing.core.config import get_config
from vector_indexing.core.utils import format_bytes
from vector_indexing.components.backends.turbopuffer import (
    TurbopufferBackend,
    TurbopufferBuffer,
)
from logger import create_log

logger = create_log(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Copy data between TurboPuffer namespaces with optional filtering",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--src",
        required=True,
        help="Source namespace name",
    )
    parser.add_argument(
        "--dest",
        required=True,
        help="Destination namespace name",
    )
    filter_group = parser.add_mutually_exclusive_group()
    filter_group.add_argument(
        "--filter",
        type=str,
        default=None,
        help="TurboPuffer filter as JSON string",
    )
    filter_group.add_argument(
        "--barcode-file",
        type=Path,
        default=None,
        metavar="FILE",
        help="Path to a file of barcodes (one per line); builds a barcode In filter",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=os.environ.get("TURBOPUFFER_REGION", "aws-us-east-1"),
        help="TurboPuffer region (default: aws-us-east-1)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied without actually copying",
    )
    parser.add_argument(
        "--env",
        default="production",
        help="Environment name used to load config/.env.<env> (default: production)",
    )

    return parser.parse_args()


def parse_filter(filter_str: str | None) -> list | None:
    """Parse filter JSON string into a list."""
    if filter_str is None:
        return None
    try:
        parsed = json.loads(filter_str)
        if not isinstance(parsed, list):
            raise ValueError("Filter must be a JSON array")
        return parsed
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON filter: {e}")


def copy_namespace(
    src_name: str,
    dest_name: str,
    filters: list | None = None,
    region: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Copy data from source namespace to destination namespace.

    Args:
        src_name: Source namespace name
        dest_name: Destination namespace name
        filters: Optional TurboPuffer filter to apply
        region: TurboPuffer region (e.g. aws-us-east-1)
        dry_run: If True, only count matching documents

    Returns:
        dict with copy statistics
    """
    config = get_config()

    # Set default region if not set by environment
    if region and not os.environ.get("TURBOPUFFER_REGION"):
        os.environ["TURBOPUFFER_REGION"] = region

    src_backend = TurbopufferBackend(src_name, config=config)
    dest_backend = TurbopufferBackend(dest_name, config=config)

    logger.info(f"Source namespace: {src_name}")
    logger.info(f"Destination namespace: {dest_name}")
    logger.info(f"Region: {os.environ.get('TURBOPUFFER_REGION')}")
    if filters:
        logger.info(f"Filter: {json.dumps(filters)}")

    logger.info("Scanning source namespace...")

    if dry_run:
        total_scanned = 0
        total_bytes = 0
        for chunk in src_backend.scan(filters=filters):
            total_scanned += 1
            total_bytes += chunk.estimated_bytes
            if total_scanned % 1000 == 0:
                logger.info(f"Scanned {total_scanned} documents...")

        logger.info(
            f"\n[DRY RUN] Would copy {total_scanned} documents ({format_bytes(total_bytes)})"
        )
        return {
            "documents_scanned": total_scanned,
            "estimated_bytes": total_bytes,
            "written_bytes": 0,
            "dry_run": True,
        }

    # Use TurbopufferBuffer for adaptive batching and retry logic
    with TurbopufferBuffer(dest_backend) as buffer:
        for chunk in src_backend.scan(filters=filters):
            result = buffer.add(chunk)
            if result:
                logger.info(
                    f"Copied {buffer.total_inserted} documents "
                    f"({format_bytes(buffer.total_estimated_bytes)} estimated, "
                    f"{format_bytes(buffer.total_written_bytes)} written)"
                )

    stats = {
        "documents_copied": buffer.total_inserted,
        "estimated_bytes": buffer.total_estimated_bytes,
        "written_bytes": buffer.total_written_bytes,
    }

    logger.info(
        f"\nCopied {buffer.total_inserted} documents "
        f"({format_bytes(buffer.total_estimated_bytes)} estimated, "
        f"{format_bytes(buffer.total_written_bytes)} written)"
    )

    return stats


def main():
    args = parse_args()

    from utils.load_env import load_env

    load_env(f"config/.env.{args.env}")

    from logger import configure_loggers

    configure_loggers(log_level="info", stage="development")

    if args.src == args.dest:
        logger.error("Source and destination namespaces must be different")
        sys.exit(1)

    if args.barcode_file:
        from vector_indexing.scripts.index_books import load_barcodes_from_file

        barcodes = load_barcodes_from_file(args.barcode_file)
        logger.info(f"Loaded {len(barcodes)} barcodes from {args.barcode_file}")
        filters = ["barcode", "In", barcodes]
    else:
        filters = parse_filter(args.filter)

    try:
        stats = copy_namespace(
            src_name=args.src,
            dest_name=args.dest,
            filters=filters,
            region=args.region,
            dry_run=args.dry_run,
        )
        logger.info(f"Stats: {json.dumps(stats, indent=2)}")
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
