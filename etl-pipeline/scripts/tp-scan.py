#!/usr/bin/env python
"""CLI tool for managing Turbopuffer namespaces."""

import sys
import os
import argparse
import logging
import json

import turbopuffer as tpuf

logger = logging.getLogger(__name__)


def get_client(
    api_key: str | None = None, region: str = "aws-us-east-1"
) -> tpuf.Turbopuffer:
    """Initialize turbopuffer client with API key.

    Priority: api_key arg > TURBOPUFFER_API_KEY env var
    Region: region arg > TURBOPUFFER_REGION env var > default (aws-us-east-1)
    """
    key = api_key or os.environ.get("TURBOPUFFER_API_KEY")
    if not key:
        logger.error(
            "No API key provided. Use --api-key or set TURBOPUFFER_API_KEY env var."
        )
        sys.exit(1)
    tpuf.api_key = key
    region = os.environ.get("TURBOPUFFER_REGION", region)
    return tpuf.Turbopuffer(region=region)


def row_to_dict(row) -> dict:
    """Convert a turbopuffer row to a plain dict."""
    return row.model_dump() if hasattr(row, "model_dump") else dict(row)


def print_docs(rows, json_output: bool = False, detailed: bool = False):
    """Print documents in JSON or human-readable format.

    Args:
        rows: List of turbopuffer rows (or a single row)
        json_output: If True, output as JSON
        detailed: If True (and not json_output), show full details per doc
    """
    # Handle single row - check for model_dump to detect single row object
    if hasattr(rows, "model_dump"):
        rows = [rows]

    docs = [row_to_dict(row) for row in rows]

    if json_output:
        # Single doc -> object, multiple -> array
        output = docs[0] if len(docs) == 1 else docs
        print(json.dumps(output, indent=2, default=str))
    else:
        for doc in docs:
            if detailed:
                for key, value in doc.items():
                    if key == "vector" and value:
                        print(f"  {key}: [{len(value)} dims]")
                    elif key == "text" and value and len(str(value)) > 200:
                        print(f"  {key}: {str(value)[:200]}...")
                    else:
                        print(f"  {key}: {value}")
            else:
                doc_id = doc.get("id", "?")
                text = doc.get("text", "")
                if text and len(text) > 100:
                    text = text[:100] + "..."
                print(f"  [{doc_id}] {text}")


def cmd_list(args):
    """List all namespaces."""
    client = get_client(args.api_key)
    namespaces = list(client.namespaces())

    if args.json:
        print(json.dumps([ns.id for ns in namespaces], indent=2))
    else:
        logger.info(f"Found {len(namespaces)} namespaces:")
        for ns in namespaces:
            print(f"  - {ns.id}")


def cmd_info(args):
    """Get info/metadata for a namespace."""
    client = get_client(args.api_key)
    ns = client.namespace(args.namespace)

    try:
        metadata = ns.metadata()

        if args.json:
            data = {
                "namespace": args.namespace,
                "approx_row_count": metadata.approx_row_count,
                "approx_logical_bytes": metadata.approx_logical_bytes,
                "created_at": str(metadata.created_at) if metadata.created_at else None,
                "updated_at": str(metadata.updated_at) if metadata.updated_at else None,
                "index_status": metadata.index.status if metadata.index else None,
            }
            print(json.dumps(data, indent=2))
        else:
            logger.info(f"Namespace: {args.namespace}")
            print(f"  Approx rows:  {metadata.approx_row_count:,}")
            print(f"  Approx size:  {metadata.approx_logical_bytes:,} bytes")
            print(f"  Created at:   {metadata.created_at}")
            print(f"  Updated at:   {metadata.updated_at}")
            if metadata.index:
                print(f"  Index status: {metadata.index.status}")
    except Exception as e:
        logger.error(f"Failed to get metadata for '{args.namespace}': {e}")
        sys.exit(1)


def cmd_schema(args):
    """Get schema for a namespace."""
    client = get_client(args.api_key)
    ns = client.namespace(args.namespace)

    try:
        schema = ns.schema()

        if args.json:
            print(json.dumps(dict(schema), indent=2, default=str))
        else:
            logger.info(f"Schema for namespace '{args.namespace}':")
            for field, spec in schema.items():
                print(f"  {field}: {spec}")
    except Exception as e:
        logger.error(f"Failed to get schema for '{args.namespace}': {e}")
        sys.exit(1)


def cmd_delete(args):
    """Delete a namespace."""
    client = get_client(args.api_key)
    ns = client.namespace(args.namespace)

    if not args.force:
        try:
            metadata = ns.metadata()
            count = metadata.approx_row_count
        except Exception:
            count = "unknown"
        # NOTE: maybe we should remove force, i.e. always ask for confirmation
        confirm = input(f"Delete namespace '{args.namespace}' ({count:,} rows)? [y/N] ")
        if confirm.lower() != "y":
            logger.info("Aborted.")
            return

    try:
        ns.delete_all()
        logger.info(f"Deleted namespace '{args.namespace}'")
    except Exception as e:
        logger.error(f"Failed to delete namespace '{args.namespace}': {e}")
        sys.exit(1)


def cmd_warm(args):
    """Warm the cache for a namespace.

    Hints turbopuffer to prepare for low-latency requests.
    Useful before user sessions or latency-sensitive operations.
    """
    client = get_client(args.api_key)
    ns = client.namespace(args.namespace)

    try:
        result = ns.hint_cache_warm()
        if args.json:
            print(
                json.dumps(
                    {
                        "namespace": args.namespace,
                        "status": result.status,
                        "message": result.message,
                    }
                )
            )
        else:
            print(f"{result.status}: {result.message}")
    except Exception as e:
        logger.error(f"Failed to warm cache for '{args.namespace}': {e}")
        sys.exit(1)


def cmd_recall(args):
    """Test ANN recall quality for a namespace.

    Compares approximate nearest neighbor results against exact kNN.
    A recall of 1.0 means 100% of exact results were found by ANN.
    """
    client = get_client(args.api_key)
    ns = client.namespace(args.namespace)

    try:
        result = ns.recall(num=args.num, top_k=args.top_k)
        if args.json:
            print(
                json.dumps(
                    {
                        "namespace": args.namespace,
                        "avg_recall": result.avg_recall,
                        "avg_ann_count": result.avg_ann_count,
                        "avg_exhaustive_count": result.avg_exhaustive_count,
                    },
                    indent=2,
                )
            )
        else:
            print(f"Recall: {result.avg_recall:.2%}")
            print(f"  ANN count:        {result.avg_ann_count:.1f}")
            print(f"  Exhaustive count: {result.avg_exhaustive_count:.1f}")
    except Exception as e:
        logger.error(f"Failed to test recall for '{args.namespace}': {e}")
        sys.exit(1)


def cmd_get(args):
    """Get a document by ID."""
    client = get_client(args.api_key)
    ns = client.namespace(args.namespace)

    try:
        query_kwargs = {
            "rank_by": ("id", "asc"),
            "top_k": 1,
            "filters": ["id", "Eq", args.id],
        }
        # vectors are large and often not needed so exclude by default unless --vectors is specified
        if args.vectors:
            query_kwargs["include_attributes"] = True
        else:
            query_kwargs["exclude_attributes"] = ["vector"]

        result = ns.query(**query_kwargs)

        if not result.rows:
            logger.error(f"Document '{args.id}' not found in '{args.namespace}'")
            sys.exit(1)

        print_docs(result.rows[0], json_output=args.json, detailed=True)
    except Exception as e:
        logger.error(f"Failed to get document '{args.id}' from '{args.namespace}': {e}")
        sys.exit(1)


def cmd_sample(args):
    """Sample documents from a namespace."""
    client = get_client(args.api_key)
    ns = client.namespace(args.namespace)

    try:
        query_kwargs = {
            "rank_by": ("id", "asc"),
            "top_k": args.limit,
        }
        # vectors are large and often not needed so exclude by default unless --vectors is specified
        if args.vectors:
            query_kwargs["include_attributes"] = True
        else:
            query_kwargs["exclude_attributes"] = ["vector"]

        result = ns.query(**query_kwargs)

        if not args.json:
            logger.info(f"Sample of {len(result.rows)} docs from '{args.namespace}':")
        print_docs(result.rows, json_output=args.json, detailed=False)
    except Exception as e:
        logger.error(f"Failed to sample from '{args.namespace}': {e}")
        sys.exit(1)


def cmd_query(args):
    """Execute a raw JSON query against a namespace.

    Takes JSON either as positional arg or from --file.
    """
    client = get_client(args.api_key)
    ns = client.namespace(args.namespace)

    # Get JSON from positional arg or --file (but not both)
    if args.query_json and args.file:
        logger.error("Provide either a JSON query or --file, not both.")
        sys.exit(1)
    elif args.query_json:
        query_json = args.query_json
    elif args.file:
        try:
            with open(args.file, "r") as f:
                query_json = f.read()
        except Exception as e:
            logger.error(f"Failed to read query file '{args.file}': {e}")
            sys.exit(1)
    else:
        logger.error("No query provided. Use positional JSON arg or --file.")
        sys.exit(1)

    try:
        query_kwargs = json.loads(query_json)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON: {e}")
        sys.exit(1)

    # Convert rank_by list to tuple if needed (JSON doesn't have tuples)
    if "rank_by" in query_kwargs and isinstance(query_kwargs["rank_by"], list):
        query_kwargs["rank_by"] = tuple(query_kwargs["rank_by"])

    # Exclude vectors by default unless explicitly requested
    if not args.vectors and "exclude_attributes" not in query_kwargs:
        query_kwargs["exclude_attributes"] = ["vector"]

    try:
        result = ns.query(**query_kwargs)

        if not args.json:
            logger.info(
                f"Query returned {len(result.rows)} docs from '{args.namespace}':"
            )
        print_docs(result.rows, json_output=args.json, detailed=args.detailed)
    except Exception as e:
        logger.error(f"Query failed: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Manage Turbopuffer namespaces",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment:
  TURBOPUFFER_API_KEY    API key (can also use --api-key)

Examples:
  %(prog)s list                        # List all namespaces
  %(prog)s info vra-dev                # Get metadata for namespace
  %(prog)s schema vra-dev              # Get schema for namespace
  %(prog)s get vra-dev doc123          # Get document by ID
  %(prog)s sample vra-dev --limit 5    # Sample 5 documents
  %(prog)s query vra-dev '{"rank_by": ["id", "asc"], "top_k": 5}'
  %(prog)s query vra-dev --file query.json
  %(prog)s recall vra-dev              # Test ANN recall quality
  %(prog)s warm vra-dev                # Warm cache for low-latency queries
  %(prog)s delete vra-dev --force      # Delete namespace (no confirmation)
""",
    )
    parser.add_argument(
        "--api-key", help="Turbopuffer API key (or set TURBOPUFFER_API_KEY)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # list
    p_list = subparsers.add_parser("list", help="List all namespaces")
    p_list.add_argument("--json", action="store_true", help="Output as JSON")
    p_list.set_defaults(func=cmd_list)

    # info
    p_info = subparsers.add_parser("info", help="Get namespace metadata")
    p_info.add_argument("namespace", help="Namespace name")
    p_info.add_argument("--json", action="store_true", help="Output as JSON")
    p_info.set_defaults(func=cmd_info)

    # schema
    p_schema = subparsers.add_parser("schema", help="Get namespace schema")
    p_schema.add_argument("namespace", help="Namespace name")
    p_schema.add_argument("--json", action="store_true", help="Output as JSON")
    p_schema.set_defaults(func=cmd_schema)

    # get
    p_get = subparsers.add_parser("get", help="Get document by ID")
    p_get.add_argument("namespace", help="Namespace name")
    p_get.add_argument("id", help="Document ID")
    p_get.add_argument(
        "--vectors", action="store_true", help="Include vectors in output"
    )
    p_get.add_argument("--json", action="store_true", help="Output as JSON")
    p_get.set_defaults(func=cmd_get)

    # sample
    p_sample = subparsers.add_parser("sample", help="Sample documents")
    p_sample.add_argument("namespace", help="Namespace name")
    p_sample.add_argument(
        "--limit", "-n", type=int, default=5, help="Number of docs (default: 5)"
    )
    p_sample.add_argument(
        "--vectors", action="store_true", help="Include vectors in output"
    )
    p_sample.add_argument("--json", action="store_true", help="Output as JSON")
    p_sample.set_defaults(func=cmd_sample)

    # query
    p_query = subparsers.add_parser("query", help="Execute raw JSON query")
    p_query.add_argument("namespace", help="Namespace name")
    p_query.add_argument("query_json", nargs="?", help="Query as JSON string")
    p_query.add_argument("--file", "-f", help="Read query from JSON file")
    p_query.add_argument(
        "--vectors", action="store_true", help="Include vectors in output"
    )
    p_query.add_argument(
        "--detailed", "-d", action="store_true", help="Show detailed output"
    )
    p_query.add_argument("--json", action="store_true", help="Output as JSON")
    p_query.set_defaults(func=cmd_query)

    # recall
    p_recall = subparsers.add_parser("recall", help="Test ANN recall quality")
    p_recall.add_argument("namespace", help="Namespace name")
    p_recall.add_argument(
        "--num", type=int, default=25, help="Number of searches to run (default: 25)"
    )
    p_recall.add_argument(
        "--top-k", type=int, default=10, help="Search for top_k neighbors (default: 10)"
    )
    p_recall.add_argument("--json", action="store_true", help="Output as JSON")
    p_recall.set_defaults(func=cmd_recall)

    # warm
    p_warm = subparsers.add_parser("warm", help="Warm cache for low-latency queries")
    p_warm.add_argument("namespace", help="Namespace name")
    p_warm.add_argument("--json", action="store_true", help="Output as JSON")
    p_warm.set_defaults(func=cmd_warm)

    # delete
    p_delete = subparsers.add_parser("delete", help="Delete a namespace")
    p_delete.add_argument("namespace", help="Namespace name")
    p_delete.add_argument(
        "--force", "-f", action="store_true", help="Skip confirmation"
    )
    p_delete.set_defaults(func=cmd_delete)

    args = parser.parse_args()

    # Configure logging based on verbosity
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    args.func(args)


if __name__ == "__main__":
    main()
