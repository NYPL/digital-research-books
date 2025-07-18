from dataclasses import dataclass, field
from dateutil import parser
from datetime import datetime, timedelta, timezone
import os


def get_start_datetime(
    process_type: str | None = None, ingest_period: str | None = None
) -> str | None:
    if ingest_period is not None:
        return parser.parse(ingest_period)

    if process_type == "daily":
        return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=24)

    if process_type == "weekly":
        return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=7)

    return None


@dataclass
class ProcessParams:
    process_type: str = "daily"
    custom_file: str | None = None
    ingest_period: str | None = None
    record_id: str | None = None
    limit: int | None = None
    offset: int = 0
    source: str | None = None
    options: dict[str, str] = field(default_factory=dict)


def parse_process_args(*args) -> ProcessParams:
    default_limit = 10 if os.environ.get("ENVIRONMENT") == "qa" else None
    raw_options = args[7] if len(args) > 7 and isinstance(args[7], list) else []
    options = {}

    for option in raw_options:
        if "=" in option:
            key, value = option.split("=", 1)
            options[key] = value

    return ProcessParams(
        process_type=args[0] if (len(args) > 0 and args[0]) else "daily",
        custom_file=args[1] if len(args) > 1 else None,
        ingest_period=args[2] if len(args) > 2 else None,
        record_id=args[3] if len(args) > 3 else None,
        limit=int(args[4]) if len(args) > 4 and args[4] is not None else default_limit,
        offset=int(args[5]) if len(args) > 5 and args[5] is not None else 0,
        source=args[6] if len(args) > 6 else None,
        options=options,
    )
