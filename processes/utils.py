from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional


def get_start_datetime(process_type: Optional[str]=None, ingest_period: Optional[str]=None) -> Optional[str]:
    if ingest_period is not None:
        return datetime.strptime(ingest_period, '%Y-%m-%dT%H:%M:%S')

    if process_type == 'daily':
        return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=24)
    
    if process_type == 'weekly':
        return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=7)
    
    return None


@dataclass
class ProcessParams:
    process_type: str = 'daily'
    custom_file: Optional[str] = None
    ingest_period: Optional[str] = None
    record_id: Optional[str] = None
    limit: Optional[int] = None
    offset: int = 0
    source: Optional[str] = None


def parse_process_args(*args) -> ProcessParams:
    return ProcessParams(
        process_type=args[0] if len(args) > 0 else 'daily',
        custom_file=args[1] if len(args) > 1 else None,
        ingest_period=args[2] if len(args) > 2 else None,
        record_id=args[3] if len(args) > 3 else None,
        limit=int(args[4]) if len(args) > 4 and args[4] else None,
        offset=int(args[5]) if len(args) > 5  and args[5] else 0,
        source=args[6] if len(args) > 6 else None,
    )
