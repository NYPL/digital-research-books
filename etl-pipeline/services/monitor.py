import newrelic.agent
from model import Record


def record_event(event_name: str, data: dict):
    newrelic.agent.record_custom_event(event_name, data)


def record_record_event(record: Record, event_name: str, data: dict):
    record_event(
        event_name,
        {
            **data,
            "record.id": record.id,
            "record.title": record.title,
            "record.source": record.source,
            "record.identifiers": record.identifiers,
        },
    )


def track_work_records_chosen(record: Record, num_records: int):
    event_name = f"Cluster:WorkRecordsChosen"
    record_record_event(
        record,
        event_name,
        {
            "num_records": num_records,
        },
    )
