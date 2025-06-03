import newrelic.agent
from model import Record, Source


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


def track_oclc_related_records_found(
    record: Record, num_matches: int, fell_back_to_title_author: bool
):
    event_name = "Embellish:OCLCRelatedRecordsFound"
    record_record_event(
        record,
        event_name,
        {
            "num_matches": num_matches,
            "fell_back_to_title_author": fell_back_to_title_author,
        },
    )


def track_editions_identified(
    record: Record, num_clusters: int, num_editions: int, num_records: int
):
    event_name = "Cluster:EditionsIdentified"
    record_record_event(
        record,
        event_name,
        {
            "num_clusters": num_clusters,
            "num_editions": num_editions,
            "num_records": num_records,
        },
    )


def track_record_pipeline_message_succeeded(
    record, execution_time: float, message_body: str
):
    event_name = "RecordPipeline:MessageSucceeded"
    data = {
        "message_body": message_body,
        "execution_time": execution_time,
    }
    record_record_event(record, event_name, data=data)


def track_record_pipeline_message_failed(execution_time: float, message_body: str):
    event_name = "RecordPipeline:MessageFailed"
    data = {
        "message_body": message_body,
        "execution_time": execution_time,
    }
    record_event(event_name, data=data)


def track_records_ingested(number_of_records: str, source: Source):
    event_name = "RecordIngest:IngestCount"
    data = {
        "number_of_records": number_of_records,
        "source": source
    }

    record_event(event_name, data)
