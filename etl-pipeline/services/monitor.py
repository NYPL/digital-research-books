import newrelic.agent

class Monitor:
    def __init__(self):
        self.metrics = {}

    @staticmethod
    def record_event(event_name: str, data: dict):
        newrelic.agent.record_custom_event(event_name, data)

    @staticmethod
    def track_work_records_chosen(record_id: str, num_records: int):
        event_name = f"FRBR:WorkRecordsChosen"
        Monitor.record_event(event_name, {"num_records": num_records, "record.id": record_id})
