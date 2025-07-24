import os

from managers import DBManager, SQSManager
from sqlalchemy import select
from model import Record, GRINStatus
from utils.chunker import chunk


def main():
    sqs_manager = SQSManager(queue_name=os.environ["GRIN_INGEST_SQS_QUEUE"])

    with DBManager() as db_manager:
        # Change this query to select barcodes to redrive
        query = (
            select(GRINStatus.barcode)
            .join(GRINStatus.record)
            .filter(Record.state == "staged", GRINStatus.state == "downloaded")
        )

        barcodes = db_manager.session.execute(query).scalars().all()

        for chunked_barcodes in chunk(barcodes, 10):
            sqs_manager.send_message_to_queue({"barcodes": chunked_barcodes})


if __name__ == "__main__":
    main()
