import os

from managers import DBManager, SQSManager
from sqlalchemy import select
from model import GRINStatus
from utils.chunker import chunk


def main():
    sqs_manager = SQSManager(queue_name=os.environ["GRIN_INGEST_SQS_QUEUE"])

    with DBManager() as db_manager:
        # Change this query to select barcodes to redrive
        query = select(GRINStatus.barcode).filter(GRINStatus.state == "converted")

        barcodes = db_manager.session.execute(query).scalars().all()

        for chunked_barcodes in chunk(iter(barcodes), 10):
            sqs_manager.send_message_to_queue({"barcodes": chunked_barcodes})


if __name__ == "__main__":
    main()
