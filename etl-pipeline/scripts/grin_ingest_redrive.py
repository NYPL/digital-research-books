import os

from managers import DBManager, SQSManager
from sqlalchemy import select
from model import Record, GRINStatus


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

        for barcode in barcodes:
            sqs_manager.send_message_to_queue({"barcode": barcode})


if __name__ == "__main__":
    main()
