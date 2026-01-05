import os

import boto3
from sqlalchemy import func, select
import file_conversion.pdfs.mets_parser as mets_parser


from logger import create_log
from digital_assets.utils.get_stored_file_url import get_stored_file_url
from model import Record, Part, Source, FileFlags, GRINStatus, GRINState
from managers import DBManager, SQSManager


logger = create_log(__name__)

BATCH_SIZE = 1000


def get_mets_file_from_s3(s3_client, barcode: str) -> mets_parser.METSFile:
    key = f"grin/{barcode}/NYPL_{barcode}.xml"
    try:
        s3_object = s3_client.get_object(
            Key=key,
            Bucket=os.environ["PRIVATE_FILE_BUCKET"],
        )
        mets_str = s3_object["Body"].read()
        return mets_parser.METSFile.from_mets_str(mets_str)
    except Exception as e:
        logger.error(f"Error fetching/parsing METS file for barcode {barcode}: {e}")
        return None


def create_first_page_part(barcode: str, mets_file: mets_parser.METSFile) -> Part:
    if not mets_file:
        return None

    return Part(
        index=1,
        url=get_stored_file_url(
            storage_name=os.environ["PRIVATE_FILE_BUCKET"],
            file_path=f"grin/{barcode}/{mets_file.first_page}",
        ),
        source=Source.GRIN.value,
        file_type="application/ocr",
        flags=str(FileFlags(reader=True)),
    )


def windowed_query(session, stmt, column, windowsize):
    stmt = stmt.add_columns(column).order_by(column)
    last_id = None

    while True:
        subq = stmt

        if last_id is not None:
            subq = subq.filter(column > last_id)

        result = session.execute(subq.limit(windowsize))
        chunk = result.all()

        if not chunk:
            break

        last_id = chunk[-1][-1]

        for row in chunk:
            yield row[0]


def main():
    record_pipeline_queue = os.environ["RECORD_PIPELINE_SQS_QUEUE"]
    sqs_manager = SQSManager(record_pipeline_queue)
    s3_client = boto3.client("s3")
    bucket = os.environ["PRIVATE_FILE_BUCKET"]

    with DBManager() as db_manager:
        stmt = (
            select(Record)
            .join(GRINStatus, GRINStatus.record_id == Record.id)
            .filter(
                Record.source == "grin",
                func.split_part(Record.rights, "|", 2) == "public_domain",
                GRINStatus.state == GRINState.DOWNLOADED.value,
                Record.state == "ingested",
            )
        )

        total_records = db_manager.session.scalar(
            select(func.count()).select_from(stmt.alias())
        )
        logger.info(f"Found {total_records} GRIN records to process")

        total_updated = 0
        total_has_first_page = 0
        batch_count = 0

        for record in windowed_query(db_manager.session, stmt, Record.id, BATCH_SIZE):
            try:
                logger.info(f"Processing record with source_id {record.source_id}")
                barcode = record.source_id.split("|")[0]
                first_page_url = (
                    f"https://{bucket}.s3.amazonaws.com/grin/{barcode}/00000001"
                )

                has_first_page = record.has_part and any(
                    first_page_url in str(part) for part in record.has_part
                )

                if not has_first_page:
                    mets_file = get_mets_file_from_s3(s3_client, barcode)
                    if mets_file:
                        first_page_part = create_first_page_part(barcode, mets_file)
                        record.has_part.append(str(first_page_part))

                        db_manager.session.add(record)
                        total_updated += 1
                        batch_count += 1
                else:
                    total_has_first_page += 1
                    logger.info(
                        f"Record {record.source_id} already has first_page_part"
                    )

                sqs_manager.send_message_to_queue(
                    message={"source_id": record.source_id, "source": record.source}
                )

                logger.info(f"Sent record {record.source_id} to queue")

                if batch_count >= BATCH_SIZE:
                    db_manager.commit_changes()
                    logger.info(
                        f"Committed batch: {total_updated} records updated so far"
                    )
                    batch_count = 0

            except Exception as e:
                logger.error(
                    f"Failed to process record with source_id {record.source_id}: {e}"
                )

        if batch_count > 0:
            db_manager.commit_changes()
            logger.info(f"Committed final batch")

        logger.info(
            f"Updated {total_updated} GRIN records with has_part, {total_has_first_page} already had first_page_part"
        )


if __name__ == "__main__":
    main()
