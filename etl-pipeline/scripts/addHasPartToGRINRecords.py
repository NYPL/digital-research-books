import os

import boto3
from sqlalchemy import func, or_
import file_conversion.pdfs.mets_parser as mets_parser


from logger import create_log
from digital_assets.utils.get_stored_file_url import get_stored_file_url
from model import Record, Part, Source, FileFlags, GRINStatus, GRINState
from managers import DBManager, SQSManager


s3_client = boto3.client("s3")
logger = create_log(__name__)

BATCH_SIZE = 1000


def get_mets_file_from_s3(barcode: str) -> mets_parser.METSFile:
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


def main():
    record_pipeline_queue = os.environ["RECORD_PIPELINE_SQS_QUEUE"]
    sqs_manager = SQSManager(record_pipeline_queue)

    with DBManager() as db_manager:
        query = (
            db_manager.session.query(Record)
            .join(GRINStatus, GRINStatus.record_id == Record.id)
            .filter(
                Record.source == "grin",
                func.lower(Record.rights).contains("public domain"),
                or_(Record.has_part == "{}", Record.has_part.is_(None)),
                GRINStatus.state == GRINState.DOWNLOADED.value,
            )
        )

        total_updated = 0

        for record in query.yield_per(BATCH_SIZE):
            try:
                barcode = record.source_id.split("|")[0]
                mets_file = get_mets_file_from_s3(barcode)

                if mets_file:
                    first_page_part = create_first_page_part(barcode, mets_file)
                    record.has_part = [str(first_page_part)]
                    db_manager.session.add(record)
                    total_updated += 1

                    sqs_manager.send_message_to_queue(
                        message={"source_id": record.source_id, "source": record.source}
                    )
            except Exception as e:
                logger.error(
                    f"Failed to process record with source_id {record.source_id}: {e}"
                )

        db_manager.session.commit()
        logger.info(f"Updated {total_updated} GRIN records with has_part")


if __name__ == "__main__":
    main()
