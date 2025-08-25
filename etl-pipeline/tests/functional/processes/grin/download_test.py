from datetime import datetime
import os
import pytest
from sqlalchemy import delete
from uuid import uuid4

import file_conversion.pdfs.mets_parser as mets_parser
from processes.grin.download import GRINDownloadService
from model import Record, FRBRStatus, RecordState, Source, GRINStatus, GRINState


@pytest.fixture
def grin_status(db_manager, downloadable_barcode):
    barcode = downloadable_barcode

    # Setup: Ensure a clean state before creating the new record
    db_manager.session.execute(delete(GRINStatus).where(GRINStatus.barcode == barcode))
    db_manager.session.execute(
        delete(Record).where(Record.source_id == f"{barcode}|grin")
    )
    db_manager.commit_changes()
    grin_record = Record(
        uuid=uuid4(),
        frbr_status=FRBRStatus.TODO.value,
        cluster_status=False,
        state=RecordState.STAGED.value,
        source_id=f"{barcode}|grin",
        source=Source.GRIN.value,
        grin_status=GRINStatus(
            barcode=barcode,
            failed_download=0,
            state=GRINState.CONVERTED.value,
            date_created=datetime(1991, 8, 25),
        ),
    )

    db_manager.session.add(grin_record)
    db_manager.commit_changes()

    yield grin_record.grin_status

    db_manager.session.execute(delete(GRINStatus).where(GRINStatus.barcode == barcode))
    db_manager.session.execute(
        delete(Record).where(Record.source_id == f"{barcode}|grin")
    )
    db_manager.commit_changes()


def test_grin_download(s3_manager, grin_status):
    bucket = os.environ["PRIVATE_FILE_BUCKET"]
    grin_download_service = GRINDownloadService(bucket=bucket)

    ocr_dir, mets_file_path = grin_download_service.download_barcode(
        grin_status.barcode
    )

    assert_ocr_package_downloaded(
        s3_manager, bucket, grin_status.barcode, mets_file_path, ocr_dir
    )
    assert_mets_file_uploaded(s3_manager, bucket, mets_file_path)

    delete_ocr_package(s3_manager, bucket, ocr_dir)


def assert_ocr_package_downloaded(s3_manager, bucket, barcode, mets_file_path, ocr_dir):
    ocr_package_head_response = s3_manager.client.head_object(
        Bucket=bucket, Key=f"{ocr_dir}{barcode}.tar.gz.gpg"
    )

    assert ocr_package_head_response["ResponseMetadata"]["HTTPStatusCode"] == 200

    mets_file_head_response = s3_manager.client.head_object(
        Bucket=bucket, Key=f"{mets_file_path}"
    )

    assert mets_file_head_response["ResponseMetadata"]["HTTPStatusCode"] == 200


def assert_mets_file_uploaded(s3_manager, bucket, mets_file_path):
    mets_file = s3_manager.get_object(key=mets_file_path, bucket=bucket)

    assert mets_file["ResponseMetadata"]["HTTPStatusCode"] == 200


def delete_ocr_package(s3_manager, bucket, ocr_dir):
    paginator = s3_manager.client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=ocr_dir)

    objects_to_delete = []

    for page in pages:
        for object in page.get("Contents", []):
            objects_to_delete.append({"Key": object["Key"]})

    if objects_to_delete:
        for i in range(0, len(objects_to_delete), 1000):
            s3_manager.client.delete_objects(
                Bucket=bucket, Delete={"Objects": objects_to_delete[i : i + 1000]}
            )
