import base64
import os
from concurrent.futures import ThreadPoolExecutor

from ocrmypdf.hocrtransform import HocrTransform

# api imports
from ...utils import APIUtils
from . import items_blueprint

# shared code imports
import file_conversion.pdfs.mets_parser as mets_parser
from managers import DBManager, S3Manager
from model import Item, Record
from processes.grin.unpack import GRINUnpackService
from utils.common import get_temp_dir
# TODO: since the unpack functions are used in multiple places they should be \
# moved to a shared code location.

RESPONSE_TYPE = "itemRead"


@items_blueprint.route("/<item_id>/read/<page_id>", methods=["GET"])
def item_read(item_id, page_id):
    with DBManager() as db_manager:
        record_source_id = (
            db_manager.session.query(Record.source_id)
            .join(Item, Item.record_id == Record.id)
            .filter(Item.id == int(item_id))
            .scalar()
        )

        barcode = record_source_id.split("|")[0]

    bucket = os.environ["PRIVATE_FILE_BUCKET"]
    prefix = f"grin/{barcode}/{page_id}"

    storage_manager = S3Manager()

    mets_file = mets_parser.METSFile.from_mets_str(
        storage_manager.get_object(
            key=f"grin/{barcode}/NYPL_{barcode}.xml",
            bucket=os.environ["PRIVATE_FILE_BUCKET"],
        )["Body"].read()
    )

    previous_pages, next_pages = mets_file.get_surrounding_pages(page_id)

    response = storage_manager.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj["Key"] for obj in response.get("Contents", [])]

    ocr_key, image_key = _find_files_from_list_objects(files, page_id)

    with get_temp_dir(in_memory=True) as tmpdir:
        if not ocr_key or not image_key:
            unpack_service = GRINUnpackService(bucket)
            unpacked_files = unpack_service.unpack_barcode_package(barcode)
            ocr_key, image_key = _find_files_from_dict(unpacked_files, page_id)

            if not ocr_key or not image_key:
                return APIUtils.formatResponseObject(
                    404, RESPONSE_TYPE, {"message": "Page not found"}
                )

            ocr_path = os.path.join(tmpdir, os.path.basename(ocr_key))
            image_path = os.path.join(tmpdir, os.path.basename(image_key))

            with open(ocr_path, "wb") as f:
                f.write(unpacked_files[ocr_key])
            with open(image_path, "wb") as f:
                f.write(unpacked_files[image_key])
        else:
            ocr_path = os.path.join(tmpdir, os.path.basename(ocr_key))
            image_path = os.path.join(tmpdir, os.path.basename(image_key))

            # Download OCR and image files in parallel
            with ThreadPoolExecutor(max_workers=2) as executor:
                ocr_future = executor.submit(
                    lambda: storage_manager.client.download_file(
                        bucket, ocr_key, ocr_path
                    )
                )
                image_future = executor.submit(
                    lambda: storage_manager.client.download_file(
                        bucket, image_key, image_path
                    )
                )
                ocr_future.result()
                image_future.result()

        pdf_data = _create_pdf_from_paths(ocr_path, image_path, page_id)

    return APIUtils.formatResponseObject(
        200,
        RESPONSE_TYPE,
        {
            "pageName": f"{page_id}.pdf",
            "pageData": base64.b64encode(pdf_data).decode("utf-8"),
            "pageContentType": "application/pdf",
            "previousPages": previous_pages,
            "nextPages": next_pages,
        },
    )


def _create_pdf_from_paths(ocr_path, image_path, page_id):
    with get_temp_dir(in_memory=True) as tmpdir:
        pdf_path = os.path.join(tmpdir, f"{page_id}.pdf")
        hocr_transform = HocrTransform(hocr_filename=ocr_path, dpi=300)
        hocr_transform.to_pdf(out_filename=pdf_path, image_filename=image_path)
        with open(pdf_path, "rb") as f:
            return f.read()


def _find_files_from_list_objects(file_list, page_id):
    ocr_key = next((f for f in file_list if f.endswith("html") and page_id in f), None)
    image_key = next(
        (
            f
            for f in file_list
            if f.lower().endswith((".tif", ".jp2", ".png", ".jpg", ".jpeg", ".tiff"))
            and page_id in f
        ),
        None,
    )

    return ocr_key, image_key


def _find_files_from_dict(file_dict, page_id):
    ocr_key = f"{page_id}.html" if f"{page_id}.html" in file_dict else None

    for suffix in [".tif", ".jp2", ".png", ".jpg", ".jpeg", ".tiff"]:
        if f"{page_id}{suffix}" in file_dict:
            image_key = f"{page_id}{suffix}"
            break
        else:
            image_key = None

    return ocr_key, image_key
