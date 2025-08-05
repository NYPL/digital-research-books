import base64
from ocrmypdf.hocrtransform import HocrTransform
import os
import tempfile

from api.utils import APIUtils
from .items import items_blueprint
import file_conversion.pdfs.mets_parser as mets_parser
from managers import S3Manager


RESPONSE_TYPE = "itemRead"


@items_blueprint.route("/<item_id>/read/<page_id>", methods=["GET"])
def item_read(item_id, page_id):
    bucket = os.environ["PRIVATE_FILE_BUCKET"]
    prefix = f"grin/33433115534525/{page_id}"

    storage_manager = S3Manager()

    mets_file = mets_parser.METSFile.from_mets_str(
        storage_manager.get_object(
            key=f"grin/33433115534525/NYPL_33433115534525.xml",
            bucket=os.environ["PRIVATE_FILE_BUCKET"],
        )["Body"].read()
    )

    previous_pages, next_pages = mets_file.get_surrounding_pages(page_id)

    response = storage_manager.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj["Key"] for obj in response.get("Contents", [])]

    ocr_key = next((f for f in files if f.endswith("html")), None)
    image_key = next(
        (
            f
            for f in files
            if f.lower().endswith((".png", ".jpg", ".jpeg", ".tiff", ".tif", ".jp2"))
        ),
        None,
    )

    if not ocr_key or not image_key:
        return APIUtils.formatResponseObject(
            404, RESPONSE_TYPE, {"message": "Page not found"}
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        ocr_path = os.path.join(tmpdir, os.path.basename(ocr_key))
        image_path = os.path.join(tmpdir, os.path.basename(image_key))

        storage_manager.client.download_file(bucket, ocr_key, ocr_path)
        storage_manager.client.download_file(bucket, image_key, image_path)

        pdf_path = os.path.join(tmpdir, f"{page_id}.pdf")

        hocr_transform = HocrTransform(hocr_filename=ocr_path, dpi=300)
        hocr_transform.to_pdf(out_filename=pdf_path, image_filename=image_path)

        with open(pdf_path, "rb") as f:
            pdf_data = f.read()

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
