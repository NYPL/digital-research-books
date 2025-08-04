from flask import Response
from ocrmypdf.hocrtransform import HocrTransform
import os
import tempfile

from api.utils import APIUtils
from .items import items_blueprint
from managers import S3Manager


RESPONSE_TYPE = "itemRead"


@items_blueprint.route("/<item_id>/read/<page_id>", methods=["GET"])
def item_read(item_id, page_id):
    bucket = os.environ["PRIVATE_FILE_BUCKET"]
    prefix = f"grin/33433115534525/{page_id}"

    storage_manager = S3Manager()

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
        print(ocr_path)
        print(image_path)

        storage_manager.client.download_file(bucket, ocr_key, ocr_path)
        storage_manager.client.download_file(bucket, image_key, image_path)

        pdf_path = os.path.join(tmpdir, f"{page_id}.pdf")

        hocr_transform = HocrTransform(hocr_filename=ocr_path, dpi=300)
        hocr_transform.to_pdf(out_filename=pdf_path, image_filename=image_path)

        with open(pdf_path, "rb") as f:
            pdf_data = f.read()

    return Response(
        pdf_data,
        mimetype="application/pdf",
        headers={"Content-Disposition": f"inline; filename={page_id}.pdf"},
    )
