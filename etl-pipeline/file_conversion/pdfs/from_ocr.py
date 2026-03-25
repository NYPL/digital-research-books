import math
import os
import pathlib
import pypdf
import tempfile

from . import mets_parser
from . import model
from . import path
from . import page_label
from . import pdf_page
from utils.chunker import chunk
from digital_assets import get_stored_file_url
from managers import S3Manager
from services.monitor import track_time

from logger import create_log

DEFAULT_CHUNK_SIZE = 100
NUMBER_OF_SUBPROCESSES = os.cpu_count() or 12

logger = create_log(__name__)


@track_time(function_name="PDFGeneration", logger=logger)
def generate_pdf(
    storage_manager: S3Manager,
    bucket_name: str,
    upload_bucket_name: str,
    file_permissions: dict,
    barcode: str,
    ocr_dir: str,
    mets_file_key: str,
) -> str:
    with tempfile.TemporaryDirectory() as tmpdirname:
        mets_file = mets_parser.METSFile.from_mets_str(
            storage_manager.get_object(key=mets_file_key, bucket=bucket_name)[
                "Body"
            ].read()
        )
        mets_path = path.METSPath(mets_file_key)

        metadata = model.get_metadata(
            storage_manager, bucket_name, mets_path, mets_file
        )

        ordered_page_locations = _generate_individual_pdf_pages(
            mets_file, ocr_dir, bucket_name, tmpdirname
        )

        pdf_url = _merge_and_upload_pdf(
            ordered_page_locations,
            barcode,
            mets_file,
            metadata,
            storage_manager,
            upload_bucket_name,
            file_permissions,
            tmpdirname,
        )

    return pdf_url


def _generate_individual_pdf_pages(
    mets_file: "mets_parser.METSFile", ocr_dir: str, bucket_name: str, tmpdirname: str
) -> list[str]:
    ordered_page_locations = []
    page_generator = pdf_page.PDFPageGenerator(bucket_name, ocr_dir)

    page_count = mets_file.page_count
    chunk_size = (
        math.ceil(page_count / NUMBER_OF_SUBPROCESSES)
        if page_count
        else DEFAULT_CHUNK_SIZE
    )

    processes = []
    for i, pages in enumerate(chunk(mets_file.iter_pages(), size=chunk_size), start=1):
        logger.info(f"Building chunk {i}")
        subprocess = pdf_page.PDFPageSubprocess(page_generator)

        for page in pages:
            pdf_page_location = str(
                pathlib.Path(tmpdirname, page.image_file.fid).with_suffix(".pdf"),
            )

            if not page.ocr_file.location:
                continue

            ordered_page_locations.append(pdf_page_location)
            subprocess.add_page(page, pdf_page_location)

        logger.info(f"Starting subprocess for chunk {i}")
        subprocess.start()
        processes.append(subprocess)

    for subprocess in processes:
        subprocess.join()
        if subprocess.process.exitcode != 0:
            raise RuntimeError("PDF generation subprocess failed")

    return ordered_page_locations


def _merge_and_upload_pdf(
    ordered_page_locations: list[str],
    barcode: str,
    mets_file: mets_parser.METSFile,
    metadata: model.Metadata,
    storage_manager: S3Manager,
    bucket_name: str,
    file_permissions: dict[str, str],
    tmpdirname: str,
) -> str:
    with pypdf.PdfWriter() as writer:
        if metadata:
            writer.add_metadata(
                {
                    "/Title": metadata.title or "",
                    "/Author": metadata.author or "",
                    "/Subject": metadata.subject or "",
                }
            )

        chapter_counter = 0
        page_labeler = page_label.PageLabeler()

        for i, (pdf_page_location, mets_page) in enumerate(
            zip(ordered_page_locations, mets_file.iter_pages())
        ):
            writer.append(pdf_page_location)

            if mets_page.is_chapter_start:
                chapter_counter += 1
                writer.add_outline_item(
                    title=f"Chapter {chapter_counter}", page_number=i
                )

            if mets_page.order_label:
                page_labeler.add_page_label(page_index=i, label=mets_page.order_label)

            # Free disk space after appending the pdf page
            os.remove(pdf_page_location)

        page_labeler.write(writer)

        merged_pdf_path = f"{tmpdirname}/merged.pdf"
        with open(merged_pdf_path, "wb") as merged_pdf:
            writer.write(merged_pdf)

        with open(merged_pdf_path, "rb") as merged_pdf:
            output_key = f"pdfs/{barcode}.pdf"
            storage_manager.client.upload_fileobj(
                merged_pdf, bucket_name, str(output_key), file_permissions
            )

    logger.info(f"Generated PDF: {output_key}")
    return get_stored_file_url(bucket_name, output_key)
