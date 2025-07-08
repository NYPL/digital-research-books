import datetime
import io
import math
import os
import pathlib
import pypdf
import re
import tempfile

from . import mets_parser
from . import model
from . import path
from . import page_label
from . import pdf_page
from . import s3
from ..util.chunk import chunk
from ..record_ingestor import RecordIngestor
from mappings.marc_record import map_marc_record
from model import Source
from pymarc import parse_xml_to_array
from xml.etree import ElementTree as ET

from logger import create_log

DEFAULT_CHUNK_SIZE = 100
NUMBER_OF_SUBPROCESSES = os.cpu_count() or 12

logger = create_log(__name__)


def generate_pdf_process(
    bucket_name: str, barcode: str, ocr_dir: str, mets_file_key: str
) -> dict:
    bucket = s3.Bucket(bucket_name)

    with tempfile.TemporaryDirectory() as tmpdirname:
        mets_file, mets_path, metadata, xml_data = _read_mets_and_metadata(
            bucket, mets_file_key
        )

        ordered_page_locations = _generate_individual_pdf_pages(
            mets_file, ocr_dir, bucket_name, tmpdirname
        )

        pdf_url = _merge_and_upload_pdf(
            ordered_page_locations, mets_file, metadata, mets_path, bucket, tmpdirname
        )

        _ingest_record(xml_data, barcode, pdf_url)

    return {"pdf_key": str(mets_path.tagged_pdf_key)}


def _read_mets_and_metadata(bucket: s3.Bucket, mets_file_key: str):
    mets_file = mets_parser.METSFile.from_mets_str(
        bucket.get(key=mets_file_key)["Body"].read()
    )
    mets_path = path.METSPath(mets_file_key)

    metadata, xml_data = model.get_metadata(bucket, mets_path, mets_file)
    model.write_metadata(bucket, mets_path, metadata, mets_path.tagged_pdf_key)

    return mets_file, mets_path, metadata, xml_data


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
    mets_file: mets_parser.METSFile,
    metadata: model.Metadata,
    mets_path: path.METSPath,
    bucket: s3.Bucket,
    tmpdirname: str,
) -> str:
    logger.info("Generating PDF")

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
            output_key = mets_path.tagged_pdf_key
            bucket.upload_file(merged_pdf, output_key)

    logger.info(f"Generated PDF: {output_key}")
    return bucket.get_public_url(output_key)


def _ingest_record(xml_data: ET.Element, barcode: str, pdf_url: str):
    record_ingestor = RecordIngestor(Source.GRIN.value)

    xml_bytes = ET.tostring(xml_data, encoding="utf-8")
    xml_file = io.BytesIO(xml_bytes)

    marc_records = parse_xml_to_array(xml_file)
    for marc_record in marc_records:
        record = map_marc_record(marc_record, source=Source.GRIN, pdf_url=pdf_url)
        record.source_id = f"{barcode}|grin"

        # TODO: use a deterministic method of getting rights status
        if _is_in_public_domain(record):
            record_ingestor.ingest([record])


def _is_in_public_domain(record) -> bool:
    year = int(re.search(r"[0-9]{4}", record.dates[0]).group(0))
    publication_date = datetime.date(year, 1, 1)
    current_year = datetime.date.today().year

    threshold_year = current_year - 95

    public_domain_threshold_date = datetime.date(threshold_year, 1, 1)

    rights = record.rights.lower() if record.rights else ""
    is_public_domain = "public_domain" in rights or "public domain" in rights

    return publication_date < public_domain_threshold_date or is_public_domain
