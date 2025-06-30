import math
import os
import pathlib
import tempfile

import pypdf

from . import mets_parser
import model
from . import path
from . import page_label
from . import pdf_page
from . import s3
from ..util.chunk import chunk
from logger import create_log

DEFAULT_CHUNK_SIZE = 100
NUMBER_OF_SUBPROCESSES = os.cpu_count() or 12

logger = create_log(__name__)


class PDFGenerationProcess:
    def __init__(self, bucket, ocr_dir, mets_file):
        self.bucket_name = bucket
        self.bucket = s3.Bucket(self.bucket_name)
        self.ocr_dir = ocr_dir
        self.mets = mets_file

    def run_process(self):
        mets_file = mets_parser.METSFile.from_mets_str(
            self.bucket.get(key=self.mets)["Body"].read(),
        )
        mets_path = path.METSPath(self.mets)

        metadata = model.get_metadata(self.bucket, mets_path, mets_file)
        model.write_metadata(self.bucket, mets_path, metadata, mets_path.tagged_pdf_key)

        ordered_page_locations = []
        page_generator = pdf_page.PDFPageGenerator(self.bucket_name, self.ocr_dir)
        processes = []

        page_count = mets_file.page_count
        chunk_size = (
            math.ceil(page_count / NUMBER_OF_SUBPROCESSES)
            if page_count
            else DEFAULT_CHUNK_SIZE
        )

        with tempfile.TemporaryDirectory() as tmpdirname:
            for i, pages in enumerate(
                chunk(mets_file.iter_pages(), size=chunk_size), start=1
            ):
                logger.info(f"Building chunk {i}")
                subprocess = pdf_page.PDFPageSubprocess(page_generator)
                for page in pages:
                    pdf_page_location = str(
                        pathlib.Path(tmpdirname, page.image_file.fid).with_suffix(
                            ".pdf"
                        ),
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
                        page_labeler.add_page_label(
                            page_index=i, label=mets_page.order_label
                        )

                    # Free disk space after appending the pdf page
                    os.remove(pdf_page_location)

                page_labeler.write(writer)

                with open(f"{tmpdirname}/merged.pdf", "wb") as merged_pdf:
                    writer.write(merged_pdf)

                with open(f"{tmpdirname}/merged.pdf", "rb") as merged_pdf:
                    output_key = path.METSPath(self.mets).tagged_pdf_key
                    self.bucket.upload_file(merged_pdf, output_key)

        logger.info(f"Generated PDF: {output_key}")

        return {"pdf_key": str(mets_path.tagged_pdf_key)}
