import botocore.exceptions
import multiprocessing as mp
import pathlib
import tempfile
import time

from . import checksum
from . import mets_parser
import os

import PIL.Image
from ocrmypdf.hocrtransform import HocrTransform
from lxml import etree
from logger import create_log
from managers import S3Manager

PIL.Image.MAX_IMAGE_PIXELS = None

logger = create_log(__name__)


class PDFPageGenerator:
    def __init__(
        self,
        bucket_name: str,
        ocr_dir: str,
        alto_to_hocr_file: str = "alto_to_hocr.xsl",
    ):
        self.storage_manager = S3Manager()
        self.bucket_name = bucket_name
        self.ocr_dir = ocr_dir
        self.alto_to_hocr_file = alto_to_hocr_file

    def generate(
        self,
        image_file_location: str,
        ocr_file_location: str,
        outfile_location: str,
    ) -> None:
        # HocrTransform requires inputs as file paths that can be accessed via `os`
        # methods, so write the s3 objects to a temporary dir that will be discarded
        # after each page is generated
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_image = str(pathlib.Path(tmpdirname) / image_file_location)
            tmp_ocr = str(pathlib.Path(tmpdirname) / ocr_file_location)
            self._download_file_with_retries(
                self._s3_key(image_file_location), tmp_image, 3
            )
            self._download_file_with_retries(
                self._s3_key(ocr_file_location), tmp_ocr, 3
            )

            if self._is_alto_xml(ocr_path=tmp_ocr):
                self._convert_alto_to_hocr(ocr_path=tmp_ocr)
            else:
                mets_parser.reset_hocr_doctype(tmp_ocr)

            # Optimal OCR accuracy is 300 dots per inch (dpi)
            hocr = HocrTransform(hocr_filename=tmp_ocr, dpi=300)
            hocr.to_pdf(out_filename=outfile_location, image_filename=tmp_image)

    def _is_alto_xml(self, ocr_path: str) -> bool:
        try:
            ocr_file = etree.parse(ocr_path)
            root = ocr_file.getroot()

            return "http://www.loc.gov/standards/alto" in root.tag
        except Exception:
            return False

    def _convert_alto_to_hocr(self, ocr_path: str):
        alto_to_hocr_xsl = etree.parse(self.alto_to_hocr_file)
        alto_to_hocr_transform = etree.XSLT(alto_to_hocr_xsl)

        alto_doc = etree.parse(ocr_path)
        hocr_doc = alto_to_hocr_transform(alto_doc)

        hocr_doc.write(ocr_path)

    def _download_file_with_retries(
        self, key: str, out_location: str, retries: int
    ) -> None:
        backoff_in_seconds = 1

        for _ in range(0, retries):
            try:
                self.storage_manager.client.download_file(
                    self.bucket_name, key, out_location
                )

                if self._file_downloaded(key, out_location):
                    break
            except botocore.exceptions.ClientError as e:
                error_code = e.response.get("ResponseMetadata", {}).get(
                    "HTTPStatusCode"
                )

                # S3 will return 503 service unavailable when we are rate limited
                if error_code == 503:
                    time.sleep(backoff_in_seconds)
                    backoff_in_seconds *= 2

                    continue
                else:
                    raise

    def _s3_key(self, location: str) -> str:
        return str(pathlib.Path(self.ocr_dir) / location)

    def _file_downloaded(self, key: str, tmp_file: str):
        if not os.path.exists(tmp_file):
            logger.warning(f"Temp file {tmp_file} does not exist.")
            return False

        local_file_size = os.path.getsize(tmp_file)

        response = self.storage_manager.client.head_object(
            Bucket=self.bucket_name, Key=key
        )
        s3_object_size = response["ContentLength"]

        if local_file_size != s3_object_size:
            logger.warning(
                f"File size mismatch for {tmp_file}: \
                           local={local_file_size}, S3={s3_object_size}"
            )
            return False

        local_md5 = checksum.calculate_md5(tmp_file)
        # ETag in S3 is the MD5 checksum for non-multipart uploads
        s3_md5 = response["ETag"].strip('"')

        if local_md5 != s3_md5:
            logger.warning(
                f"MD5 checksum mismatch for {tmp_file}: \
                           local={local_md5}, S3={s3_md5}"
            )
            return False

        return True


class PDFPageSubprocess:
    """
    Manages a subprocess for generating PDF pages from a chunk of OCR data.
    WARNING: You may be tempted to use some sort of resource pool for managing
    these subprocesses - this has been tried:
      https://github.com/NYPL/pdf-pipeline/pull/63
    Lambda does not have the os support to managed the shared state between processes
    needed to do pooling:
      https://stackoverflow.com/questions/34005930/multiprocessing-semlock-is-not-implemented-when-running-on-aws-lambda
    """

    def __init__(self, page_generator: PDFPageGenerator):
        self.page_generator = page_generator
        self.pages: list[tuple[mets_parser.PDFPage, str]] = []
        self.process = mp.Process(target=self.execute)

    def add_page(self, page: mets_parser.Page, out_location: str) -> None:
        self.pages.append((page, out_location))

    def start(self) -> None:
        self.process.start()

    def join(self) -> None:
        self.process.join()

    def execute(self) -> None:
        try:
            for mets_page, out_location in self.pages:
                self.page_generator.generate(
                    mets_page.image_file.location,
                    mets_page.ocr_file.location,
                    out_location,
                )
        except Exception as e:
            logger.exception(f"Failed to generate PDF page to: {out_location}")
            os._exit(1)
