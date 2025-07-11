import io
from unittest.mock import patch
import pypdf
import pytest
from processes.pdf_generation.pdf_page import PDFPageGenerator
from botocore.exceptions import ClientError

OCR_DIR = "0000011245674"
ALTO_OCR_DATA_INDEX = "00000004"
ALTO_OCR_FILE_TYPES = ["jp2", "txt", "xml"]


def test_generate_pdf_page(s3_manager, tmp_path):
    output_dir = tmp_path / "pdf_generation"
    output_dir.mkdir()
    output_file_location = output_dir / f"{ALTO_OCR_DATA_INDEX}.pdf"

    for file_type in ALTO_OCR_FILE_TYPES:
        file_name = f"tests/fixtures/pdf_generation/{ALTO_OCR_DATA_INDEX}.{file_type}"
        with open(file=file_name, mode="rb") as f:
            s3_manager.client.put_object(
                Key=f"{OCR_DIR}/{ALTO_OCR_DATA_INDEX}.{file_type}",
                Bucket="drb-files-limited-local",
                Body=f,
            )

    pdf_page_generator = PDFPageGenerator(
        bucket_name="drb-files-limited-local",
        ocr_dir=OCR_DIR,
        alto_to_hocr_file="processes/util/alto_to_hocr.xsl",
    )

    pdf_page_generator.generate(
        image_file_location=f"{ALTO_OCR_DATA_INDEX}.jp2",
        ocr_file_location=f"{ALTO_OCR_DATA_INDEX}.xml",
        outfile_location=str(output_file_location),
    )

    assert output_file_location.exists()
    assert output_file_location.stat().st_size > 0

    with open(output_file_location, "rb") as f:
        reader = pypdf.PdfReader(f)
        assert len(reader.pages) == 1
        page = reader.pages[0]
        extracted_text = page.extract_text()

        expected_text_from_alto = "The Church For Americans"
        assert expected_text_from_alto in extracted_text


def test_generate_pdf_page_missing_image(s3_manager, tmp_path):
    output_dir = tmp_path / "pdf_generation"
    output_dir.mkdir()
    output_file_location = output_dir / f"{ALTO_OCR_DATA_INDEX}.pdf"

    def mock_get_object_failure(Key, Bucket):
        if Key == f"{OCR_DIR}/{ALTO_OCR_DATA_INDEX}.jp2":
            raise s3_manager.client.exceptions.NoSuchKey("No such key")
        elif Key == f"{OCR_DIR}/{ALTO_OCR_DATA_INDEX}.xml":
            return {"Body": io.BytesIO(b"<alto></alto>")}
        raise ValueError(f"Unexpected S3 key: {Key}")

    with patch.object(
        s3_manager.client, "get_object", side_effect=mock_get_object_failure
    ):
        pdf_page_generator = PDFPageGenerator(
            bucket_name="drb-files-limited-local",
            ocr_dir=OCR_DIR,
            alto_to_hocr_file="processes/util/alto_to_hocr.xsl",
        )
        with pytest.raises(ClientError) as exception:
            pdf_page_generator.generate(
                image_file_location=f"{OCR_DIR}/{ALTO_OCR_DATA_INDEX}.jp2",
                ocr_file_location=f"{OCR_DIR}/{ALTO_OCR_DATA_INDEX}.xml",
                outfile_location=str(output_file_location),
            )
        assert (
            "An error occurred (404) when calling the HeadObject operation: Not Found"
            == str(exception.value)
        )
        assert not output_file_location.exists()
