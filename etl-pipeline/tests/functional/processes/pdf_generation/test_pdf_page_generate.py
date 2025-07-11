import pathlib
import processes.pdf_generation.pdf_page as pdf_page
import os

MOCK_FILE_SIZE = 172725
MOCK_CHECKSUM = "bdf525092aaded56691ddc13e1832179"

def test_pdf_page_generate(mocker, mock_bucket, s3_manager):
    bucket = mock_bucket(os.environ["FILE_BUCKET"])
    hocr_transform = mocker.MagicMock()
    mocker.patch("processes.pdf_generation.mets_parser.reset_hocr_doctype")
    mocker.patch("processes.pdf_generation.pdf_page.HocrTransform", return_value=hocr_transform)
    mocker.patch("os.path.exists", return_value=True)
    mocker.patch("os.path.getsize", return_value=MOCK_FILE_SIZE)
    mocker.patch.object(
        bucket,
        "get_head",
        return_value={"ContentLength": MOCK_FILE_SIZE, "ETag": MOCK_CHECKSUM},
    )
    # mocker.patch("managers.s3", return_value=s3_manager)
    mocker.patch("processes.pdf_generation.checksum.calculate_md5", return_value=MOCK_CHECKSUM)

    generator = pdf_page.PDFPageGenerator(
       os.environ["FILE_BUCKET"], "unzipped_files/my-ocr"
    )
    generator.storage_manager = s3_manager
    s3_manager.client.put_object(
        Bucket=os.environ["FILE_BUCKET"],
        Key="unzipped_files/my-ocr/my_ocr.html",
        Body="",
        ContentType="application/json",
    )
    s3_manager.client.put_object(
        Bucket=os.environ["FILE_BUCKET"],
        Key="unzipped_files/my-ocr/my_image.tif",
        Body="",
        ContentType="application/json",
    )
    s3_manager.client.put_object(
        Bucket=os.environ["FILE_BUCKET"],
        Key="unzipped_files/my-ocr/my_output.pdf",
        Body="",
        ContentType="application/json",
    )
    generator.generate("my_image.tif", "my_ocr.html", "my_output.pdf")

    img_download_call, ocr_download_call = bucket.download_file.mock_calls
    img_key, img_tmp_file = img_download_call.args
    ocr_key, ocr_tmp_file = ocr_download_call.args

    assert img_key == "unzipped_files/my-ocr/my_image.tif"
    assert ocr_key == "unzipped_files/my-ocr/my_ocr.html"

    img_tmp_path = pathlib.Path(img_tmp_file)
    ocr_tmp_path = pathlib.Path(ocr_tmp_file)
    assert img_tmp_path.parent == ocr_tmp_path.parent

    hocr_transform.to_pdf.assert_called_once_with(
        out_filename="my_output.pdf",
        image_filename=str(img_tmp_path),
    )


def test_subprocess(mocker):
    page_generator = mocker.create_autospec(pdf_page.PDFPageGenerator)
    process = mocker.MagicMock()
    mocker.patch("multiprocessing.Process", return_value=process)
    subprocess = pdf_page.PDFPageSubprocess(page_generator)
    assert subprocess.pages == []
    assert subprocess.page_generator == page_generator
    assert subprocess.process == process

    page_1 = mocker.MagicMock()
    page_2 = mocker.MagicMock()
    subprocess.add_page(page_1, mocker.sentinel.page_1_location)
    subprocess.add_page(page_2, mocker.sentinel.page_2_location)
    assert subprocess.pages == [
        (page_1, mocker.sentinel.page_1_location),
        (page_2, mocker.sentinel.page_2_location),
    ]

    subprocess.start()
    process.start.assert_called_once()
    subprocess.join()
    process.join.assert_called_once()

    subprocess.execute()
    assert page_generator.generate.call_count == 2
    page_generator.generate.assert_has_calls(
        [
            mocker.call(
                page_1.image_file.location,
                page_1.ocr_file.location,
                mocker.sentinel.page_1_location,
            ),
            mocker.call(
                page_2.image_file.location,
                page_2.ocr_file.location,
                mocker.sentinel.page_2_location,
            ),
        ],
    )
