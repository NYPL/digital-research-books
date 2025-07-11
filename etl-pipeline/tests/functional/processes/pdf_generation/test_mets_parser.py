import pytest

import processes.pdf_generation.mets_parser as mets_parser


TEST_METADATA_FILES_SOURCE_IDENTIFIERS = [
    (
        "tests/fixtures/pdf_generation/metadata_files/UCAL_C3263821.xml",
        mets_parser.MetadataFileSourceIdentifier("Google", "barcode", "UCAL_C3263821"),
    ),
    (
        "tests/fixtures/pdf_generation/metadata_files/NYPL_33333211223819.xml",
        mets_parser.MetadataFileSourceIdentifier(
            "Google", "barcode", "NYPL_33333211223819"
        ),
    ),
]

TEST_METS_FILES = [
    ("tests/fixtures/pdf_generation/metadata_files/c3263821.mets.xml"),
    ("tests/fixtures/pdf_generation/metadata_files/NYPL_33333211223819.xml"),
]


@pytest.mark.parametrize(
    "metadata_filename, expected_source_identifier",
    TEST_METADATA_FILES_SOURCE_IDENTIFIERS,
)
def test_get_page_labels(metadata_filename, expected_source_identifier):
    with open(f"{metadata_filename}", "r") as metadata_filedata:
        metadata_file = mets_parser.MetadataFile.from_mets_str(
            metadata_filedata.read().encode("utf-8")
        )
        source_identifier = metadata_file.get_source_identifier()

        assert source_identifier == expected_source_identifier


@pytest.mark.parametrize("mets_filename", TEST_METS_FILES)
def test_get_chapter_labels(mets_filename):
    with open(f"{mets_filename}", "r") as mets_filedata:
        mets_file = mets_parser.METSFile.from_mets_str(
            mets_filedata.read().encode("utf-8")
        )

        has_chapter_label = False

        for page in mets_file.iter_pages():
            if page.is_chapter_start:
                has_chapter_label = True

        assert has_chapter_label, "Unable to get chapter labels"


def test_get_table_of_contents_label():
    mets_filename = (
        "tests/fixtures/pdf_generation/metadata_files/NYPL_33333211223819.xml"
    )

    with open(mets_filename, "r") as mets_filedata:
        mets_file = mets_parser.METSFile.from_mets_str(
            mets_filedata.read().encode("utf-8")
        )

        has_table_of_contents_label = False

        for page in mets_file.iter_pages():
            if page.is_toc:
                has_table_of_contents_label = True

        assert has_table_of_contents_label, "Unable to get table of contents label"
