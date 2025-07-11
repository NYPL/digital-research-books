import pytest

import processes.pdf_generation.marc as marc
import processes.pdf_generation.mets_parser as mets_parser


TEST_METADATA_FILES = [
    ("tests/fixtures/pdf_generation/metadata_files/UCAL_C3263821.xml"),
    ("tests/fixtures/pdf_generation/metadata_files/NYPL_33333211223819.xml"),
]


@pytest.mark.parametrize("metadata_filename", TEST_METADATA_FILES)
def test_get_oclc_number(metadata_filename):
    with open(f"{metadata_filename}", "r") as metadata_filedata:
        metadata_file = mets_parser.MetadataFile.from_mets_str(
            metadata_filedata.read().encode("utf-8")
        )

    metadata = metadata_file.get_metadata()
    marc_record = marc.Record.from_node(metadata.xml_data)

    assert marc_record.oclc_number is not None
