import pytest
from processes.grin import conversion
from processes.grin.conversion import GRINConversion
import random 



@pytest.fixture()
def barcodes(grin_client):
    available_barcodes_scrape_fragment = "_all_books?&book_state=NEW&format=text"
    byte_response = grin_client.get(available_barcodes_scrape_fragment)
    lines = byte_response.decode("utf8").strip().split("\n")
    filtered_barcodes = [
        b.strip() for b in lines if b.strip().isdigit() and len(b.strip()) == 14
    ]

    return (
        random.sample(filtered_barcodes, 5)
        if len(filtered_barcodes) >= 5
        else filtered_barcodes
    )


@pytest.fixture()
def expected_barcodes_statuses():
    return [
        "Success",
        "Already being converted",
        "Not allowed to be downloaded",
        "Other error",
    ]

def test_convert_new_barcodes(barcodes, expected_barcodes_statuses, grin_client):
    conversion_process = GRINConversion(grin_client)
    converting_barcodes, converted_barcodes = conversion_process._convert_barcodes(barcodes)

    assert isinstance(converting_barcodes, list)
    assert isinstance(converted_barcodes, list)

    for barcode in converting_barcodes:
        assert barcode in barcodes
        assert any(status in expected_barcodes_statuses for status in ["Success", "Already being converted"])

    for barcode in converted_barcodes:
        assert barcode in barcodes
        assert "Already available for download" in expected_barcodes_statuses


    

