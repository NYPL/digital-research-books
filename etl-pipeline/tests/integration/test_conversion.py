import pytest
from processes.grin import conversion
from processes.grin.conversion import GRINConversion
import random 


def test_convert_new_barcodes(barcodes, grin_client):
     
     for barcode in barcodes:
        if not barcode:
            pytest.skip("No barcodes provided for testing.")
        else:
            conversion_process = GRINConversion(grin_client)    
            converting_barcodes, converted_barcodes = conversion_process._convert_barcodes(barcodes)

            assert isinstance(converting_barcodes, list)
            assert isinstance(converted_barcodes, list)
            assert barcode in barcodes
            assert set(converting_barcodes).issubset(set(barcodes)), "Converting barcodes should be a subset of provided barcodes"
            assert set(converted_barcodes).issubset(set(barcodes)), "Converted barcodes should be a subset of provided barcodes"
            assert not set(converting_barcodes).intersection(set(converted_barcodes)), "Converting and converted barcodes should not overlap"
            assert len(converting_barcodes) + len(converted_barcodes) <= len(barcodes), "Total converting and converted barcodes should not exceed provided barcodes"





    

