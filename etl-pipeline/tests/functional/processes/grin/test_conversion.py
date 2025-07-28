import pytest
from processes.grin import conversion
from processes.grin.conversion import GRINConversion
from model import GRINState, GRINStatus, Record, RecordState, FRBRStatus, Source


def test_convert_new_barcodes(barcodes, grin_client, db_manager):
    for barcode in barcodes:
        if not barcode:
            pytest.skip("No barcodes provided for testing.")
        else:
            conversion_process = GRINConversion(grin_client)    
            conversion_process.runProcess()

            
            assert db_manager.session.query(
                GRINStatus
            ).filter(GRINStatus.barcode.in_(barcodes)).count() > 0
            assert all(
                status.state == GRINState.CONVERTING.value
                for status in db_manager.session.query(GRINStatus)
                .filter(GRINStatus.barcode.in_(barcodes))
                .all()
            )
            assert db_manager.session.query(
                GRINStatus
            ).filter(GRINStatus.state == GRINState.PENDING_CONVERSION.value).count() > 0   

            assert  db_manager.session.query(
                GRINStatus
            ).filter(GRINStatus.state == GRINState.CONVERTING.value).count() > 0 
               
        


    

