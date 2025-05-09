import logging
import os
from google.auth.transport.requests import (
    AuthorizedSession,
)
from model import GRINState, GRINStatus, Record, FRBRStatus
from managers import DBManager
from google.oauth2.service_account import Credentials
from services.ssm_service import SSMService
from logger import create_log

# TODO: Update this once the GRINClient class is ready
from scripts.grin_mirror import GRINClient

logger = create_log(__name__)


class GRINConversion:
    def __init__(self):
        self.db_manager = DBManager(
            user=os.environ.get("POSTGRES_USER", None),
            pswd=os.environ.get("POSTGRES_PSWD", None),
            host=os.environ.get("POSTGRES_HOST", None),
            port=os.environ.get("POSTGRES_PORT", None),
            db=os.environ.get("POSTGRES_NAME", None),
        )
        self.db_manager.create_session()

        self.client = GRINClient()

    def acquire_and_convert(self):
        # acquire the new books within the month
        pass

    def convert_backfills(self):
        #  initialize conversion the new books, and update the DB
        pass

    def process_converted(self):
        converted_barcodes = self.client.converted()

        if len(converted_barcodes) > 0:
            for barcode in converted_barcodes:
                try:
                    self.db_manager.session.query(GRINStatus).filter(
                        GRINStatus.barcode == f"{barcode}",
                        GRINStatus.state != GRINState.DOWNLOADED.value,
                        GRINStatus.state != GRINState.CONVERTED.value,
                    ).update({"state": GRINState.CONVERTED.value})
                    self.db_manager.commit_changes()
                except:
                    self.db_manager.session.rollback()
                    raise


if __name__ == "__main__":
    # Run the process
    grin_conversion = GRINConversion()
    grin_conversion.process_converted()
