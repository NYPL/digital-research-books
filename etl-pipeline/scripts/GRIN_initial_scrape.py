from datetime import datetime
import logging
from typing import List, Iterator
from google.auth.transport.requests import (
    AuthorizedSession,
)
from model import GRINState, GRINStatus, Record, FRBRStatus
from managers import DBManager
from google.oauth2.service_account import Credentials
import json
from services.ssm_service import SSMService
from uuid import uuid4
import os


DEFAULT_BASE_URL = "https://books.google.com/libraries/NYPL"
DEFAULT_CHUNK_SIZE = 5000

logging.basicConfig(
    filename="GRIN_initial_scrape.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class AuthSession(object):
    """Eventually, this will a service, we will use this mock code for now"""

    def __init__(self):
        self.creds = self.load_creds()
        self.session = AuthorizedSession(self.creds)

    def get_session(self):
        return self.session

    def load_creds(self):
        ssm_service = SSMService()
        service_account_file = ssm_service.get_parameter("grin-auth")
        service_account_info = json.loads(service_account_file)

        scopes = [
            "openid",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
        ]

        creds = Credentials.from_service_account_info(
            service_account_info, scopes=scopes
        )
        return creds


def main():
    auth = AuthSession()
    authed_session = auth.get_session()

    db_manager = DBManager()
    db_manager.create_session()

    url = f"{DEFAULT_BASE_URL}/_all_books?book_state=NEW&book_state=PREVIOUSLY_DOWNLOADED&format=text"
    logging.info(f"Scraping url: {url}")

    response = authed_session.get(url, timeout=600)
    response.raise_for_status()

    barcodes = response.content.decode("utf8").strip().split("\n")

    if len(barcodes) > 0:
        insert_into_db(
            barcodes=barcodes, db_manager=db_manager, chunk_size=DEFAULT_CHUNK_SIZE
        )
    else:
        logging.info("No record found")


def insert_into_db(barcodes: List[str], db_manager: DBManager, chunk_size: int):
    logging.info(f"Processing {len(barcodes)} barcodes")

    for chunked_barcodes in chunk(iter(barcodes), chunk_size):
        new_records: List[Record] = []
        for barcode in chunked_barcodes:
            new_records.append(
                Record(
                    uuid=uuid4(),
                    frbr_status=FRBRStatus.TODO.value,
                    source_id=f"{barcode}|grin",
                    grin_status=GRINStatus(
                        barcode=barcode,
                        failed_download=0,
                        state=GRINState.PENDING_CONVERSION.value,
                        date_created=datetime(1991, 8, 25),
                    ),
                )
            )

        logging.info(f"Inserting {len(chunked_barcodes)} barcodes into Record")

        try:
            db_manager.session.add_all(new_records)
            db_manager.commit_changes()
        except Exception:
            logging.exception("Failed to insert barcodes")
            logging.exception(chunked_barcodes)
            raise

    logging.info("Complete.")


def chunk(xs: Iterator, size: int) -> Iterator[list]:
    while True:
        chunk = []
        try:
            for _ in range(size):
                chunk.append(next(xs))
            yield chunk
        except StopIteration:
            if chunk:
                yield chunk

            break


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(e, exc_info=True)
