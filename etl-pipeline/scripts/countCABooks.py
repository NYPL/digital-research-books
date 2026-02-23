import os
import json
import uuid

from model import Record, Work, Edition, Identifier
from model.postgres.edition import EDITION_IDENTIFIERS

from managers import DBManager


def main():
    """Creating a list of public domain Cali books from Hathitrust that exists in the database"""

    dbManager = DBManager(
        user=os.environ.get("POSTGRES_USER", None),
        pswd=os.environ.get("POSTGRES_PSWD", None),
        host=os.environ.get("POSTGRES_HOST", None),
        port=os.environ.get("POSTGRES_PORT", None),
        db=os.environ.get("POSTGRES_NAME", None),
    )

    dbManager.create_session()

    editionCount = 0
    workCount = 0
    UCBooksDRB = []
    editionIDS = []
    editionWorkID = None

    with open("UCBooks.json") as f:
        UCBooks = json.load(f)

    for sourceID in UCBooks:
        for record in (
            dbManager.session.query(Record)
            .filter(Record.source_id == f"{sourceID}|hathi")
            .all()
        ):
            if record:
                for edition in (
                    dbManager.session.query(Edition)
                    .join(EDITION_IDENTIFIERS)
                    .join(Identifier)
                    .filter(Identifier.identifier == sourceID)
                    .filter(Identifier.authority == "hathi")
                    .all()
                ):
                    editionIDS.append(edition.id)
                    editionCount += 1
                    editionWorkID = edition.work_id

                for work in (
                    dbManager.session.query(Work).filter(Work.id == editionWorkID).all()
                ):
                    UCBooksDRB.append(
                        {
                            "record": record.source_id,
                            "edition": editionIDS,
                            "work": work.uuid,
                        }
                    )
                    workCount += 1
                editionIDS = []

    print(UCBooksDRB)
    print(editionCount, workCount)
    print(editionCount + workCount)

    dbManager.close_connection()


if __name__ == "__main__":
    main()
