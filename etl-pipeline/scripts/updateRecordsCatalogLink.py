import os
import re

from model import Record
from managers import DBManager


def main():
    """Updating NYPL catalog records with new link to replace text/html link"""

    dbManager = DBManager(
        user=os.environ.get("POSTGRES_USER", None),
        pswd=os.environ.get("POSTGRES_PSWD", None),
        host=os.environ.get("POSTGRES_HOST", None),
        port=os.environ.get("POSTGRES_PORT", None),
        db=os.environ.get("POSTGRES_NAME", None),
    )

    dbManager.create_session()

    catalogRegex = r"catalog\": true"
    linkRegex = r"text\/html"

    for record in dbManager.session.query(Record).filter(Record.source == "nypl").all():
        recordArray = []

        for i, elem in enumerate(record.has_part):
            if re.search(linkRegex, elem) != None:
                if re.search(catalogRegex, elem) != None:
                    recordArray.append(
                        record.has_part[i].replace(
                            "text/html", "application/html+catalog"
                        )
                    )
            else:
                recordArray.append(record.has_part[i])

        record.has_part = recordArray

    dbManager.commit_changes()


if __name__ == "__main__":
    main()
