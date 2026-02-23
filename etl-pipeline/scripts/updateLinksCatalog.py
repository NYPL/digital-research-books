import os
import re

from model import Link
from managers import DBManager


def main():
    """Updating NYPL Link media type with new link to replace text/html link"""

    dbManager = DBManager(
        user=os.environ.get("POSTGRES_USER", None),
        pswd=os.environ.get("POSTGRES_PSWD", None),
        host=os.environ.get("POSTGRES_HOST", None),
        port=os.environ.get("POSTGRES_PORT", None),
        db=os.environ.get("POSTGRES_NAME", None),
    )

    dbManager.create_session()

    for link in (
        dbManager.session.query(Link)
        .filter(Link.media_type == "text/html")
        .filter(Link.flags != {})
        .filter(Link.flags["catalog"] == "true")
        .all()
    ):
        link.media_type = "application/html+catalog"

    dbManager.commit_changes()


if __name__ == "__main__":
    main()
