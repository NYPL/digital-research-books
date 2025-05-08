import os
import re

from model import Record, Edition, Link
from managers import DBManager


def main():
    """Updating the publisher location and links flags for ISAC records"""

    dbManager = DBManager(
        user=os.environ.get("POSTGRES_USER", None),
        pswd=os.environ.get("POSTGRES_PSWD", None),
        host=os.environ.get("POSTGRES_HOST", None),
        port=os.environ.get("POSTGRES_PORT", None),
        db=os.environ.get("POSTGRES_NAME", None),
    )

    dbManager.create_session()

    for record in dbManager.session.query(Record).filter(Record.source == "isac").all():
        # Removing |publisherLocation from spatial attribute in Record Table
        charPattern = re.compile(r"\|publisherLocation")
        record.spatial = re.sub(charPattern, "", record.spatial)

        # Reversing download and reader values of webpub mainfest in has_part attribute
        if not record.has_part:
            continue

        hasPartManifest = record.has_part[0].split("|")

        if (
            hasPartManifest[len(hasPartManifest) - 2] == "application/webpub+json"
            and hasPartManifest[-1]
            == '{"catalog": false, "download": true, "reader": false, "embed": false}'
        ):
            hasPartManifest[-1] = (
                '{"catalog": false, "download": false, "reader": true, "embed": false}'
            )
            newHasPart = []
            newHasPart.append("|".join(hasPartManifest))
            for i in record.has_part[1:]:
                newHasPart.append(i)
            record.has_part = newHasPart

    # Removing |publisherLocation from publication_place attribute in Edition Table
    # This makes this change show up on the frontend unlike changing the record spatial attribute
    for edition in dbManager.session.query(Edition).filter(
        Edition.publication_place == "Chicago|publisherLocation"
    ):
        charPattern = re.compile(r"\|publisherLocation")
        edition.publication_place = re.sub(charPattern, "", edition.publication_place)

    # Reversing download and reader values of webpub mainfest links in Link Table
    # This change wil show up on the frontend unlike changing the record has_part attribute
    for link in (
        dbManager.session.query(Link)
        .filter(Link.media_type == "application/webpub+json")
        .filter(
            Link.flags
            == '{"embed": false,"reader": false,"catalog": false,"download": true}'
        )
        .all()
    ):
        link.flags = {
            "embed": False,
            "reader": True,
            "catalog": False,
            "download": False,
        }

    dbManager.commit_changes()


if __name__ == "__main__":
    main()
