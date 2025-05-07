import os

from model import Link, Item
from model.postgres.item import ITEM_LINKS
from managers import DBManager


def main():
    """Updating Link flags with a new nypl_login flag"""

    dbManager = DBManager(
        user=os.environ.get("POSTGRES_USER", None),
        pswd=os.environ.get("POSTGRES_PSWD", None),
        host=os.environ.get("POSTGRES_HOST", None),
        port=os.environ.get("POSTGRES_PORT", None),
        db=os.environ.get("POSTGRES_NAME", None),
    )

    dbManager.create_session()

    for item in dbManager.session.query(Item).filter(Item.source == "UofM"):
        for link in (
            dbManager.session.query(Link)
            .join(ITEM_LINKS)
            .filter(ITEM_LINKS.c.item_id == item.id)
            .filter(Link.media_type == "application/pdf")
            .all()
        ):
            if link.flags:
                # The link.flags doesn't update if the dict method isn't called on it
                newLinkFlag = dict(link.flags)
                newLinkFlag["nypl_login"] = True
                link.flags = newLinkFlag

    dbManager.commit_changes()


if __name__ == "__main__":
    main()
