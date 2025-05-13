import numpy as np
from sentence_transformers import SentenceTransformer

from managers import DBManager
from model import Record


def main():
    model = SentenceTransformer("all-mpnet-base-v2")

    db_manager = DBManager()
    db_manager.create_session()

    records = db_manager.session.query(Record).all()

    for record in records:
        author_names = (
            [author.split("|")[0] for author in record.authors]
            if record.authors
            else ""
        )
        publishers = (
            [publisher.split("|")[0] for publisher in record.publisher]
            if record.publisher
            else ""
        )
        title_authors_publishers = (
            f"{record.title}; {', '.join(author_names)}; {', '.join(publishers)}"
        )

        record.embedding = model.encode(title_authors_publishers)

        db_manager.session.add(record)

        db_manager.session.commit()


if __name__ == "__main__":
    main()
