"""
Delete "work"'s in works ES index that are missing from "works" DB tabke
"""

import os

from model import Work
from managers import DBManager, ElasticsearchManager
from elasticsearch_dsl import Search


def main():
    es_manager = ElasticsearchManager()
    es_manager.create_elastic_connection()

    index = os.environ["ELASTICSEARCH_INDEX"]
    batch_size = 1000

    with DBManager() as db_manager:
        db_work_uuids = {
            str(work[0])
            for work in db_manager.session.query(Work.uuid).yield_per(10000)
        }

    to_delete = []
    deleted_count = 0

    for i, work in enumerate(
        Search(index=index).query("match_all").params(size=10000).scan(), 1
    ):
        if work.uuid not in db_work_uuids:
            to_delete.append(work.uuid)

            if len(to_delete) >= batch_size:
                deleted_count += len(to_delete)
                es_manager.delete_work_records(to_delete)
                to_delete = []

        print(f"Synced {i:,} work documents, deleted {deleted_count}", end="\r")

    if to_delete:
        deleted_count += len(to_delete)
        es_manager.delete_work_records(to_delete)

    print(f"Synced {i:,} work documents, deleted {deleted_count}")


if __name__ == "__main__":
    main()
