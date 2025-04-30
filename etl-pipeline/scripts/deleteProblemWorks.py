import os

from model import Work
from managers import DBManager, ElasticsearchManager
from elasticsearch_dsl import Search


def main():
    dbManager = DBManager(
        user=os.environ.get("POSTGRES_USER", None),
        pswd=os.environ.get("POSTGRES_PSWD", None),
        host=os.environ.get("POSTGRES_HOST", None),
        port=os.environ.get("POSTGRES_PORT", None),
        db=os.environ.get("POSTGRES_NAME", None),
    )

    esManager = ElasticsearchManager()
    esManager.create_elastic_connection()

    dbManager.create_session()

    uuid = "0288f50c-f2db-47fc-9ff9-b8b95b4cc712"

    dbManager.session.query(Work).filter(Work.uuid == uuid).delete()

    resp = Search(index=os.environ["ELASTICSEARCH_INDEX"]).query("match", uuid=uuid)
    resp.delete()

    dbManager.session.commit()
    dbManager.session.close()
