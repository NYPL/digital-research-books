import os
from elasticsearch7.exceptions import NotFoundError, ConflictError
from sqlalchemy.sql.functions import array_agg

from model import Work, Edition, ESWork
from managers import DBManager, ElasticsearchManager


def main():
    """Updating is_government_document field of current gov doc works in ES to be the boolean value True"""

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

    batchSize = 1000
    for work in (
        dbManager.session.query(Work.uuid, array_agg(Edition.measurements))
        .join(Edition)
        .filter(Edition.measurements != None)
        .filter(Edition.measurements != [])
        .filter(Edition.measurements != [{}])
        .group_by(Work.uuid)
        .yield_per(batchSize)
    ):
        uuid, editionMeasurementArray = work

        break_out_flag = False
        for editionMeasurements in editionMeasurementArray:
            for measurement in editionMeasurements:
                if measurement["type"] == "government_document":
                    if measurement["value"] == "1":
                        try:
                            workRec = ESWork.get(uuid, index=esManager.index)
                            workRec.is_government_document = True
                            workRec.save()
                            break_out_flag = True
                            break
                        except NotFoundError or ValueError or ConflictError:
                            if ValueError:
                                print("Empty value")
                            elif ConflictError:
                                print("Version number error")
                            else:
                                print("Work not indexed, skipping")
                            continue

            if break_out_flag:
                break

    dbManager.close_connection()


if __name__ == "__main__":
    main()
