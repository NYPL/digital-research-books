import os
import requests
import json

from model import Record, Item
from managers import DBManager


def main():

    '''Creating Schomburg Collection'''

    dbManager = DBManager(
        user= os.environ.get('POSTGRES_USER', None),
        pswd= os.environ.get('POSTGRES_PSWD', None),
        host= os.environ.get('POSTGRES_HOST', None),
        port= os.environ.get('POSTGRES_PORT', None),
        db= os.environ.get('POSTGRES_NAME', None)
    )

    dbManager.generate_engine()

    dbManager.create_session()

    edition_ids_array = []

    for record in dbManager.session.query(Record) \
        .filter(Record.source == 'SCH Collection/Hathi files') \
        .filter(Record.cluster_status == True):
            for item in dbManager.session.query(Item) \
                .filter(Item.record_id == record.id):
                    edition_ids_array.append(item.edition_id)

    url = "http://127.0.0.1:5050/collections"
    json_body = {
        "title": "Schomburg Collection",
        "creator": "Dmitri",
        "description": "Collection of Schomburg Works",
        "editionIDs": edition_ids_array
    }
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, data=json.dumps(json_body), headers=headers) 
        response.raise_for_status()
    except requests.exceptions.RequestException:
        raise Exception

    dbManager.close_connection()

if __name__ == "__main__":
    main()