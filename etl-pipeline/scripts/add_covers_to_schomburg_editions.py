import argparse
import os

import boto3

from digital_assets.utils.get_stored_file_url import get_stored_file_url
from load_env import load_env_file
from model import Collection, Edition, Link
from managers import DBManager


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="")
    parser.add_argument("--env", default="qa")
    args = parser.parse_args()
    env = args.env
    load_env_file(f"local-{env}", file_string="config/{}.yaml")
    db_manager = DBManager(
        user= os.environ["POSTGRES_USER"],
        pswd= os.environ["POSTGRES_PSWD"],
        host= os.environ["POSTGRES_HOST"],
        port= os.environ["POSTGRES_PORT"],
        db= os.environ["POSTGRES_NAME"],
    )
    bucket = os.environ["FILE_BUCKET"]
    s3 = boto3.Session(profile_name=args.profile).client("s3")
    db_manager.create_session()
    collection = (
        db_manager.session.query(Collection).filter(
            Collection.title == "Schomburg Collection",
        ).one()
    )
    for edition in collection.editions:
        links_by_url = { link.url: link for link in edition.links }
        for identifier in edition.identifiers:
            hathi_id = identifier.identifier
            cover_key = f"covers/publisher_backlist/hathi_{hathi_id}.png"
            cover_url = get_stored_file_url(bucket, cover_key)
            if identifier.authority != "hathi":
                continue
            if cover_url in links_by_url:
                continue
            if not cover_exists(s3, bucket, cover_key):
                print(f"Cover not found for hathi id {hathi_id}")
                continue

            link = Link(url=cover_url, media_type="image/png", flags={"cover": True})
            edition.links.append(link)

        db_manager.session.commit()

def cover_exists(s3, bucket, cover_key) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=cover_key)
    except Exception as e:
        print(cover_key)
        print(e)
        return False

    return True


if __name__ == "__main__":
    main()
