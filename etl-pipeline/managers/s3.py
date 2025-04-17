import base64
import boto3
from botocore.exceptions import ClientError
import hashlib
from io import BytesIO
import mimetypes
import os
from zipfile import ZipFile
from managers import WebpubManifest
from digital_assets import get_stored_file_url
from model import Record, Part, FileFlags
from managers import DOABLinkManager
from model import Source

from logger import create_log

logger = create_log(__name__)


class S3Manager:

    def __init__(self):
        self.client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS', None),
            aws_secret_access_key=os.environ.get('AWS_SECRET', None),
            region_name=os.environ.get('AWS_REGION', None),
            endpoint_url=os.environ.get('S3_ENDPOINT_URL', None)
        )

    def store_pdf_manifest(self, record: Record, bucket_name, flags=FileFlags(reader=True), path: str=None):
        
        if record.source == 'doab':
            link_manager = DOABLinkManager(record)

            link_manager.parse_links()

            print(f'Length of manifests: {len(link_manager.manifests)}')
            
            for manifest in link_manager.manifests:
                manifest_path, manifest_json = manifest
                self.create_manifest_in_s3(manifest_path=manifest_path, manifest_json=manifest_json, bucket=bucket_name)

                print(f'manifest_path={manifest_path}')
                record.has_part.insert(0, str(Part(
                    index=1,
                    url=manifest_path,
                    source=record.source,
                    file_type='application/webpub+json',
                    flags=str(flags)
                )))

            for epub_link in link_manager.epub_links:
                epub_path, epub_uri = epub_link
                record.has_part.append(str(Part(
                    index=1,
                    url=epub_path,
                    source=Source.LOC.value,
                    file_type='application/epub+zip',
                    flags=str(FileFlags(download=True)),
                    source_url=epub_uri
                )))
        else:
            record_id = record.source_id.split('|')[0]
            pdf_part = next(filter(lambda part: part.file_type == 'application/pdf', record.parts), None)

            if pdf_part is not None:
                if path:
                    manifest_path = f'manifests/{path}/{record.source}/{record_id}.json'
                else:
                    manifest_path = f'manifests/{record.source}/{record_id}.json'
                    
                manifest_url = get_stored_file_url(storage_name=bucket_name, file_path=manifest_path)
                manifest_json = self.generate_manifest(record=record, source_url=pdf_part.url, manifest_url=manifest_url)

                self.create_manifest_in_s3(manifest_path=manifest_path, manifest_json=manifest_json, bucket=bucket_name)

                record.has_part.insert(0, str(Part(
                    index=pdf_part.index,
                    url=manifest_url,
                    source=record.source,
                    file_type='application/webpub+json',
                    flags=str(flags)
                )))

    def create_manifest_in_s3(self, manifest_path: str, manifest_json, bucket: str):
        self.put_object(manifest_json.encode('utf-8'), manifest_path, bucket)

    def generate_manifest(self, record: Record, source_url: str, manifest_url: str):
        manifest = WebpubManifest(source_url, 'application/pdf')

        manifest.addMetadata(record)
        manifest.addChapter(source_url, record.title)
        manifest.links.append({
            'rel': 'self',
            'href': manifest_url,
            'type': 'application/webpub+json'
        })

        return manifest.toJson()

    def put_object(self, object, key: str, bucket: str, bucket_permissions: str='public-read'):
        object_md5 = S3Manager.get_md5_hash(object)
        object_extension = key[-4:].lower()
        get_object_response = None

        try:
            if object_extension == 'epub':
                get_object_response = self.get_object(key, bucket, md5_hash=object_md5)
        except S3Error:
            logger.info(f'{key} does not yet exist')

        if get_object_response and (get_object_response['ResponseMetadata']['HTTPStatusCode'] == 304 or get_object_response['Metadata'].get('md5checksum', None) == object_md5):
            logger.info(f'Skipping save of unmodified file: {key}')
            return None

        try:
            if object_extension == 'epub':
                self.store_epub(object, key, bucket)

            object_type = mimetypes.guess_type(key)[0] or 'binary/octet-stream'

            return self.client.put_object(
                Body=object,
                Bucket=bucket,
                Key=key,
                ContentMD5=object_md5,
                ContentType=object_type,
                Metadata={ 'md5Checksum': object_md5 },
                **({ 'ACL': bucket_permissions } if bucket_permissions is not None else {})
            )
        except ClientError as e:
            raise S3Error(f'Unable to store file {key} in s3: {e}')

    def store_epub(self, object, key: str, bucket: str):
        key_prefix = '.'.join(key.split('.')[:-1])

        with ZipFile(BytesIO(object), 'r') as epub_zip:
            for component in epub_zip.namelist():
                self.put_object(object=epub_zip.open(component).read(), key=f'{key_prefix}/{component}', bucket=bucket)

    def get_object(self, key: str, bucket: str, md5_hash=None):
        try:
            if md5_hash:
                return self.client.get_object(
                    Bucket=bucket,
                    Key=key,
                    IfNoneMatch=md5_hash
                )
            
            return self.client.get_object(Bucket=bucket, Key=key)
        except ClientError:
            raise S3Error('Unable to get object from s3')

    @staticmethod
    def get_md5_hash(object):
        m = hashlib.md5()
        m.update(object)
        
        return base64.b64encode(m.digest()).decode('utf-8')


class S3Error(Exception):
    def __init__(self, message=None):
        self.message = message
