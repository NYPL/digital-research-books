from digital_assets import get_stored_file_url
from processes import RecordClusterer, LinkFulfiller
from model import FileFlags, Record, Part, Item
import json
import os
from sqlalchemy.orm import joinedload


def test_fulfill_links(db_manager, redis_manager, s3_manager, limited_access_record_uuid):
    record_clustered = RecordClusterer(db_manager=db_manager, redis_manager=redis_manager)

    record = db_manager.session.query(Record).filter_by(uuid=limited_access_record_uuid).first()
    
    record_clustered.cluster_record(record)

    item = (
        db_manager.session.query(Item)
            .options(joinedload(Item.links))
            .filter_by(record_id=record.id)
            .first()
    )

    assert item is not None, 'Item not created by cluster process'
    
    epub_link = next((link for link in item.links if link.media_type == 'application/epub+zip'), None)
    assert epub_link is not None, 'EPUB link not found in item links'

    test_manifest = {
        'links': [{'type': epub_link.media_type, 'href': epub_link.url}],
        'readingOrder': [{'type': epub_link.media_type, 'href': epub_link.url}],
        'resources': [{'type': epub_link.media_type, 'href': epub_link.url}],
        'toc': [{'href': epub_link.url}]
    }
    
    manifest_key = f'manifests/publisher_backlist/test_{limited_access_record_uuid}.json'
    s3_manager.client.put_object(
        Bucket=os.environ['FILE_BUCKET'],
        Key=manifest_key,
        Body=json.dumps(test_manifest),
        ContentType='application/json'
    )

    record.has_part.insert(0, str(Part(
        index=1,
        url=get_stored_file_url(storage_name=os.environ['FILE_BUCKET'], file_path=manifest_key),
        source=record.source,
        file_type='application/webpub+json',
        flags=str(FileFlags(reader=True, nypl_login=True, fulfill_limited_access=True))
    )))

    link_fulfiller = LinkFulfiller(db_manager)
    link_fulfiller.fulfill_records_links([record])

    manifest_file = s3_manager.client.get_object(Bucket=os.environ['FILE_BUCKET'], Key=manifest_key)
    fulfilled_manifest_json = json.loads(manifest_file['Body'].read().decode())

    expected_url = f"{os.environ['DRB_API_URL']}/fulfill/{epub_link.id}"

    for manifest_section in ['links', 'readingOrder', 'resources', 'toc']:
        for manifest_link in fulfilled_manifest_json.get(manifest_section, []):
            assert manifest_link.get('href') == expected_url

    s3_manager.client.delete_object(
        Bucket=os.environ['FILE_BUCKET'],
        Key=manifest_key
    )
