import os
from typing import Optional
from uuid import uuid4

from model import FileFlags, FRBRStatus, Part, Record, Source
from .rights import get_rights_string
from digital_assets import get_stored_file_url

# LOC response model: https://www.loc.gov/apis/json-and-yaml/responses/item-and-resource/    
def map_loc_record(source_record: dict) -> Optional[Record]:
    if not source_record.get('resources'):
        return None
    
    first_file_resource = source_record.get('resources')[0]

    if 'pdf' not in first_file_resource.keys() and not 'epub_file' in first_file_resource.keys():
        return None

    item_details = source_record.get('item', {})
    lccn = source_record.get('number_lccn')[0]
    source_id = f'{lccn}|lccn'

    return Record(
        uuid=uuid4(),
        frbr_status=FRBRStatus.TODO.value,
        cluster_status=False,
        source=Source.LOC.value,
        title=source_record.get('title'),
        alternative=source_record.get('other_title'),
        source_id=source_id,
        medium=_get_medium(source_record),
        authors=[f'{author}|||true' for author in source_record.get('contributor', [])] if source_record.get('contributor') else None,
        dates=[f"{source_record.get('date')}|publication_date"] if source_record.get('date') else None,
        publisher=_parse_publishers(item_details.get('created_published')),
        identifiers=[id for id in [
            f'{lccn}|lccn',
            f"{item_details.get('call_number')[0]}|call_number" if item_details.get('call_number') else None
        ] if id is not None],
        contributors=[f'{contributor}|||contributor' for contributor in source_record.get('contributor', [])] if source_record.get('contributor') else None,
        extent=item_details.get('medium')[0] if item_details.get('medium') else None,
        subjects=[f'{subject}||' for subject in item_details.get('subjects')] if item_details.get('subjects') else None,
        languages=[f'||{language}' for language in item_details.get('language')] if item_details.get('language') else None,
        rights=get_rights_string(rights_source='loc', license=item_details.get('rights_advisory')[0]) if item_details.get('rights_advisory') else None,
        abstract=source_record.get('description')[0] if source_record.get('description') else None,
        is_part_of=[f'{part_of}|collection' for part_of in source_record.get('partof')] if source_record.get('partof') else None,
        spatial=item_details.get('location')[0] if item_details.get('location') else None,
        has_part=_get_has_part(first_file_resource, source_id)
    )


def _get_medium(source_record: dict) -> Optional[str]:
    mediums = source_record.get('original_format', [])

    if not mediums:
        return None
    
    return mediums[0]


def _parse_publishers(created_published_list: Optional[list[str]]) -> Optional[list[str]]:
    if created_published_list is None:
        return
    
    publishers = []

    for created_published_data in created_published_list:
        if ':' not in created_published_data:
            publisher_and_location_data = created_published_data.split(',', 1)

            if len(publisher_and_location_data) >= 2 and ',' in publisher_and_location_data[1]:
                publisher = publisher_and_location_data[1].split(',')[0].strip(' ')
                publishers.append(publisher)
        else:
            publisher_and_location_data = created_published_data.split(':', 1)
            publisher_info = publisher_and_location_data[1]
            publisher = publisher_info.split(',', 1)[0].strip()
            publishers.append(publisher)

    return publishers if publishers else None


def _get_has_part(first_file_resource: dict, source_id: str) -> list[str]:
    has_part = []

    if 'pdf' in first_file_resource.keys():
        has_part.append(str(Part(
            index=1,
            url=first_file_resource['pdf'],
            source=Source.LOC.value,
            file_type='application/pdf',
            flags=str(FileFlags(download=True))
        )))

    if 'epub_file' in first_file_resource.keys():
        record_id = source_id.split('|')[0]
        epub_location = f'epubs/loc/{record_id}.epub'
        epub_url = get_stored_file_url(storage_name=os.environ['FILE_BUCKET'], file_path=epub_location)

        has_part.append(str(Part(
            index=1,
            url=epub_url,
            source=Source.LOC.value,
            file_type='application/epub+zip',
            flags=str(FileFlags(download=True)),
            source_url=first_file_resource['epub_file']
        )))

    return has_part
