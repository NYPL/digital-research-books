import re
from . import utils
from model import FileFlags, FRBRStatus, Part, Record, Source
from uuid import uuid4
from datetime import datetime, timezone
from mappings.base_mapping import CustomFormatter
from .rights import get_rights_string


DEFAULT_PUBLISHER = 'John Hopkins University Press||'
MUSE_ROOT = 'https://muse.jhu.edu'


def map_muse_record(muse_record) -> Record:
    identifiers = _get_identifiers(muse_record)
    alternative = _get_formatted_field(muse_record, '240', '{a} {k}')
    has_version = _get_formatted_field(muse_record, '250', '{a} {b}|')
    spatial = _get_formatted_field(muse_record, '264', '{a}')
    extent = _get_formatted_field(muse_record, '300', '{a}{b}{c}')
    toc = _get_formatted_field(muse_record, '505', '{a}')

    rights = get_rights_string(
        rights_source=Source.MUSE.value,
        license='https://creativecommons.org/licenses/by-nc/4.0/',
        rights_statement='Attribution-NonCommercial 4.0 International'
    )

    return Record(
        uuid=uuid4(),
        frbr_status=FRBRStatus.TODO.value,
        cluster_status=False,
        source=Source.MUSE.value,
        source_id=list(identifiers[0].split('|'))[0],
        identifiers=identifiers,
        authors=_get_authors(muse_record),
        title=_get_title(muse_record),
        alternative=alternative[0] if alternative else None,
        has_version=has_version[0] if has_version else None,
        publisher=_get_publishers(muse_record),
        spatial=spatial[0] if spatial else None,
        dates=_get_dates(muse_record),
        languages=_get_languages(muse_record),
        extent=extent[0] if extent else None,
        table_of_contents=toc[0] if toc else  None,
        abstract=_get_abstracts(muse_record),
        subjects=_get_subjects(muse_record),
        contributors=_get_contributors(muse_record),
        is_part_of=_get_formatted_field(muse_record, '490', '{a}|{v}|volume'),
        has_part=_get_has_part(muse_record),
        rights=rights,
        date_created=datetime.now(timezone.utc).replace(tzinfo=None),
        date_modified=datetime.now(timezone.utc).replace(tzinfo=None)
    )

def add_has_part_link(record, url, media_type, flags, source_url=None):
    last_item_no = int(record.has_part[-1][0])

    record.has_part.append(str(Part(
        index=last_item_no,
        source=Source.MUSE.value,
        url=url,
        file_type=media_type,
        flags=flags,
        source_url=source_url
    )))

def _get_formatted_field(record: Record, field: str, string_format: str) -> list[str]:
    formatted_data = []
    field_data = record.get_fields(field)
    subfield_keys = re.findall(r'\{(\w+)\}', string_format)
    formatter = CustomFormatter()

    for data in field_data:
        if data.is_control_field():
            subfield_values = {subfield_keys[0]: getattr(data, 'data')}
            formatted_string = formatter.format(string_format, *subfield_values.values())
            formatted_data.append(utils.clean_formatted_string(formatted_string))
        else:
            subfield_values = { key: data.get_subfields(key)[0] for key in subfield_keys if data.get_subfields(key) }
            
            if any(subfield_values.values()):
                formatted_string = formatter.format(string_format, **subfield_values)
                formatted_data.append(utils.clean_formatted_string(formatted_string))
    
    return formatted_data

def _get_formatted_fields(record: Record, fields: list[tuple[str, str]]) -> list[str]:
    return [formatted_item for field, string_format in fields for formatted_item in _get_formatted_field(record, field, string_format)]

def _get_identifiers(record):
    fields = [
        ('001', '{0}|muse'),
        ('010', '{z}|lccn'),
        ('020', '{a}{z}|isbn'),
        ('022', '{a}|issn'),
        ('035', '{a}|oclc'),
        ('040', '{a}|muse')
    ]
    all_identifiers = _get_formatted_fields(record, fields)

    return [_cleanup_identifier(identifier) for identifier in all_identifiers]

def _get_authors(record):
    fields = [
        ('100', '{a} {b} {c} {d}|||true'),
        ('110', '{a} {b} {c} {n} {d}|||true'),
    ]
    return _get_formatted_fields(record, fields)

def _get_title(record):
    fields = [
        ('245', '{a} {b}'),
        ('130', '{a}')
    ]

    all_titles = _get_formatted_fields(record, fields)

    return all_titles[0]

def _get_publishers(record):
    publishers = _get_formatted_field(record, '264', '{b}||')

    return publishers or [DEFAULT_PUBLISHER]

def _get_dates(record):
    dates = _get_formatted_field(record, '264', '{c}|publication_date')

    if not dates and (publication_date := record['008'].data[11:15]):
        dates.append(f'{publication_date}|publication_date')

    return dates

def _get_languages(record):
    languages = _get_formatted_field(record, '008', '||{0}')
    formatted_languages = [
        extracted_langauge
        for language in languages
        if (extracted_langauge := _extract_language(language))
    ]

    return formatted_languages

def _get_abstracts(record):
    fields = [
        ('500', '{a}'),
        ('520', '{a}'),
        ('504', '{a}')
    ]

    return _get_formatted_fields(record, fields)

def _get_subjects(record):
    fields = [
        ('600', '{a} {d} -- {v} -- {x} -- {y} -- {z}|{2}|{0}'),
        ('610', '{a} {d} -- {v} -- {x} -- {y} -- {z}|{2}|{0}'),
        ('611', '{a} {d} -- {v} -- {x} -- {y} -- {z}|{2}|{0}'),
        ('630', '{a} {p} -- {v} -- {x} -- {y} -- {z}|{2}|{0}'),
        ('650', '{a} {b} -- {v} -- {x} -- {y} -- {z}|{2}|{0}'),
        ('651', '{a} -- {v} -- {x} -- {y} -- {z}|{2}|{0}'),
        ('656', '{a} -- {v} -- {x} -- {y} -- {z}|{2}|{0}')
    ]
    
    all_subjects = _get_formatted_fields(record, fields)

    return [_clean_up_subject_head(subject) for subject in all_subjects]

def _get_contributors(record):
    fields = [
        ('260', '{f}|||manufacturer'),
        ('700', '{a} {b} {c} {d}|||{e}'),
        ('710', '{a} {b} {c} {d}|||{e}'),
        ('711', '{a} {e}|||{j}')
    ]

    return _get_formatted_fields(record, fields)

def _get_has_part(record):
    has_part = []
    field_data = record.get_fields('856')
    for data in field_data:
        url = data.get_subfields('u')[0]
        if url:
            has_part.append(str(Part(
                index=1,
                source=Source.MUSE.value,
                url=url,
                file_type='text/html',
                flags=str(FileFlags(embed=True))
            )))
        
    return has_part

def _clean_up_subject_head(subject):
    subject_str, *subject_metadata = subject.split('|')
    subject_parts = subject_str.split('--')

    out_parts = []

    for part in subject_parts:
        clean_parts = part.strip(' .')
        
        if clean_parts == '': continue

        out_parts.append(clean_parts)

    cleaned_subject = ' -- '.join([part for part in out_parts])

    return '|'.join([cleaned_subject] + subject_metadata)

def _extract_language(language):
    _, _, marc_data, *_ = language.split('|')
    marc_data = marc_data.split(' ')

    # MARC data example: 100607s2011 mdu o 00 0 eng d
    if len(marc_data) >= 7:
        return f'||{marc_data[5]}'

    return None

def _cleanup_identifier(identifier):
    oclc_number_prefix = '(OCoLC)'
    id, id_type = identifier.split('|')
    id = id.strip()

    if id.startswith(oclc_number_prefix):
        return f'{id[len(oclc_number_prefix):]}|{id_type}'
    
    return f'{id}|{id_type}'
