from mappings.oclc_bib import map_oclc_record

base_oclc_bib = {
    'identifier': {
        'oclcNumber': 1234
    },
    'work': {
        'id': 1
    },
    'title': {
        'mainTitles': [{
            'text': 'The Story of DRB'
        }]
    },
    'subjects': [{
        'subjectName': {
            'text': 'Subject'
        },
        'vocabulary': 'fast'
    }],
    'contributor': {
        'creators': [{
            'firstName': {
                'text': 'Hathi'
            },
            'secondName': {
                'text': 'Trust'
            },
            'isPrimary': True
        }]
    },
    'digitalAccessAndLocations': [
        {
            'uri': 'http://localhost:5000/text.pdf'
        }
    ]
}


def test_oclc_bib_mapping_full_name():
    oclc_record = map_oclc_record(base_oclc_bib)

    assert ['Trust, Hathi|||true'] == oclc_record.authors
    

def test_oclc_bib_mapping_no_first_name():
    base_oclc_bib['contributor'] = {
        'creators': [{
            'secondName': {
                'text': 'Trust'
            },
            'isPrimary': True
        }]
    }

    oclc_record = map_oclc_record(base_oclc_bib)

    assert ['Trust|||true'] == oclc_record.authors

def test_oclc_bib_mapping_no_second_name():
    base_oclc_bib['contributor'] = {
        'creators': [{
            'firstName': {
                'text': 'Hathi'
            },
            'isPrimary': True
        }]
    }

    oclc_record = map_oclc_record(base_oclc_bib)

    assert ['Hathi|||true'] == oclc_record.authors

def test_oclc_bib_mapping_fallback_to_romanized_text():
    base_oclc_bib['contributor'] = {
        'creators': [{
            'firstName': {
                'romanizedText': 'Homer'
            },
            'secondName': {
                'romanizedText': 'Simpson'
            },
            'isPrimary': True
        }]
    }

    oclc_record = map_oclc_record(base_oclc_bib)

    assert ['Simpson, Homer|||true'] == oclc_record.authors
