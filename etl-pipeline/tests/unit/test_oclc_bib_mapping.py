from mappings.oclc_bib import OCLCBibMapping

base_oclc_bib = {
    "identifier": {"oclcNumber": 1234},
    "work": {"id": 1},
    "title": {"mainTitles": [{"text": "The Story of DRB"}]},
    "subjects": [{"subjectName": {"text": "Subject"}, "vocabulary": "fast"}],
    "contributor": {
        "creators": [
            {
                "firstName": {"text": "Hathi"},
                "secondName": {"text": "Trust"},
                "isPrimary": True,
            }
        ]
    },
}


def test_oclc_bib_mapping_full_name():
    oclc_bib_mapping = OCLCBibMapping(base_oclc_bib)

    assert ["Trust, Hathi|||true"] == oclc_bib_mapping.record.authors


def test_oclc_bib_mapping_no_first_name():
    base_oclc_bib["contributor"] = {
        "creators": [{"secondName": {"text": "Trust"}, "isPrimary": True}]
    }

    oclc_bib_mapping = OCLCBibMapping(base_oclc_bib)

    assert ["Trust|||true"] == oclc_bib_mapping.record.authors


def test_oclc_bib_mapping_no_second_name():
    base_oclc_bib["contributor"] = {
        "creators": [{"firstName": {"text": "Hathi"}, "isPrimary": True}]
    }

    oclc_bib_mapping = OCLCBibMapping(base_oclc_bib)

    assert ["Hathi|||true"] == oclc_bib_mapping.record.authors


def test_oclc_bib_mapping_fallback_to_romanized_text():
    base_oclc_bib["contributor"] = {
        "creators": [
            {
                "firstName": {"romanizedText": "Homer"},
                "secondName": {"romanizedText": "Simpson"},
                "isPrimary": True,
            }
        ]
    }

    oclc_bib_mapping = OCLCBibMapping(base_oclc_bib)

    assert ["Simpson, Homer|||true"] == oclc_bib_mapping.record.authors
