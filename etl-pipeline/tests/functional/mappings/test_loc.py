import json
from mappings.loc import map_loc_record
from model import Source


def test_map_loc_record():
    with open("tests/fixtures/test-loc.json") as f:
        loc_data = json.load(f)

    loc_record = map_loc_record(loc_data)

    assert loc_record is not None
    assert loc_record.title == "Guia de espécies fitoterápicas brasileiras"
    assert loc_record.source_id == "2024350289|lccn"
    assert loc_record.identifiers == ["2024350289|lccn", "RS175.B7|call_number"]
    assert loc_record.authors == [
        "capote, anna claudia morais de oliveira|||true",
        "beltrame, flávio luís|||true",
    ]
    assert loc_record.medium == "book"
    assert loc_record.contributors == [
        "capote, anna claudia morais de oliveira|||contributor",
        "beltrame, flávio luís|||contributor",
    ]
    assert loc_record.languages == ["||por"]
    assert (
        loc_record.extent
        == "1 online resource (225 pages) : illustrations (chiefly color)"
    )
    assert loc_record.spatial == "Brazil"
    assert (
        loc_record.abstract
        == "Includes bibliographical references. Description based on online resource; title from PDF title page (Atena Editora, viewed February 26, 2025)."
    )
    assert loc_record.publisher == ["Atena Editora", "AL Editora Olyver"]
    assert loc_record.dates == ["2024|publication_date"]
    assert loc_record.subjects == [
        "Materia medica, Vegetable--Brazil||",
        "Medicinal plants--Brazil--Identification||",
    ]
    assert (
        loc_record.rights
        == "loc|Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International CC BY-NC-ND 4.0 https://creativecommons.org/licenses/by-nc-nd/4.0/legalcode|||"
    )
    assert loc_record.is_part_of == [
        "open access books|collection",
        "general collections|collection",
        "catalog|collection",
        "lc online resource|collection",
    ]
    assert loc_record.has_part == [
        '1|https://tile.loc.gov/storage-services/master/gdc/gdcebookspublic/20/24/35/02/89/2024350289/2024350289.pdf|loc|application/pdf|{"download": true}'
    ]
