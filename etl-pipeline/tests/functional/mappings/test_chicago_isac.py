import json
from mappings.chicago_isac import map_chicago_isac_record
from model import Source


def test_map_chicago_isac_record():
    with open("ingestJSONFiles/chicagoISAC_metadata.json") as f:
        chicago_isac_data = json.load(f)

    for chicago_isac_record in chicago_isac_data:
        dcdw_record = map_chicago_isac_record(chicago_isac_record)

        if dcdw_record is not None:
            assert dcdw_record.source == Source.CHICACO_ISAC.value
            assert dcdw_record.title
            assert dcdw_record.has_part
