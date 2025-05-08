import json
from model import RecordState

TEST_SOURCE = "test_source"


def generate_test_data(
    title="test record",
    uuid=None,
    frbr_status="complete",
    state=RecordState.EMBELLISHED.value,
    cluster_status=False,
    source=TEST_SOURCE,
    authors=["Test Author||true"],
    languages=["test_language"],
    dates=["1907|publication_date"],
    publisher=["test publisher||"],
    identifiers=[],
    source_id="test_source|test",
    contributors=["test contributor|||contributor"],
    extent="11, 164 p. ;",
    is_part_of=["Tauchnitz edition|Vol. 4560|volume"],
    abstract=["test abstract 1", "test abstract 2"],
    subjects=["test_subject||"],
    rights="test source|public_domain|expiration of copyright term for non-US work with corporate author|Public Domain|2021-10-02 05:25:13",
    has_part=None,
    flags={"catalog": False, "download": False, "reader": False, "embed": True},
):
    return {
        "title": title,
        "uuid": uuid,
        "frbr_status": frbr_status,
        "cluster_status": cluster_status,
        "state": state,
        "source": source,
        "authors": authors,
        "languages": languages,
        "dates": dates,
        "publisher": publisher,
        "identifiers": identifiers,
        "source_id": source_id,
        "contributors": contributors,
        "extent": extent,
        "is_part_of": is_part_of,
        "abstract": abstract,
        "subjects": subjects,
        "rights": rights,
        "has_part": has_part
        or [f"1|example.com/1.pdf|{TEST_SOURCE}|text/html|{json.dumps(flags)}"],
    }
