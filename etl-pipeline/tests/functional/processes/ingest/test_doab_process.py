from model import Source
from processes import DOABProcess
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests


def test_doab_process():
    doab_process = DOABProcess('complete', None, None, None, 1, None)

    doab_process.runProcess()

    records = assert_ingested_records(sources=[Source.DOAB.value])
    assert_uploaded_manifests(records)
