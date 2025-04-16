from processes import MUSEProcess
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests


def test_muse_process():
    muse_process = MUSEProcess('complete', None, None, None, 5, None)
    number_of_records_ingested = muse_process.runProcess()

    muse_process.runProcess()

    records = assert_ingested_records(source_name='muse', expected_number_of_records=number_of_records_ingested)

    assert_uploaded_manifests(records)
