import json
import os
import random
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
import requests_mock
from logger import create_log
from managers import DBManager, RedisManager, S3Manager
from model import (
    Collection,
    Edition,
    FileFlags,
    Item,
    Link,
    Part,
    Record,
    RecordState,
    Work,
)
from model.postgres.item import ITEM_LINKS
from processes import RecordClusterer
from processes.grin.grin_client import GRINClient
from sqlalchemy import delete, text
from utils.load_env import load_env

from tests.fixtures.generate_test_data import generate_test_data

logger = create_log(__name__)

TEST_SOURCE = "test_source"


def pytest_addoption(parser):
    parser.addoption(
        "--env", action="store", default="local", help="Environment to use for tests"
    )


# NOTE: autouse=True does not guarantee execution before other session scoped \
# fixtures unless an explicit dependency on setup_env is specified.
# NOTE: if an error occurs in setup, the error is reported for each test in \
# the session, but the captured stdout/logs messages appear only for the first \
# occurrence of the error
@pytest.fixture(scope="session", autouse=True)
def setup_env(pytestconfig, request):
    # Check if test session is all tests in the unit/ folder
    only_unit_tests = all("unit" in item.keywords for item in request.session.items)
    # NOTE: pytest item keywords are based on the path from the pytest rootdir, \
    # which is set by default to the highest dir containing conftest.py. So as long as \
    # the "unit" directory is at or below conftest.py in the directory tree \
    # (and pytest `--rootdir` is not overridden), the "unit" keyword will be \
    # present for all tests under the "unit/" folder.

    # Set environment
    # empty strings (and other things that coerce to False) will be set as 'local'
    # NOTE: `make integration` and `make functional` without ENVIRONMENT set \
    # will set an empty string to --env=
    environment = pytestconfig.getoption("--env") or "local"

    # Error if attempting to run function or integration tests against \
    # production environment
    if (not only_unit_tests) and ("production" in environment):
        pytest.exit(
            "ENVIRONMENT ERROR: Integration and functional tests cannot be run on production environments."
        )

    print(f'Loading environment: "{environment}" during test setup')
    config_dir = Path(__file__).parent.parent / "config"
    load_env(config_dir / f".env.{environment}", raise_if_no_file=True)
    # Setting ENVIRONMENT so that downstream fixtures and tests can determine \
    # execution behavior based on the environment
    os.environ["ENVIRONMENT"] = environment


def pytest_sessionstart(session):
    # Configure logging for the full duration of the testing session

    import logging

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,
            "root": {
                "level": "WARNING",
            },
            "loggers": {
                "drb": {
                    "level": "DEBUG",
                },
            },
        }
    )


def create_or_update_record(record_data: dict, db_manager: DBManager) -> Record:
    existing_record = (
        db_manager.session.query(Record)
        .filter(Record.source_id == record_data.get("source_id"))
        .first()
    )

    if existing_record:
        for key, value in record_data.items():
            if key != "uuid" and hasattr(existing_record, key):
                setattr(existing_record, key, value)

        existing_record.date_modified = datetime.now(timezone.utc).replace(tzinfo=None)
        record_data["uuid"] = existing_record.uuid

        db_manager.session.commit()

        return existing_record

    new_record = Record(**record_data)

    db_manager.session.add(new_record)
    db_manager.session.commit()

    return new_record


@pytest.fixture(scope="session")
def db_manager(setup_env):
    db_manager = DBManager()
    try:
        db_manager.create_session()
        db_manager.session.execute(text("SELECT 1"))

        yield db_manager

        db_manager.close_connection()
    except:
        print("db_manager error")
        import traceback

        traceback.print_exc()
        yield None


@pytest.fixture(scope="session")
def s3_manager(setup_env):
    try:
        s3_manager = S3Manager()

        yield s3_manager
    except:
        yield None


@pytest.fixture(scope="session")
def redis_manager(setup_env):
    try:
        manager = RedisManager()
        manager.create_client()
    except:
        return None
    else:
        yield manager

        if os.environ.get("ENVIRONMENT") not in {"qa", "production"}:
            manager.clear_cache()


@pytest.fixture(scope="session")
def test_title():
    return "Integration Test Book"


@pytest.fixture(scope="session")
def test_subject():
    return "Integration Test Subject"


@pytest.fixture(scope="session")
def test_language():
    return "Integration Test Language"


@pytest.fixture(scope="session")
def frbrized_record_data(
    db_manager, redis_manager, test_title, test_subject, test_language
):
    # TODO: find path forward to connect to (localhost? qa?) db in GH actions
    if db_manager is None:
        return {
            "edition_id": 1982731,
            "work_id": "701c5f00-cd7a-4a7d-9ed1-ce41c574ad1d",
            "link_id": 1982731,
        }

    flags = {"catalog": False, "download": False, "reader": False, "embed": True}
    test_frbrized_record_data = {
        "title": test_title,
        "uuid": uuid4(),
        "frbr_status": "complete",
        "state": RecordState.EMBELLISHED.value,
        "cluster_status": False,
        "source": TEST_SOURCE,
        "authors": ["Ayan||true"],
        "languages": [test_language],
        "dates": ["1907-|publication_date"],
        "publisher": ["Project Gutenberg Literary Archive Foundation||"],
        "identifiers": [],
        "source_id": "4064148285|test",
        "contributors": [
            "Metropolitan Museum of Art (New York, N.Y.)|||contributor",
            "Metropolitan Museum of Art (New York, N.Y.)|||repository",
            "Thomas J. Watson Library|||provider",
        ],
        "extent": ("11, 164 p. ;"),
        "is_part_of": ["Tauchnitz edition|Vol. 4560|volume"],
        "abstract": ["test abstract 1", "test abstract 2"],
        "subjects": [f"{test_subject}||"],
        "rights": (
            "hathitrust|public_domain|expiration of copyright term for non-US work with corporate author|Public Domain|2021-10-02 05:25:13"
        ),
        "has_part": [
            f"1|example.com/1.pdf|{TEST_SOURCE}|text/html|{json.dumps(flags)}"
        ],
    }

    frbrized_record = create_or_update_record(
        record_data=test_frbrized_record_data, db_manager=db_manager
    )

    record_clusterer = RecordClusterer(
        db_manager=db_manager, redis_manager=redis_manager
    )
    record_clusterer.cluster_record(frbrized_record)

    frbrized_model = (
        db_manager.session.query(Item, Edition, Work)
        .join(Edition, Edition.id == Item.edition_id)
        .join(Work, Work.id == Edition.work_id)
        .filter(Item.record_id == frbrized_record.id)
        .first()
    )

    item, edition, work = frbrized_model if frbrized_model else (None, None, None)

    links = (
        db_manager.session.query(Link)
        .join(ITEM_LINKS)
        .filter(ITEM_LINKS.c.item_id == item.id)
        .all()
    )

    yield {
        "edition_id": str(edition.id) if item else None,
        "work_id": str(work.uuid) if work else None,
        "link_id": links[0].id if links and len(links) > 0 else None,
    }


@pytest.fixture(scope="session")
def test_edition_id(frbrized_record_data):
    return frbrized_record_data.get("edition_id")


@pytest.fixture(scope="session")
def test_collection_id(db_manager, test_edition_id):
    if not db_manager:
        yield "3650664c-c8be-4d07-8d64-2d7003b02048"
        return

    edition = (
        db_manager.session.query(Edition).filter(Edition.id == test_edition_id).first()
    )
    test_collection = Collection(
        title="Test Collection",
        uuid=uuid4(),
        creator="Integration Tests",
        owner="Integration Tests",
        description="A test collection for integration tests.",
        type="static",
        editions=[edition],
    )

    db_manager.session.add(test_collection)
    db_manager.session.commit()

    yield test_collection.uuid

    with db_manager.engine.connect() as connection:
        with connection.begin():
            connection.execute(
                delete(Collection).where(Collection.uuid == test_collection.uuid)
            )


@pytest.fixture(scope="session")
def test_work_id(frbrized_record_data):
    return frbrized_record_data.get("work_id")


@pytest.fixture(scope="session")
def test_link_id(frbrized_record_data):
    return frbrized_record_data.get("link_id")


@pytest.fixture(scope="session")
def unembellished_record_uuid(db_manager):
    test_unembellished_record_data = {
        "title": "Emma",
        "uuid": uuid4(),
        "frbr_status": "to_do",
        "cluster_status": False,
        "source": TEST_SOURCE,
        "authors": ["Jane, Austen||true"],
        "identifiers": ["0198837755|isbn"],
        "source_id": "0198837755|isbn",
        "date_modified": datetime.now(timezone.utc).replace(tzinfo=None),
    }

    unembellished_record = create_or_update_record(
        record_data=test_unembellished_record_data, db_manager=db_manager
    )

    return unembellished_record.uuid


@pytest.fixture(scope="session")
def unembellished_pipeline_record_uuid(db_manager):
    test_unembellished_pipeline_record_data = {
        "title": "Sense and sensibility",
        "uuid": uuid4(),
        "frbr_status": "to_do",
        "cluster_status": False,
        "source": TEST_SOURCE,
        "authors": ["Austen, Jane||true"],
        "identifiers": ["1503292738|isbn"],
        "source_id": "1503292738|isbn",
        "dates": ["1811|publication_date"],
        "has_part": [
            str(
                Part(
                    index=1,
                    url="https://example.com/book.epub",
                    source=TEST_SOURCE,
                    file_type="application/epub+zip",
                    flags=str(FileFlags(embed=True)),
                )
            ),
        ],
        "date_modified": datetime.now(timezone.utc).replace(tzinfo=None),
    }

    unembellished_pipeline_record = create_or_update_record(
        record_data=test_unembellished_pipeline_record_data, db_manager=db_manager
    )

    return unembellished_pipeline_record.uuid


@pytest.fixture(scope="session")
def unclustered_record_uuid(db_manager):
    test_unclustered_record_data = generate_test_data(
        title="unclustered record", uuid=uuid4(), source_id="unclustered|test"
    )

    unclustered_record = create_or_update_record(
        record_data=test_unclustered_record_data, db_manager=db_manager
    )

    return unclustered_record.uuid


@pytest.fixture(scope="session")
def unclustered_pipeline_record_uuid(db_manager):
    test_unclustered_record_data = generate_test_data(
        title="unclustered pipeline record", uuid=uuid4(), source_id="unclustered|test"
    )

    unclustered_record = create_or_update_record(
        record_data=test_unclustered_record_data, db_manager=db_manager
    )

    return unclustered_record.uuid


@pytest.fixture(scope="session")
def unclustered_multi_edition_uuid(db_manager):
    test_unclustered_edition_data = generate_test_data(
        title="multi edition record",
        uuid=uuid4(),
        source_id="unclustered_edition|test",
        dates=["1988|publication_date"],
        identifiers=["1234567891011|isbn"],
    )
    test_unclustered_edition_data2 = generate_test_data(
        title="the multi edition record",
        uuid=uuid4(),
        source_id="unclustered_edition2|test",
        dates=["1977|publication_date"],
        identifiers=["1234567891011|isbn"],
    )

    unclustered_multi_edition = create_or_update_record(
        record_data=test_unclustered_edition_data, db_manager=db_manager
    )
    create_or_update_record(
        record_data=test_unclustered_edition_data2, db_manager=db_manager
    )

    return unclustered_multi_edition.uuid


@pytest.fixture(scope="session")
def unclustered_multi_item_uuid(db_manager):
    test_unclustered_item_data = generate_test_data(
        title="multi item record",
        uuid=uuid4(),
        source_id="unclustered_item|test",
        dates=["1966|publication_date"],
        identifiers=["2341317561|isbn"],
    )
    test_unclustered_item_data2 = generate_test_data(
        title="multi item record",
        uuid=uuid4(),
        source_id="unclustered_item2|test",
        dates=["1966|publication_date"],
        identifiers=["2341317561|isbn"],
        has_part=[
            f"1|example.com/2.pdf|{TEST_SOURCE}|text/html|{str(FileFlags(embed=True))}"
        ],
    )

    unclustered_multi_item = create_or_update_record(
        record_data=test_unclustered_item_data, db_manager=db_manager
    )
    create_or_update_record(
        record_data=test_unclustered_item_data2, db_manager=db_manager
    )

    return unclustered_multi_item.uuid


@pytest.fixture(scope="session")
def limited_access_record_uuid(db_manager):
    test_limited_access_record_data = {
        "title": "Bluets",
        "uuid": uuid4(),
        "frbr_status": "complete",
        "cluster_status": False,
        "authors": ["Nelson, Maggie||true"],
        "dates": ["2009|publication_date"],
        "publisher": ["Wave Books||"],
        "identifiers": ["1933517409|isbn"],
        "rights": "in_copyright|cc-by|Public Domain|expired_copyright|2024-01-01",
        "contributors": ["qaContributor|||contributor"],
        "subjects": ["poetry||"],
        "source": TEST_SOURCE,
        "source_id": "pbtestSourceID",
        "publisher_project_source": ["University of Michigan Press"],
        "has_part": [
            str(
                Part(
                    index=1,
                    url="https://example.com/book.epub",
                    source=TEST_SOURCE,
                    file_type="application/epub+zip",
                    flags=str(
                        FileFlags(
                            reader=True, nypl_login=True, fulfill_limited_access=True
                        )
                    ),
                )
            ),
        ],
    }

    limited_access_record = create_or_update_record(
        record_data=test_limited_access_record_data, db_manager=db_manager
    )

    return limited_access_record.uuid


@pytest.fixture
def mock_epub_to_webpub(requests_mock):
    requests_mock.real_http = True
    with open("tests/fixtures/webpub-manifest.json", "rb") as f:
        response_content = f.read()

    matcher = re.compile("https://epub-to-webpub.vercel.app/api/.*")
    requests_mock.get(matcher, content=response_content)


@pytest.fixture
def mock_sqs_manager():
    with patch("processes.record_ingestor.SQSManager") as MockSQSManager:
        mock_sqs_manager_instance = MagicMock()
        MockSQSManager.return_value = mock_sqs_manager_instance

        yield mock_sqs_manager_instance


@pytest.fixture(scope="module")
def grin_client(setup_env):
    client = GRINClient()

    yield client


@pytest.fixture()
def generate_test_barcodes(grin_client):
    available_barcodes_scrape_fragment = "_all_books?&book_state=NEW&format=text"
    byte_response = grin_client.get(available_barcodes_scrape_fragment)
    lines = byte_response.decode("utf8").strip().split("\n")
    filtered_barcodes = [
        b.strip() for b in lines if b.strip().isdigit() and len(b.strip()) == 14
    ]

    return (
        random.sample(filtered_barcodes, 5)
        if len(filtered_barcodes) >= 5
        else filtered_barcodes
    )


@pytest.fixture()
def downloadable_barcode(grin_client):
    today = datetime.now()
    three_days = today - timedelta(3)
    range_start = three_days.strftime("%Y-%m-%d")
    range_end = today.strftime("%Y-%m-%d")

    converted_book = grin_client.get(
        "_converted?result_count=1&last_conversion_date_start=%s&last_conversion_date_end=%s&book_state=PREVIOUSLY_DOWNLOADED&format=text"
        % (range_start, range_end)
    )
    barcode = converted_book.decode("utf8").strip().split(".")[0]

    return barcode


@pytest.fixture()
def expected_barcodes_statuses():
    return [
        "Success",
        "Already being converted",
        "Not allowed to be downloaded",
        "Other error",
    ]
