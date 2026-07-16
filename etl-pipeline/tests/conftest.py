import json
import os
import random
import re
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import List
from unittest.mock import MagicMock, patch
from uuid import uuid4

import numpy as np
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
from vector_indexing.core.types import ChunkDocument
from vector_indexing.core.utils import Timer

logger = create_log(__name__)


# NOTE: use of pytest-xdist to run tests in parallel processes results in all \
# session scoped fixtures being run once in each process. Make sure to prevent \
# race conditions on external data modified in session fixtures.


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
    # Check if session is all tests in the unit/ folder
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

    # Exit if attempting to run function or integration tests against \
    # production environment
    if (not only_unit_tests) and ("production" in environment):
        pytest.exit(
            "ENVIRONMENT ERROR: Integration and functional tests cannot be run on production environments."
        )

    # Exit if required docker compose services are not healthy
    # ALT FUTURE: use a custom @pytest.mark to identify specific tests that \
    # require specific services to be up
    if (not only_unit_tests) and (environment == "local"):
        import subprocess

        # see: docker-compose.yaml
        REQUIRED_HEALTHY_CONTAINERS = [
            "drb_local_db",
            "drb_local_es",
            "drb_local_rs",
            "drb_local_api",
            "drb_local_aws",
        ]

        result = subprocess.run(
            ["docker", "ps", "--filter", "health=healthy", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
        )
        healthy_containers = set(result.stdout.split())
        missing = [
            name
            for name in REQUIRED_HEALTHY_CONTAINERS
            if name not in healthy_containers
        ]
        if missing:
            pytest.exit(
                "DOCKER HEALTHCHECK FAILED: `docker compose up` required for non-unit tests in 'local' env.\n\n"
                f"Containers not healthy: {missing}"
            )

    print(f'Loading environment: "{environment}" during test setup')
    config_dir = Path(__file__).parent.parent / "config"
    # Exit early if env cannot be loaded
    try:
        load_env(config_dir / f".env.{environment}", raise_if_no_file=True)
    except Exception as e:
        pytest.exit(
            f"Error loading environment {environment}: {e}\n\n{traceback.format_exc()}"
        )
    # Set ENVIRONMENT so that downstream fixtures and tests can determine \
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
    """Update fields of record identified by source_id"""
    existing_records = (
        db_manager.session.query(Record)
        .filter(Record.source_id == record_data.get("source_id"))
        .all()
    )
    if len(existing_records) > 1:
        raise ValueError(
            f"Multiple records found with source_id '{record_data.get('source_id')}'. Expected at most one."
        )
    existing_record = existing_records[0] if existing_records else None

    if existing_record:
        for key, value in record_data.items():
            if key != "uuid" and hasattr(existing_record, key):
                setattr(existing_record, key, value)
        existing_record.date_modified = datetime.now(timezone.utc).replace(tzinfo=None)

        # Q: this line appears pointless?
        record_data["uuid"] = existing_record.uuid

        # update DB to match current in-memory values of the Record ORM obj
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
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
    source_id = f"4064148285|test|{worker_id}"
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
        "source_id": source_id,
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

    print(f"DIAGNOSTIC: frbrized_record_data -> {worker_id=} {source_id=}")

    record_clusterer = RecordClusterer(
        db_manager=db_manager, redis_manager=redis_manager
    )
    record_clusterer.cluster_record(frbrized_record)

    with Timer(
        None,
        on_exit=lambda _, elapsed: print(
            f"Reading frbrized_model from DB took {elapsed:.2f}"
        ),
    ):
        # collect frbrized record by source_id (joining records slows this down slightly)
        frbrized_model = (
            db_manager.session.query(Item, Edition, Work)
            .join(Edition, Edition.id == Item.edition_id)
            .join(Work, Work.id == Edition.work_id)
            .join(Record, Record.id == Item.record_id)
            .filter(Record.source_id == frbrized_record.source_id)
            .first()
        )

    if not frbrized_model:
        raise Exception(
            f"No test FRBR data with source id {frbrized_record.source_id} after an attempt to create it."
        )

    item, edition, work = frbrized_model

    links = (
        db_manager.session.query(Link)
        .join(ITEM_LINKS)
        .filter(ITEM_LINKS.c.item_id == item.id)
        .all()
    )

    if not (links and len(links) > 0):
        raise Exception("No link data available after attempt to create it.")

    # DIAGNOSTIC: print all available FRBR records after test record creation
    print("DIAGNOSTIC: frbrized_record_data fixture selected ids:")
    print(
        f"{frbrized_record.id=} {frbrized_record.source_id=} {item.id=}, {edition.id=}, {work.id=} {links[0].id=}"
    )
    all_frbr_records = (
        db_manager.session.query(Item, Edition, Work)
        .join(Edition, Edition.id == Item.edition_id)
        .join(Work, Work.id == Edition.work_id)
        .limit(500)
    )
    print("DIAGNOSTIC: FRBR items after test FRBR record creation")
    for row in all_frbr_records:
        print(
            f"{row.Work.title=} {row.Work.id=} {row.Edition.id=} {row.Edition.title=} {row.Item.id=} {row.Item.record_id=} "  # TODO: load the identifiers relationship
        )
    all_item_links = (
        db_manager.session.query(Link)
        .join(ITEM_LINKS)
        .filter(ITEM_LINKS.c.item_id == item.id)
        .limit(500)
    )
    print(f"DIAGNOSTIC: Item={item.id} links after test FRBR record creation")
    for link in all_item_links:
        print(f"{link.id=} {item.id=}")

    yield {
        "edition_id": str(edition.id),
        "work_id": str(work.uuid),
        "link_id": links[0].id,
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


# TODO: see if similar functionality is duplicated elsewhere in teh code/test base
@pytest.fixture
def mock_search_backend(mocker):
    """
    Factory fixture that mocks all external I/O used in search_catalog and search_book.
    The ChunkDocuments passed to the factory are used as synthetic search results.

    For search_book all chunks should be from the same edition/book (this is not enforced).

    Call the returned function with a list of ChunkDocuments to initiate the patching.

    The fixture stubs:
      - hybrid_search → returns ChunkDocuments as ScoredHits
      - map_editions_and_records → returns synthetic item_ids keyed by barcode
      - get_frbr_data_by_edition → returns SimpleNamespace ORM-like rows
        built from each ChunkDocument's book_metadata (first chunk per edition),
        used by search_book/update_chat's contentSearch setup
      - get_frbr_data_by_barcode → returns SimpleNamespace ORM-like rows
        built from each ChunkDocument's book_metadata (first chunk per barcode),
        used by search_catalog
      - Embedder → returns a dummy zero vector from embed_query (via get_index_config())
      - Backend → replaced with a no-op mock (via get_index_config())

    Returns:
        Callable that accepts a list of ChunkDocuments and activates all mocks.
    """
    mock_embedder = mocker.MagicMock()
    mock_embedder.embed_query.return_value = np.zeros(768).tolist()
    mock_backend = mocker.MagicMock()
    mocker.patch(
        "api.assistant.agent.get_index_config",
        return_value={"embedder": mock_embedder, "backend": mock_backend},
    )

    def _first_chunk_doc_by(chunk_docs: List[ChunkDocument], key_fn) -> dict:
        """Map each unique key (via key_fn) to its first matching ChunkDocument."""
        first_by_key: dict = {}
        for chunk_doc in chunk_docs:
            key = key_fn(chunk_doc)
            first_by_key.setdefault(key, chunk_doc)
        return first_by_key

    def _to_frbr_row(chunk_doc: ChunkDocument, **extra_attrs) -> SimpleNamespace:
        """
        Build an ORM-like row from a ChunkDocument's book_metadata.
        format_frbr_fields() is the only place these attributes are read, so
        a SimpleNamespace is sufficient.
        """
        book_metadata = chunk_doc.book_metadata
        return SimpleNamespace(
            Work=SimpleNamespace(
                title=book_metadata.title,
                authors=[{"name": author} for author in book_metadata.author],
                subjects=[{"heading": subject} for subject in book_metadata.subject],
            ),
            Edition=SimpleNamespace(
                id=book_metadata.edition_id,
                publication_date=book_metadata.publication_date,
                publishers=[],  # not in BookMetadata; yields "(Publishers Unavailable)"
                languages=[{"language": lang} for lang in book_metadata.language],
            ),
            **extra_attrs,
        )

    def _make_frbr_lookup(rows_by_key: dict):
        """Build a get_frbr_data_by_*(keys) side_effect that looks up rows_by_key."""

        def _lookup(keys):
            return [rows_by_key[key] for key in keys if key in rows_by_key]

        return _lookup

    def _setup(chunk_docs: List[ChunkDocument]) -> List[ChunkDocument]:
        scored_hits = [(chunk_doc, 0.5) for chunk_doc in chunk_docs]
        mocker.patch("api.assistant.agent.hybrid_search", return_value=scored_hits)

        # Assign a synthetic item_id to each unique barcode.
        # results_to_chunk_hits only reads item_id from the mapper, so the
        # value is arbitrary as long as it is non-None.
        unique_barcodes = list(dict.fromkeys(cd.barcode for cd in chunk_docs))
        mapper = {
            barcode: {"item_id": i + 1} for i, barcode in enumerate(unique_barcodes)
        }
        mocker.patch(
            "api.assistant.agent.map_editions_and_records", return_value=mapper
        )

        # Used by search_book/update_chat's contentSearch setup.
        rows_by_edition_id = {
            edition_id: _to_frbr_row(chunk_doc)
            for edition_id, chunk_doc in _first_chunk_doc_by(
                chunk_docs, lambda cd: cd.book_metadata.edition_id
            ).items()
        }
        mocker.patch(
            "api.assistant.agent.get_frbr_data_by_edition",
            side_effect=_make_frbr_lookup(rows_by_edition_id),
        )

        # Used by search_catalog.
        rows_by_barcode = {
            barcode: _to_frbr_row(chunk_doc, barcode=barcode)
            for barcode, chunk_doc in _first_chunk_doc_by(
                chunk_docs, lambda cd: cd.barcode
            ).items()
        }
        mocker.patch(
            "api.assistant.agent.get_frbr_data_by_barcode",
            side_effect=_make_frbr_lookup(rows_by_barcode),
        )

        return chunk_docs

    return _setup


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

    converted_books = grin_client.get(
        "_converted?last_conversion_date_start=%s&last_conversion_date_end=%s&book_state=PREVIOUSLY_DOWNLOADED&format=text"
        % (range_start, range_end)
    )
    filenames = converted_books.decode("utf8").strip().split("\n")

    for filename in filenames:
        filename = filename.strip()
        if not filename:
            continue
        barcode = filename.split(".")[0]
        try:
            # Set stream=True to avoid downloading the full file
            response = grin_client.get(filename, stream=True)
            response.close()
            return barcode
        except IOError:
            continue

    return None


@pytest.fixture()
def expected_barcodes_statuses():
    return [
        "Success",
        "Already being converted",
        "Not allowed to be downloaded",
        "Other error",
    ]


@pytest.fixture
def test_session_id():
    """
    Provides a unique session_id per test for isolation across parallel workers.

    Each test is prefixed with "test_".

    Setup: deletes any stale data for the session_id.
    Teardown: prints the raw conversation (captured by pytest; shown on failure),
              then always deletes session data.
    """
    import uuid
    from api.assistant.agent import delete_session_data, get_session_messages

    session_id = f"test_{uuid.uuid4()}"
    delete_session_data(session_id)

    yield session_id

    # Print convo history to logs (because it will be deleted)
    print(f"\n--- Raw agent_messages for session '{session_id}' ---")
    messages = get_session_messages(session_id)
    print(json.dumps(messages, indent=2))
    print("--- End of conversation ---\n")

    delete_session_data(session_id)


# TODO: in all places where this is used simply replace this by mocking Session \
# with an in-memory `SQLiteSession`. That way no clean up is even needed bc DB \
# writes are never made.
@pytest.fixture
def test_session(test_session_id):
    """
    A JSONBSQLAlchemySession using `test_session_id` fixture for cleanup and isolation.

    Mostly a light wrapper of `test_session_id`.
    """
    from api.db import get_async_engine
    from api.assistant.session import JSONBSQLAlchemySession

    return JSONBSQLAlchemySession(test_session_id, engine=get_async_engine())
