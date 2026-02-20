import pytest
import os
import requests

from .utils import assert_response_status
from utils.common import require_env


@pytest.mark.parametrize(
    "endpoint, expected_status",
    [
        ("/editions/{edition_id}", 200),
        ("/editions/00000000-0000-0000-0000-000000000000", 400),
        ("/editions/invalid_id_format", 400),
        ("/editions/", 404),
        ("/editions/%$@!*", 400),
    ],
)
def test_get_edition(endpoint, expected_status, test_edition_id, db_manager):
    # DIAGNOSTIC: print all available FRBR records before requesting test_edition
    from model import (
        Edition,
        Item,
        Work,
    )

    full_results = (
        db_manager.session.query(Item, Edition, Work)
        .join(Edition, Edition.id == Item.edition_id)
        .join(Work, Work.id == Edition.work_id)
        .limit(500)
    )
    print("DIAGNOSTIC: FRBR items before GET /editions test")
    for row in full_results:
        print(
            f"{row.Work.title=} {row.Work.id=} {row.Edition.id=} {row.Item.id=} {row.Edition.title=}"
        )
    print(f"DIAGNOSTIC: test_get_edition -> {test_edition_id=}")

    url = require_env("DRB_API_URL") + endpoint.format(edition_id=test_edition_id)
    response = requests.get(url)

    assert response.status_code is not None
    assert_response_status(url, response, expected_status)

    if expected_status == 200:
        response_json = response.json()
        assert response_json is not None
