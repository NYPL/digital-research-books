import pytest
import os
import requests
import functools

from .utils import assert_response_status
from utils.common import require_env

import logging


def caplog_setup_call(level=None, logger=""):
    if level is None:
        level = logging.WARNING

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Find caplog in both positional and keyword arguments
            caplog = kwargs.get("caplog")
            if caplog is None:
                for arg in args:
                    if isinstance(arg, pytest.LogCaptureFixture):
                        caplog = arg
                        break
            if caplog is None:
                raise ValueError(
                    f"No caplog fixture found in {func.__name__} arguments"
                )
            # caplog.set_level(level)

            result = func(*args, **kwargs)

            for when in "setup":  # , 'call'):
                records = caplog.get_records(when)
                print(f"XXX {records=}")
                for record in records:
                    if level <= record.levelno:
                        if record.name.startswith(logger):
                            logging.getLogger(record.name).handle(record)

            return result

        return wrapper

    return decorator


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
@caplog_setup_call(level=logging.DEBUG)
def test_get_edition(endpoint, expected_status, test_edition_id, caplog):
    # caplog.set_level(logging.DEBUG, logger='drb')

    print(f"test_get_edition -> {test_edition_id=}")
    url = require_env("DRB_API_URL") + endpoint.format(edition_id=test_edition_id)
    response = requests.get(url)

    assert response.status_code is not None
    assert_response_status(url, response, expected_status)

    if expected_status == 200:
        response_json = response.json()
        assert response_json is not None

    print(f'{caplog.get_records("setup")=}')

    # logger='drb'
    # level= logging.DEBUG
    # # caplog.set_level(level)
    # for when in ('setup', 'call'):
    #     records = caplog.get_records(when)
    #     print(f'XXX {records=}')
    #     for record in records:
    #         if level <= record.levelno:
    #             if record.name.startswith(logger):
    #                 logging.getLogger(record.name).handle(record)
