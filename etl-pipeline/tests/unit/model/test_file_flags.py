import json

from model import FileFlags


def test_file_flags_str():
    assert str(FileFlags(catalog=True, nypl_login=True)) == json.dumps(
        {"catalog": True, "nypl_login": True}
    )
