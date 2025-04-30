from constants.get_constants import get_constants


def test_get_constants():
    constants = get_constants()

    assert constants is not None
    assert constants.get("iso639", None) is not None
    assert constants.get("lc", None) is not None
    assert constants.get("hathitrust", None) is not None
    assert constants.get("marc", None) is not None
