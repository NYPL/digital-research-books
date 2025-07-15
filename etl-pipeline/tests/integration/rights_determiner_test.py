from services.rights_determiner import determine_rights


def test_determine_rights():
    rights = determine_rights("33433116781380")

    assert rights == "hathitrust|in_copyright||In Copyright|"
