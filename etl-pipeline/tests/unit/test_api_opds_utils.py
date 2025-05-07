from api.opdsUtils import OPDSUtils


class TestOPDSUtils:
    def test_addPagingOptions(self, mocker):
        mockMetadata = mocker.MagicMock()
        mockLink = mocker.MagicMock(rel="self", href="/test")
        mockFeed = mocker.MagicMock(metadata=mockMetadata, links=[mockLink])

        OPDSUtils.addPagingOptions(mockFeed, "/test", 150, perPage=50)

        assert mockLink.rel == ["self", "first", "previous"]
        assert mockLink.href == "/test?page=1"

        mockMetadata.addField.assert_has_calls(
            [
                mocker.call("numberOfItems", 150),
                mocker.call("itemsPerPage", 50),
                mocker.call("currentPage", 1),
            ]
        )

        mockFeed.addLink.assert_has_calls(
            [
                mocker.call(
                    {
                        "rel": "next",
                        "href": "/test?page=2",
                        "type": "application/opds+json",
                    }
                ),
                mocker.call(
                    {
                        "rel": "last",
                        "href": "/test?page=3",
                        "type": "application/opds+json",
                    }
                ),
            ]
        )
