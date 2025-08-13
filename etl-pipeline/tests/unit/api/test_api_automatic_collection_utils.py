import pytest

from api.automaticCollectionUtils import fetchAutomaticCollectionEditions


def test_fetchAutomaticCollectionEditions_mostRecent(mocker):
    dbClient = mocker.MagicMock()
    dbClient.fetchAutomaticCollection.return_value = mocker.MagicMock(
        keyword_query=None,
        author_query=None,
        title_query=None,
        subject_query=None,
        sort_field="date",
        sort_direction="DESC",
        limit=100,
    )
    dbClient.fetchAllPreferredEditions.return_value = (
        20,
        mocker.sentinel.sortedEditions,
    )
    total, editions = fetchAutomaticCollectionEditions(
        dbClient,
        mocker.sentinel.esClient,
        mocker.sentinel.collectionId,
        perPage=10,
        page=1,
    )
    assert total == 20
    assert editions == mocker.sentinel.sortedEditions


def test_fetchAutomaticCollectionEditions_searchBased(mocker):
    dbClient = mocker.MagicMock()
    dbClient.fetchAutomaticCollection.return_value = mocker.MagicMock(
        keyword_query=mocker.sentinel.keyword_query,
        author_query=mocker.sentinel.author_query,
        title_query=mocker.sentinel.title_query,
        subject_query=mocker.sentinel.subject_query,
        sort_field="date",
        sort_direction="DESC",
        limit=100,
    )
    dbClient.fetchEditions.return_value = mocker.sentinel.editions
    esClient = mocker.MagicMock()

    def _mockHit(*editionIds):
        return mocker.MagicMock(
            meta=mocker.MagicMock(
                inner_hits=mocker.MagicMock(
                    editions=mocker.MagicMock(
                        hits=[mocker.MagicMock(edition_id=eid) for eid in editionIds],
                    )
                ),
            ),
        )

    class _mockHits(list):
        def __init__(self, totalCount, *items):
            self.total = mocker.MagicMock(value=totalCount)
            super().__init__(items)

    esClient.searchQuery.return_value = mocker.MagicMock(
        hits=_mockHits(
            20,
            _mockHit(mocker.sentinel.edition1, mocker.sentinel.edition2),
            _mockHit(mocker.sentinel.edition3),
        ),
    )
    assert fetchAutomaticCollectionEditions(
        dbClient,
        esClient,
        mocker.sentinel.collectionId,
        perPage=10,
        page=1,
    ) == (20, mocker.sentinel.editions)
    esClient.searchQuery.assert_called_once_with(
        {
            "query": [
                ("keyword", mocker.sentinel.keyword_query),
                ("author", mocker.sentinel.author_query),
                ("title", mocker.sentinel.title_query),
                ("subject", mocker.sentinel.subject_query),
            ],
            "filter": [],
            "sort": [("date", "DESC")],
            "show_all": True,
        },
        page=1,
        perPage=10,
    )
    dbClient.fetchEditions.assert_called_once_with(
        [
            mocker.sentinel.edition1,
            mocker.sentinel.edition2,
            mocker.sentinel.edition3,
        ],
    )
