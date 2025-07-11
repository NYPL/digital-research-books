import os
import pytest
import requests
from urllib.parse import quote

from .utils import assert_response_status


@pytest.mark.parametrize(
    "endpoint, expected_status",
    [
        ("/search?query=keyword:{keyword}&page=1&size=5&readerVersion=v1", 200),
        ("/search/00000000-0000-0000-0000-000000000000", 404),
        ("/search/invalid_id_format", 404),
        ("/search/", 404),
        ("/search/%$@!*", 404),
    ],
)
def test_search(endpoint, expected_status, test_title):
    url = os.getenv("DRB_API_URL") + endpoint.format(keyword=quote(test_title))
    response = requests.get(url)

    assert response.status_code is not None
    assert_response_status(url, response, expected_status)

    if expected_status == 200:
        response_json = response.json()
        assert response_json is not None


@pytest.mark.parametrize(
    "endpoint, sort_field, sort_order",
    [
        (
            "/search?query=keyword:{keyword}&sort=date:desc&filter=language:{language}",
            "publication_date",
            "desc",
        ),
        (
            "/search?query=keyword:{keyword}&sort=date:asc&filter=language:{language}",
            "publication_date",
            "asc",
        ),
        (
            "/search?query=keyword:{keyword}&sort=author:desc&filter=language:{language}",
            "author",
            "desc",
        ),
        (
            "/search?query=keyword:{keyword}&sort=author:asc&filter=language:{language}",
            "author",
            "asc",
        ),
    ],
)
def test_search_language_and_sorting(
    endpoint, sort_field, sort_order, test_title, test_language
):
    url = os.getenv("DRB_API_URL") + endpoint.format(
        keyword=quote(test_title), language=quote(test_language)
    )
    response = requests.get(url)

    response_json = response.json()
    assert response_json is not None, "Response JSON is empty"

    for item in response_json.get("results", []):
        assert item.get("language") == test_language, (
            f"Unexpected language: {item.get('language')}"
        )

    sort_values = [item.get(sort_field) for item in response_json.get("results", [])]
    assert all(value is not None for value in sort_values), (
        f"Some {sort_field} values are missing: {sort_values}"
    )

    if sort_order == "desc":
        assert sort_values == sorted(sort_values, reverse=True), (
            f"{sort_field.capitalize()} values are not sorted in descending order: {sort_values}"
        )
    elif sort_order == "asc":
        assert sort_values == sorted(sort_values), (
            f"{sort_field.capitalize()} values are not sorted in ascending order: {sort_values}"
        )


@pytest.mark.parametrize(
    "endpoint",
    [
        ("/search?query=keyword:{keyword}&filter=format:readable"),
        ("/search?query=keyword:{keyword}&filter=format:downloadable"),
    ],
)
def test_search_readable_format_flags(endpoint, test_title):
    url = os.getenv("DRB_API_URL") + endpoint.format(keyword=quote(test_title))
    response = requests.get(url)

    response_json = response.json()
    assert response_json is not None, "Response JSON is empty"
    assert response.status_code == 200

    works = response_json.get("data", {}).get("works", [])

    is_downloadable = "format=downloadable" in endpoint
    is_readable = "format=readable" in endpoint

    links = [
        link
        for work in works
        for edition in work.get("editions", [])
        for item in edition.get("items", [])
        for link in item.get("links", [])
    ]

    for link in links:
        flags = link.get("flags", {})

        if is_readable:
            assert flags.get("reader", False) or flags.get("embed", False)

        if is_downloadable:
            assert flags.get("downloadable", False)
