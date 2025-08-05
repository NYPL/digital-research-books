from flask import request
from elasticsearch_dsl import Search, Q
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from textwrap import shorten
from typing import Optional

from .items import items_blueprint
from managers import DBManager
from model import ESPage, Item, Record
from api.utils import APIUtils

RESPONSE_TYPE = "itemSearchResponse"

try:
    embedder = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
except:
    embedder = None


@items_blueprint.route("/<item_id>/search", methods=["GET"])
def search_item(item_id):
    with DBManager() as db_manager:
        record_id = (
            db_manager.session.query(Record.id)
            .join(Item, Item.record_id == Record.id)
            .filter(Item.id == int(item_id))
            .scalar()
        )

    search_request = get_search_request(str(record_id))

    if not search_request:
        return APIUtils.formatResponseObject(
            400, RESPONSE_TYPE, {"message": "Unable to execute search"}
        )

    size = int(request.args.get("size", 10))
    search_request = search_request[:size]
    search_response = search_request.execute()

    results = [
        {
            "textPreview": shorten(hit.text, width=360, placeholder="..."),
            "highlightedText": list(hit.meta.highlight.text)
            if "highlight" in hit.meta
            else None,
            "readLink": f"/items/{item_id}/read/{hit.page_id}",
        }
        for hit in search_response
    ]

    return APIUtils.formatResponseObject(200, RESPONSE_TYPE, results)


def get_search_request(record_id: str) -> Optional[Search]:
    mode = request.args.get("mode", "keyword")
    query = request.args.get("kw")
    keyword_query = Q("match", text=query)

    if mode == "keyword":
        return (
            Search(index=ESPage.Index.name)
            .query(keyword_query)
            .filter("term", record_id=record_id)
            .highlight("text", fragment_size=150, number_of_fragments=3)
        )
    elif mode == "semantic":
        query = request.args.get("sq")

        embedding = embedder.embed_query(query)
        return (
            Search(index=ESPage.Index.name)
            .query(
                "script_score",
                query={"match_all": {}},
                script={
                    "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                    "params": {"query_vector": embedding},
                },
            )
            .filter("term", record_id=record_id)
            .highlight("text", fragment_size=150, number_of_fragments=3)
        )
    elif mode == "hybrid":
        embedding = embedder.embed_query(query)

        keyword_boost = 1.0
        semantic_boost = 2.0

        hybrid_query = Q(
            "bool",
            should=[
                Q("match", text={"query": query, "boost": keyword_boost}),
                Q(
                    "script_score",
                    query={"match_all": {}},
                    script={
                        "source": f"{semantic_boost} * cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                        "params": {"query_vector": embedding},
                    },
                ),
            ],
        )

        return (
            Search(index=ESPage.Index.name)
            .query(hybrid_query)
            .filter("term", record_id=record_id)
            .highlight("text", fragment_size=150, number_of_fragments=3)
        )

    return None
