from flask import request
from elasticsearch_dsl import Search, Q
from langchain_google_genai import GoogleGenerativeAIEmbeddings

from textwrap import shorten
from typing import Optional, Literal

from .items import items_blueprint
from managers import DBManager
from model import ESPage, Item, Record
from api.utils import APIUtils

RESPONSE_TYPE = "itemSearchResponse"
QueryMode = Literal["keyword", "semantic", "hybrid"]

try:
    embedder = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
except:
    embedder = None


@items_blueprint.route("/<item_id>/search", methods=["GET"])
def item_search(item_id):
    search_results = item_full_text_search(
        item_id,
        query_mode=request.args.get("mode", "keyword"),
        keyword=request.args.get("kw"),
        semantic_query=request.args.get("sq"),
        size=int(request.args.get("size", 10)),
    )

    return APIUtils.formatResponseObject(200, RESPONSE_TYPE, search_results)


def item_full_text_search(
    item_id: str,
    query_mode: QueryMode,
    keyword: Optional[str],
    semantic_query: Optional[str],
    size: int = 10,
) -> list[dict]:
    with DBManager() as db_manager:
        record_id = (
            db_manager.session.query(Record.id)
            .join(Item, Item.record_id == Record.id)
            .filter(Item.id == int(item_id))
            .scalar()
        )

    if query_mode == "keyword":
        search_request = (
            Search(index=ESPage.Index.name)
            .query(Q("match", text=keyword))
            .filter("term", record_id=record_id)
            .highlight("text", fragment_size=150, number_of_fragments=3)
        )
    elif query_mode == "semantic":
        embedding = embedder.embed_query(semantic_query)
        search_request = (
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
    elif query_mode == "hybrid":
        embedding = embedder.embed_query(semantic_query)

        keyword_boost = 1.0
        semantic_boost = 2.0

        hybrid_query = Q(
            "bool",
            should=[
                Q("match", text={"query": keyword, "boost": keyword_boost}),
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

        search_request = (
            Search(index=ESPage.Index.name)
            .query(hybrid_query)
            .filter("term", record_id=record_id)
            .highlight("text", fragment_size=150, number_of_fragments=3)
        )
    else:
        raise Exception(f"Unknown query mode: {query_mode}")

    search_request = search_request[:size]
    search_response = search_request.execute()

    return [
        {
            "textPreview": shorten(hit.text, width=360, placeholder="..."),
            "highlightedText": list(hit.meta.highlight.text)
            if "highlight" in hit.meta
            else None,
            "readLink": f"/items/{item_id}/read/{hit.page_id}",
        }
        for hit in search_response
    ]
