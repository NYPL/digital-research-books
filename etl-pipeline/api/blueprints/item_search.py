from flask import Blueprint, current_app, request, jsonify
from elasticsearch_dsl import Search, Q
from langchain_google_genai import GoogleGenerativeAIEmbeddings

from model import ESPage

item_search = Blueprint("item_search", __name__)


@item_search.route("/items/<item_id>/search", methods=["GET"])
def search_item(item_id):
    embedder = GoogleGenerativeAIEmbeddings(model="models/embedding-001")

    mode = request.args.get("mode", "keyword")
    size = int(request.args.get("size", 10))

    query = request.args.get("kw")
    keyword_query = Q("match", text=query)

    if mode == "keyword":
        search = Search(index=ESPage.Index.name).query(keyword_query)
    elif mode == "semantic":
        query = request.args.get("sq")

        embedding = embedder.embed_query(query)
        search = Search(index=ESPage.Index.name).query(
            "script_score",
            query={"match_all": {}},
            script={
                "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                "params": {"query_vector": embedding},
            },
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

        search = Search(index=ESPage.Index.name).query(hybrid_query)
    else:
        return jsonify({"error": f"Unknown search mode: {mode}"}), 400

    search = search[:size]
    response = search.execute()

    results = [
        {
            "text": hit.text,
            "link": "link",
        }
        for hit in response
    ]

    return jsonify(results)
