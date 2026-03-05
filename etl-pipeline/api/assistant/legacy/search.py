# ES9 import here only
from elasticsearch.dsl import Search, Q, Response
# from elasticsearch9.dsl import Search, Q

from typing import Dict

from vector_indexing.embedding import GoogleEmbedder
from utils.elastic import get_or_create_default_connection
from logger import create_log


logger = create_log(__name__)


# TODO: index metadata. Indexes must be saved with metadata at least on the \
# exact embedder used, so that teh same embedder can be looked up and used for search.


# TODO: explicitly control n hits to return in Search()
# TODO: stop returning embedding with hit
# self = object.__new__(Searcher)
# embedder = GoogleEmbedder()
# index_name = "vra_chunks_gemini-embedding-001"
# MAYBE: create a "search" factory that handles some of the common code in \
# different search methods (metadata i/o, elastic search boilerplate, etc...)
# ALT name: VRACorpusSearcher
class Searcher:
    def __init__(self, index_name, embedder):
        self.index_name = index_name
        self.embedder = embedder
        get_or_create_default_connection(
            request_timeout=60 * 5  # NOTE: in ES <8 this param is `timeout`
        )  # brute vector search is slow 5 min timeout

    # query = "shipbuilding"
    # topk=50
    def vector_search(
        self, query, topk=10, filter_query: Dict | None = None, brute=False
    ) -> Response:
        query_vector = self.embedder.get_embedding(query)

        # Brute force KNN
        if brute:
            search = (
                Search(index=self.index_name)
                .query(
                    "script_score",
                    query={"match_all": {}},
                    script={
                        "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                        "params": {"query_vector": query_vector},
                    },
                )
                # .filter("term", filter_query) # metadata filter is this correct format?
                .highlight("text", fragment_size=150, number_of_fragments=3)
            )
            search = search[:topk]

        # Approximate KNN
        else:
            search = Search(
                index=self.index_name
            ).knn(
                field="embedding",  # MAYBE: get field from INDEX_CONFIG['mapping']? maybe over complicated
                k=topk,
                # NOTE: how many and which of k= hits that are returned to the \
                # ES client is constrained by the _search API pagination set in \
                # by the _search size= parameter that defaults to 10 (see: \
                # https://www.elastic.co/docs/reference/elasticsearch/rest-apis/paginate-search-results).
                num_candidates=500,
                # NOTE: best k/num_candidates param config taken from blog post XXXX ...?
                query_vector=query_vector,
                filter=filter_query,  # filter applied during search. slows query
            )
            search = search[:topk]
            # TODO: s.params(track_total_hits=True)

        # unnecessary in >=ES9.2
        # # Prevent return of memory intensive embeddings
        # search = search.source(
        #         excludes=["embedding"]
        #     )

        resp = search.execute()
        logger.info(
            f"Vector search returned {len(resp.hits)} hits, server-side duration={resp.took / 1000:.2f}s (brute={brute}, topk={topk})"
        )
        return resp

    def keyword_search(self, query, topk=10) -> Response:
        search = (
            Search(index=self.index_name)
            .query(Q("match", text=query))
            # .filter("term", record_id=record_id) # metadata filter #NOT we are not filtering by book here
            .highlight("text", fragment_size=150, number_of_fragments=3)
            .source(excludes=["embedding"])  # do not return memory intensive embeddings
        )
        search = search[:topk]
        resp = search.execute()
        logger.info(
            f"Keyword search returned {len(resp.hits)} hits in {resp.took}ms (topk={topk})"
        )
        print(f"search took: {resp.took}")
        return resp


# # OLD idea for search funcs
# # query = 'hello'
# def search(
#     query,
#     query_mode="semantic",
#     max_hits: int = 10,
# ) -> list[dict]:
#     searcher = Searcher()
#     if query_mode == "semantic":
#         resp = searcher.vector_search(query)
#         return resp

#     elif query_mode == "keyword":
#         resp = searcher.keyword_search(query)
#         return resp

#     elif query_mode == "hybrid":
#         # Hybrid: combine keyword and semantic (vector) search
#         query_vector = searcher.embedder.get_embedding(query)
#         keyword_boost = 1.0
#         semantic_boost = 2.0

#         hybrid_query = Q(
#             "bool",
#             should=[
#                 Q("match", text={"query": query, "boost": keyword_boost}),
#                 Q(
#                     "script_score",
#                     query={"match_all": {}},
#                     script={
#                         "source": f"{semantic_boost} * cosineSimilarity(params.query_vector, 'embedding') + 1.0",
#                         "params": {"query_vector": query_vector},
#                     },
#                 ),
#             ],
#         )

#         search = (
#             Search(index=searcher.index_name)
#             .query(hybrid_query)
#             .highlight("text", fragment_size=150, number_of_fragments=3)
#         )
#         resp = search.execute()
#         return resp
#     else:
#         raise Exception(f"Unknown query mode: {query_mode}")

#     # I believe that iterating a search obj implicitly executes it
#     # search_request = search_request[:max_hits]
#     # search_response = search_request.execute()

#     # search = search.highlight("text", fragment_size=150, number_of_fragments=3)

#     # highlight doesn't seem to work on vector search

#     # return [
#     #     {
#     #         "textPreview": shorten(hit.text, width=360, placeholder="..."),
#     #         "highlightedText": list(hit.meta.highlight.text)
#     #         if "highlight" in hit.meta
#     #         else None,
#     #         "readLink": f"/items/{item_id}/read/{hit.page_id}",
#     #     }
#     #     for hit in search[:max_hits]
#     # ]
