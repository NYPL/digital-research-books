try:
    from elasticsearch_dsl import Search, Q
except ModuleNotFoundError:
    from elasticsearch.dsl import Search, Q
# from elasticsearch9.dsl import Search, Q

from typing import Dict

from vector_indexing.embedding import GoogleEmbedder
from utils.elastic import get_or_create_default_connection
from utils.utils import wrap


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
            timeout=60 * 5
        )  # brute vector search is slow 5 min timeout

    # query = "shipbuilding"
    # topk=50
    def vector_search(
        self, query, topk=10, filter_query: Dict | None = None, brute=False
    ):
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

        # unnecessary in >=ES9.2
        # # Prevent return of memory intensive embeddings
        # search = search.source(
        #         excludes=["embedding"]
        #     )

        resp = search.execute()
        print(f"Server-side search took: {resp.took / 1000} (seconds)")
        return search

    def keyword_search(self, query, topk=10):
        search = (
            Search(index=self.index_name)
            .query(Q("match", text=query))
            # .filter("term", record_id=record_id) # metadata filter #NOT we are not filtering by book here
            .highlight("text", fragment_size=150, number_of_fragments=3)
            .source(excludes=["embedding"])  # do not return memory intensive embeddings
        )
        search = search[:topk]
        resp = search.execute()
        print(f"search took: {resp.took}")
        return search


def get_book_metadata(record_ids):
    barcode_data = read_barcode_data()

    hit_data = []
    for hit in hits:
        barcode = int(hit.meta.id.split("_")[0])
        extra = barcode_data.query("barcode == @barcode").squeeze().to_dict()
        enriched_hit = {**hit.to_dict(), **{"meta": hit.meta.to_dict()}, **extra}

        hit_data.append(enriched_hit)

    return hit_data


def get_score(entry):
    return entry.get("meta", {}).get("score", float("-inf"))


# TODO: add dummy page number (p1) for book summary index chunks....


def verbose_display(entries, query, as_str=False):
    # Sort entries by ['meta']['score'] descending, missing scores last
    sorted_entries = sorted(entries, key=get_score, reverse=True)

    lines = []
    lines.append(f'QUERY: "{wrap(query)}"')
    lines.append("\n")

    for i, entry in enumerate(sorted_entries, 1):
        title = entry.get("title", "(No Title)")
        text = entry.get("text", "(No Text)")
        subjects = entry.get("subjects", "(None)")
        dates = entry.get("dates", "(None)")
        score = get_score(entry)

        # chunk id as page num for chunks and dummy page for summaries
        page = entry["meta"]["id"].split("_")[1] if "_" in entry["meta"]["id"] else 1

        lines.append(f"RESULT {i}:")
        lines.append(f"  ID: {entry['meta']['id']}")
        lines.append(f"  TITLE: {title}")
        lines.append(f"  PAGE: {page}")
        lines.append(f"  SUBJECTS: {subjects}")
        lines.append(f"  DATES: {dates}")
        lines.append(f"  SCORE: {score}")
        lines.append("  TEXT:")
        lines.append(f"{wrap(text)}\n")
        lines.append("-" * 60)

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)


def compact_display(entries, query, as_str=False):
    # Sort entries by ['meta']['score'] descending, missing scores last
    sorted_entries = sorted(entries, key=get_score, reverse=True)

    lines = []
    lines.append(f'QUERY: "{wrap(query)}"')
    lines.append("RESULTS:")
    for i, entry in enumerate(sorted_entries, 1):
        lines.append(
            f" {i:>3}:  ({get_score(entry):.3f}) {entry['meta']['id']:<19} -  {entry['title']}"
        )

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)


# query = 'hello'
def search(
    query,
    query_mode="semantic",
    max_hits: int = 10,
) -> list[dict]:
    searcher = Searcher()
    if query_mode == "semantic":
        resp = searcher.vector_search(query)
        return resp

    elif query_mode == "keyword":
        resp = searcher.keyword_search(query)
        return resp

    elif query_mode == "hybrid":
        # Hybrid: combine keyword and semantic (vector) search
        query_vector = searcher.embedder.get_embedding(query)
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
                        "params": {"query_vector": query_vector},
                    },
                ),
            ],
        )

        search = (
            Search(index=searcher.index_name)
            .query(hybrid_query)
            .highlight("text", fragment_size=150, number_of_fragments=3)
        )
        resp = search.execute()
        return resp
    else:
        raise Exception(f"Unknown query mode: {query_mode}")

    # I believe that iterating a search obj implicitly executes it
    # search_request = search_request[:max_hits]
    # search_response = search_request.execute()

    # search = search.highlight("text", fragment_size=150, number_of_fragments=3)

    # highlight doesn't seem to work on vector search

    # return [
    #     {
    #         "textPreview": shorten(hit.text, width=360, placeholder="..."),
    #         "highlightedText": list(hit.meta.highlight.text)
    #         if "highlight" in hit.meta
    #         else None,
    #         "readLink": f"/items/{item_id}/read/{hit.page_id}",
    #     }
    #     for hit in search[:max_hits]
    # ]
