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


# TODO: move to agent.py
def verbose_display(edition_data, query, as_str=False):
    """
    Display edition search results with detailed information.

    Args:
        edition_data: List of tuples (orm_work, orm_edition, edition_hit)
                     where edition_hit contains work_id, edition_id, and chunk_hits
        query: The search query string
        as_str: If True, return as string; otherwise print
    """
    lines = []
    lines.append(f'QUERY: "{wrap(query)}"')
    lines.append("\n")

    for i, (orm_work, orm_edition, edition_hit) in enumerate(edition_data, 1):
        # Extract work and edition metadata
        title = orm_work.title or "(No Title)"
        authors = orm_work.authors or []
        author_names = (
            ", ".join([a.get("name", "") for a in authors if isinstance(a, dict)])
            if authors
            else "(No Authors)"
        )
        subjects = orm_work.subjects or []
        subject_list = (
            ", ".join([s.get("subject", "") for s in subjects if isinstance(s, dict)])
            if subjects
            else "(None)"
        )

        # Extract edition metadata
        pub_date = (
            str(orm_edition.publication_date)
            if orm_edition.publication_date
            else "(No Date)"
        )
        publishers = orm_edition.publishers or []
        publisher_names = (
            ", ".join([p.get("name", "") for p in publishers if isinstance(p, dict)])
            if publishers
            else "(No Publisher)"
        )

        # Get chunk hits for this edition
        chunk_hits = edition_hit.get("chunk_hits", [])
        max_score = (
            max([h.get("meta", {}).get("score", 0) for h in chunk_hits])
            if chunk_hits
            else 0
        )

        lines.append(f"EDITION {i}:")
        lines.append(f"  WORK ID: {orm_work.id} | EDITION ID: {orm_edition.id}")
        lines.append(f"  TITLE: {title}")
        lines.append(f"  AUTHORS: {author_names}")
        lines.append(f"  PUBLISHER: {publisher_names}")
        lines.append(f"  DATE: {pub_date}")
        lines.append(f"  SUBJECTS: {subject_list}")
        lines.append(f"  MAX SCORE: {max_score:.4f}")
        lines.append(f"  CHUNKS FOUND: {len(chunk_hits)}")
        lines.append("")

        # Display top chunk hits for this edition
        for j, chunk_hit in enumerate(chunk_hits[:3], 1):  # Show top 3 chunks
            text = chunk_hit.get("text", "(No Text)")
            score = chunk_hit.get("meta", {}).get("score", 0)
            chunk_id = chunk_hit.get("meta", {}).get("id", "unknown")

            # Extract page number from chunk id
            page = chunk_id.split("_")[1] if "_" in chunk_id else "?"

            lines.append(f"    CHUNK {j}:")
            lines.append(f"      ID: {chunk_id}")
            lines.append(f"      PAGE: {page}")
            lines.append(f"      SCORE: {score:.4f}")
            lines.append(
                f"      TEXT: {wrap(text)}"
            )  # TODO: I don't think this will indent all lines
            lines.append("")

        lines.append("-" * 80)

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)


def compact_display(edition_data, query, as_str=False):
    """
    Display edition search results in compact format.

    Args:
        edition_data: List of tuples (orm_work, orm_edition, edition_hit)
                     where edition_hit contains work_id, edition_id, and chunk_hits
        query: The search query string
        as_str: If True, return as string; otherwise print
    """
    lines = []
    lines.append(f'QUERY: "{wrap(query)}"')
    lines.append("RESULTS:")

    for i, (orm_work, orm_edition, edition_hit) in enumerate(edition_data, 1):
        title = orm_work.title or "(No Title)"
        chunk_hits = edition_hit.get("chunk_hits", [])
        max_score = (
            max([h.get("meta", {}).get("score", 0) for h in chunk_hits])
            if chunk_hits
            else 0
        )

        # Truncate title if too long
        title_display = title[:60] + "..." if len(title) > 60 else title

        lines.append(
            f" {i:>3}:  ({max_score:.3f}) Ed:{orm_edition.id:<6} W:{orm_work.id:<6} [{len(chunk_hits)} chunks] - {title_display}"
        )

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)


# OLD idea for search funcs
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
