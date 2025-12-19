from flask import Blueprint, current_app, request

# shared code
from logger import create_log

# API code
from ..utils import APIUtils
from ..elastic import ElasticClient
from ..db import DBClient
from ..auth import require_api_key
from ..decorators import require_token
from ..assistant.agent import update_chat, AssistantWorkerContext


logger = create_log(__name__)

chat_blueprint = Blueprint("chat", __name__, url_prefix="/chat")

RESPONSE_TYPE = "chat"
INDEX_NAME = "vra_chunks_gemini-embedding-001"
WORKER_CONTEXT = AssistantWorkerContext(
    index_name=INDEX_NAME
)  # Q: is there a more flask idomatic way to do this?


@chat_blueprint.route("/", methods=["POST"])
@require_api_key
@require_token
def chat(user=None):
    context = request.json.get("context")
    conversation = request.json.get("messages")
    item_id = request.json.get("itemId")

    new_items = update_chat(conversation, context, item_id=item_id)

    return {"output": new_items}


## catalogSearch
# Enrich hits with book metadata + format result
db_client.createSession()
results = []
for res in search_result.hits:
    edition_ids = [e.edition_id for e in res.meta.inner_hits.editions.hits]

    try:
        highlights = {
            key: list(set(res.meta.highlight[key])) for key in res.meta.highlight
        }
    except AttributeError:
        highlights = {}

    results.append((res.uuid, edition_ids, highlights))

if es_client.sortReversed is True:
    results = [r for r in reversed(results)]

works = db_client.fetchSearchedWorks(results)

# Depending on the version of elastic search, hits will either be an integer or a dictionary
total_hits = (
    search_result.hits.total
    if isinstance(search_result.hits.total, int)
    else search_result.hits.total.value
)

facets = APIUtils.formatAggregationResult(search_result.aggregations.to_dict())
paging = APIUtils.formatPagingOptions(params.page + 1, params.size, total_hits)

search_results = {
    "totalWorks": total_hits,
    "works": APIUtils.formatWorkOutput(
        works,
        results,
        request=None,
        dbClient=db_client,
        formats=None,
        reader=reader_version,
    ),
    "paging": paging,
    "facets": facets,
    "searchParams": params.to_query_filters(),
}

data_block = {"data": search_results, "type": "catalog_search"}

db_client.closeSession()

return json.dumps(data_block, default=json_serial_uuid)
