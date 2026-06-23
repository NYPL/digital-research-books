"""Test ContainsAnyToken filter on the author field (unsupported operation).

author only supports Contains and ContainsAny per the search index schema.
This script tests what Turbopuffer returns when an unsupported operation is used.
"""

# Configure pythonpath and CWD
import os, sys
from pathlib import Path
from dotenv import find_dotenv

PROJ_ROOT = Path(find_dotenv("requirements.txt")).parent
sys.path.insert(0, str(PROJ_ROOT))
os.chdir(PROJ_ROOT)

from utils.load_env import load_env

from vector_indexing.components.backends.turbopuffer import TurbopufferBackend
from vector_indexing.components.embedders.google import GoogleEmbedder
from api.assistant.search import hybrid_search

ENVIRONMENT = "production"  # or "local", "qa"

load_env(f"config/.env.{ENVIRONMENT}")

RANKING_QUERY = "natural history specimens taxonomy"
# FILTER = ["author", "ContainsAnyToken", "Darwin"]
# FILTER = ["title", "ContainsAllTokens", "The life and letters of Charles Darwin : including an autobiographical chapter /"]
FILTER = [
    "title",
    "Eq",
    "The life and letters of Charles Darwin : including an autobiographical chapter /",
]


backend = TurbopufferBackend(index_name=os.environ["TURBOPUFFER_NAMESPACE"])
embedder = GoogleEmbedder()


query_vector = embedder.embed_query(RANKING_QUERY)
results = hybrid_search(
    backend=backend,
    query_vector=query_vector,
    ranking_query=RANKING_QUERY,
    top_k=5,
    filters=FILTER,
)
print(f"Results returned: {len(results)}")
for chunk, score in results[:3]:
    print(
        f"  score={score:.4f} | author={chunk.book_metadata.author} | title={chunk.book_metadata.title!r} | subject={chunk.book_metadata.subject}"
    )
