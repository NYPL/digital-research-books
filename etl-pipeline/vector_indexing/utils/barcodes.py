from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from vector_indexing.core.config import (
    get_config,
)
from vector_indexing.components.backends.turbopuffer import TurbopufferBackend
from model.postgres.grin_public_domain_10k import GrinPublicDomain10k


def list_10k_barcodes(start_from: str | None = None):
    """Return all barcodes from grin_public_domain_10k, sorted ascending.

    If start_from is provided, only barcodes >= start_from are returned.
    """
    config = get_config()
    engine = create_engine(config.pg_connection_url)
    with Session(engine) as db_session:
        query = select(GrinPublicDomain10k.barcode).order_by(
            GrinPublicDomain10k.barcode
        )
        if start_from is not None:
            query = query.where(GrinPublicDomain10k.barcode >= start_from)
        rows = db_session.execute(query).scalars().all()
    barcodes = list(rows)
    print(f"Fetched {len(barcodes)} barcodes from grin_public_domain_10k")
    return barcodes


# TODO: query limit 1 per barcode and get the batch_size-th largest barcode to \
# accommodate that last successful batch given fail-fast indexing
def get_last_indexed_barcode(index_name: str) -> str | None:
    """Return the lexicographically largest barcode already indexed in the given
    turbopuffer namespace, matching the ascending sort order used in list_10k_barcodes.
    Returns None if the namespace is empty or has no indexed documents.
    """
    backend = TurbopufferBackend(index_name=index_name)
    results = backend.query(
        rank_by=("barcode", "desc"),
        top_k=1,
        include_attributes=["barcode"],
    )
    if not results:
        return None
    chunk, _ = results[0]
    return chunk.barcode
