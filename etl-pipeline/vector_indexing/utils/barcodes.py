from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from vector_indexing.core.config import (
    PostgresConfig,
)
from vector_indexing.components.backends.turbopuffer import TurbopufferBackend
from model.postgres.grin_public_domain_10k import GrinPublicDomain10k


def list_10k_barcodes(start_from: str | None = None):
    """Return all barcodes from grin_public_domain_10k, sorted ascending.

    If start_from is provided, only barcodes >= start_from are returned.
    """
    engine = create_engine(PostgresConfig().connection_url)
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


def write_non_indexed_10k_barcodes(index_name: str, output_path: Path | str) -> Path:
    """Write 10k barcodes not yet indexed in the target turbopuffer namespace.

    The output file contains one barcode per line, sorted ascending.
    """
    output_path = Path(output_path)
    source_barcodes = set(list_10k_barcodes())

    backend = TurbopufferBackend(index_name=index_name)
    indexed_barcodes: set[str] = set()

    # limit.per deduplicates by barcode, yielding one row per unique barcode.
    for chunk, _ in backend.scan(
        rank_by=("barcode", "asc"),
        limit={"per": {"attributes": ["barcode"], "limit": 1}},
        include_attributes=["barcode"],
    ):
        if chunk.barcode:
            indexed_barcodes.add(chunk.barcode)

    non_indexed_barcodes = sorted(source_barcodes - indexed_barcodes)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_contents = "\n".join(non_indexed_barcodes)
    if output_contents:
        output_contents += "\n"
    output_path.write_text(output_contents)

    print(
        f"Wrote {len(non_indexed_barcodes)} non-indexed barcodes to {output_path} "
        f"for index '{index_name}'"
    )
    return output_path
