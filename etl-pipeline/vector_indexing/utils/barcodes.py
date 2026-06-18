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


# TODO: modularize this to make "TurbopufferBackend.scan()" with arbitrary queries like the one used here
def write_non_indexed_10k_barcodes(index_name: str, output_path: Path | str) -> Path:
    """Write 10k barcodes not yet indexed in the target turbopuffer namespace.

    The output file contains one barcode per line, sorted ascending.
    """
    output_path = Path(output_path)
    source_barcodes = set(list_10k_barcodes())
    missing_barcodes = set(source_barcodes)

    # limit.per returns 1 row per unique barcode value.
    # limit.total is page size for paginating the sorted query over the whole index
    # Cursor-based pagination handles datasets of any size.
    _PAGE_LIMIT = 1_000
    backend = TurbopufferBackend(index_name=index_name)
    last_barcode: str | None = None

    while missing_barcodes:
        query_kwargs: dict[str, Any] = {
            "rank_by": ("barcode", "asc"),
            "limit": {
                "total": _PAGE_LIMIT,  #
                "per": {"attributes": ["barcode"], "limit": 1},
            },
            "include_attributes": ["barcode"],
        }
        if last_barcode is not None:
            query_kwargs["filters"] = ("barcode", "Gt", last_barcode)

        result = backend.namespace.query(**query_kwargs)
        rows = result.rows
        if not rows:
            break

        last_barcode = None
        for row in rows:
            barcode = row.model_dump().get("barcode")
            last_barcode = barcode
            if barcode in missing_barcodes:
                missing_barcodes.discard(barcode)

        if last_barcode is None or len(rows) < _PAGE_LIMIT:
            break

    non_indexed_barcodes = sorted(missing_barcodes)

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
