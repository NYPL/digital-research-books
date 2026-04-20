"""Get FRBR graph data for one or more edition IDs and write to JSON file.

This script's primary use is for getting FRBR graph data from production (by default)
or qa for use by seed_frbr_data.py, which upserts to a local Postgres DB so that
editions returned by the vector DB have the metadata needed for constructing /chat
responses, enabling VRA integration tests against a local API.

The default output path is set to the centralized test fixtures location (/tests/fixtures).

Usage:
    From etl-pipeline/:
        python dev-scripts/get_frbr_graph_per_edition.py --edition-ids 15257916 15649870
"""

import argparse
import json
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parent

# Allow direct execution via `python scripts/get_frbr_graph_per_edition.py`
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from sqlalchemy import inspect

from api.db import get_frbr_data_by_edition, get_readonly_session
from model.postgres.record import Record
from utils.load_env import load_env


DEFAULT_OUTPUT_PATH = _PROJECT_ROOT / "tests" / "fixtures" / "frbr_seed.json"


def _orm_columns_to_dict(obj) -> dict:
    mapper = inspect(obj.__class__)
    row: dict = {}
    for column_attr in mapper.column_attrs:
        # Serialize using actual table column names rather than ORM attribute names.
        # Example: Record._has_version should be emitted as has_version.
        column_name = column_attr.columns[0].name
        row[column_name] = getattr(obj, column_attr.key)
    return row


def _normalize_ids(values: list[int]) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for value in values:
        edition_id = int(value)
        if edition_id in seen:
            continue
        seen.add(edition_id)
        normalized.append(edition_id)
    return normalized


def generate_seed_data(edition_ids: list[int]) -> dict:
    """Build dict from DB rows for the given edition IDs."""
    rows = get_frbr_data_by_edition(edition_ids)
    row_by_edition_id = {int(row.Edition.id): row for row in rows}

    missing = [eid for eid in edition_ids if eid not in row_by_edition_id]
    if missing:
        raise ValueError(f"Edition IDs not found in DB: {missing}")

    works: dict[int, dict] = {}
    editions: list[dict] = []
    items: dict[int, dict] = {}
    links: dict[int, dict] = {}
    rights: dict[int, dict] = {}
    item_links: set[tuple[int, int]] = set()
    item_rights: set[tuple[int, int]] = set()
    record_ids: set[int] = set()

    for edition_id in edition_ids:
        row = row_by_edition_id[edition_id]
        work = row.Work
        edition = row.Edition

        works[int(work.id)] = _orm_columns_to_dict(work)
        editions.append(_orm_columns_to_dict(edition))

        for item in edition.items:
            item_dict = _orm_columns_to_dict(item)
            items[int(item.id)] = item_dict

            if item.record_id is not None:
                record_ids.add(int(item.record_id))

            for link in item.links:
                links[int(link.id)] = _orm_columns_to_dict(link)
                item_links.add((int(item.id), int(link.id)))

            for right in item.rights:
                rights[int(right.id)] = _orm_columns_to_dict(right)
                item_rights.add((int(item.id), int(right.id)))

    records: list[dict] = []
    if record_ids:
        Session = get_readonly_session()
        with Session() as session:
            query = session.query(Record).filter(Record.id.in_(list(record_ids)))
            for record in query.order_by(Record.id.asc()).all():
                records.append(_orm_columns_to_dict(record))

    seed_data = {
        "works": list(works.values()),
        "editions": editions,
        "records": records,
        "items": list(items.values()),
        "links": list(links.values()),
        "item_links": [
            {"item_id": item_id, "link_id": link_id}
            for item_id, link_id in sorted(item_links)
        ],
        "rights": list(rights.values()),
        "item_rights": [
            {"item_id": item_id, "rights_id": rights_id}
            for item_id, rights_id in sorted(item_rights)
        ],
    }

    return seed_data


def _merge_seed_data(existing_data: dict, new_data: dict) -> dict:
    if not existing_data:
        return new_data

    merged = {}
    
    entity_keys = ["works", "editions", "records", "items", "links", "rights"]
    for key in entity_keys:
        entities = {str(item["id"]): item for item in existing_data.get(key, [])}
        for item in new_data.get(key, []):
            entities[str(item["id"])] = item
        merged[key] = list(entities.values())

    item_links = {
        (item["item_id"], item["link_id"]): item 
        for item in existing_data.get("item_links", [])
    }
    for item in new_data.get("item_links", []):
        item_links[(item["item_id"], item["link_id"])] = item
    merged["item_links"] = list(item_links.values())

    item_rights = {
        (item["item_id"], item["rights_id"]): item 
        for item in existing_data.get("item_rights", [])
    }
    for item in new_data.get("item_rights", []):
        item_rights[(item["item_id"], item["rights_id"])] = item
    merged["item_rights"] = list(item_rights.values())

    return merged


def _write_seed_data(output_path: Path, seed_data: dict) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(seed_data, fh, indent=2, default=str)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Get FRBR data and store in JSON for seed_frbr_data.py"
    )
    parser.add_argument(
        "--edition-ids",
        type=int,
        nargs="+",
        required=True,
        help="Space-separated edition IDs to export",
    )
    parser.add_argument(
        "--env",
        default="production",
        help=("Environment name to load from config/.env.<env> (default: production)"),
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output path for generated seed data",
    )
    parsed_args = parser.parse_args()

    env_path = _PROJECT_ROOT / "config" / f".env.{parsed_args.env}"
    load_env(env_path, raise_if_no_file=True)

    edition_ids = _normalize_ids(parsed_args.edition_ids)
    new_data = generate_seed_data(edition_ids)
    output_path = Path(parsed_args.output).expanduser()
    if not output_path.is_absolute():
        output_path = _PROJECT_ROOT / output_path

    existing_data = {}
    if output_path.exists():
        with open(output_path, "r", encoding="utf-8") as fh:
            try:
                existing_data = json.load(fh)
            except json.JSONDecodeError:
                existing_data = {}

    final_data = _merge_seed_data(existing_data, new_data)
    _write_seed_data(output_path, final_data)

    print(f"Wrote to seed file: {output_path}")
    print(
        "Counts: "
        f"works={len(final_data['works'])}, "
        f"editions={len(final_data['editions'])}, "
        f"records={len(final_data['records'])}, "
        f"items={len(final_data['items'])}, "
        f"links={len(final_data['links'])}, "
        f"item_links={len(final_data['item_links'])}, "
        f"rights={len(final_data['rights'])}, "
        f"item_rights={len(final_data['item_rights'])}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
