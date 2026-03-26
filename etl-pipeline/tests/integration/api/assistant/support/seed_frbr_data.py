"""Seed a local Postgres DB with FRBR graph data (../seed_data/frbr_seed.json).

Upsert order (respects FK dependencies):
    works -> editions -> records -> items -> links -> item_links -> rights -> item_rights

Defaults to docker-compose env config but can be used for any local DB with the
appropriate .env config file. See usage instructions below.

Depended on for running VRA integration tests against a local DRB API instance,
which require specific FRBR data to be present for /chat requests to succeed.

Dockerized DB usage:
    From etl-pipeline/:
        # Run dockerized local development setup
        docker compose run --rm --entrypoint python devsetup main.py \
            -e docker-compose \
            -p LocalDevelopmentSetupProcess

        # Run seeding script
        docker compose run --rm --entrypoint python devsetup \
            -m tests.integration.api.assistant.support.seed_frbr_data

Local DB usage:
    From etl-pipeline/ after running local development setup:
        python -m tests.integration.api.assistant.support.seed_frbr_data -e local
"""

import argparse
import json
import os
import sys
from pathlib import Path

from sqlalchemy.dialects.postgresql import insert as pg_insert
from managers.db import DBManager
from model.postgres.edition import Edition
from model.postgres.item import Item, ITEM_LINKS, ITEM_RIGHTS
from model.postgres.link import Link
from model.postgres.record import Record
from model.postgres.rights import Rights
from model.postgres.work import Work
from utils.load_env import load_env


SEED_FILE_PATH = Path(__file__).parent.parent / "seed_data" / "frbr_seed.json"
SUPPORTED_ENVS = ("local", "docker-compose")


def _assert_safe_db_target() -> None:
    """Abort unless target DB is explicitly local/docker-compose test infra.
    Prevents accidental seeding of non-local databases
    """
    host = (os.getenv("POSTGRES_HOST") or "").strip().lower()
    allowed_hosts = {"localhost", "127.0.0.1", "drb_local_db"}

    if host not in allowed_hosts:
        raise SystemExit("Non-local database host targeted for seeding")


def _normalize_row_for_table(
    row: dict, column_names: set[str]
) -> tuple[dict, set[str]]:
    """Map seed_data keys to real table columns and collect unknown keys."""
    normalized: dict = {}
    dropped: set[str] = set()

    for key, value in row.items():
        target_key = key

        # Support seed_data keys that mirror ORM private attrs (e.g. _has_version)
        if target_key not in column_names and target_key.startswith("_"):
            alias = target_key[1:]
            if alias in column_names:
                target_key = alias

        if target_key in column_names:
            normalized[target_key] = value
        else:
            dropped.add(key)

    return normalized, dropped


def _upsert(session, table, rows: list[dict], conflict_cols: list[str]) -> int:
    if not rows:
        return 0
    column_names = {c.name for c in table.columns}
    cleaned_rows: list[dict] = []
    dropped_cols: set[str] = set()

    for row in rows:
        normalized_row, dropped = _normalize_row_for_table(row, column_names)
        if normalized_row:
            cleaned_rows.append(normalized_row)
        dropped_cols.update(dropped)

    if dropped_cols:
        print(f"Ignoring unknown columns for {table.name}: {sorted(dropped_cols)}")

    if not cleaned_rows:
        return 0

    stmt = pg_insert(table).values(cleaned_rows)
    update_cols = {
        c.name: stmt.excluded[c.name]
        for c in table.columns
        if c.name not in conflict_cols
    }
    if update_cols:
        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_cols,
            set_=update_cols,
        )
    else:
        stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
    result = session.execute(stmt)
    return result.rowcount


def seed(seed_data: dict, db_manager) -> dict[str, int]:
    """Upsert all seed_data rows into the DB."""
    counts: dict[str, int] = {}
    session = db_manager.session

    # Derive IDs and scoped rows from the seed_data
    edition_rows = seed_data.get("editions", [])
    edition_ids = {e["id"] for e in edition_rows}
    work_ids = {e["work_id"] for e in edition_rows}
    item_rows = [
        i for i in seed_data.get("items", []) if i.get("edition_id") in edition_ids
    ]
    item_ids = {i["id"] for i in item_rows}

    # Upsert core FRBR entities first to satisfy FK dependencies
    work_rows = [w for w in seed_data.get("works", []) if w["id"] in work_ids]
    counts["works"] = _upsert(session, Work.__table__, work_rows, ["id"])
    counts["editions"] = _upsert(session, Edition.__table__, edition_rows, ["id"])

    # Upsert records and items tied to the selected editions
    record_ids_needed = {
        i["record_id"] for i in item_rows if i.get("record_id") is not None
    }
    record_rows = [
        r for r in seed_data.get("records", []) if r.get("id") in record_ids_needed
    ]
    counts["records"] = _upsert(session, Record.__table__, record_rows, ["id"])
    counts["items"] = _upsert(session, Item.__table__, item_rows, ["id"])

    # Upsert links referenced by the selected items
    link_ids_needed = {
        il["link_id"]
        for il in seed_data.get("item_links", [])
        if il["item_id"] in item_ids
    }
    link_rows = [l for l in seed_data.get("links", []) if l["id"] in link_ids_needed]
    counts["links"] = _upsert(session, Link.__table__, link_rows, ["id"])

    # Insert item-link join rows (ignore duplicates)
    item_link_rows = [
        il for il in seed_data.get("item_links", []) if il["item_id"] in item_ids
    ]
    if item_link_rows:
        stmt = pg_insert(ITEM_LINKS).values(item_link_rows)
        stmt = stmt.on_conflict_do_nothing()
        result = session.execute(stmt)
        counts["item_links"] = result.rowcount
    else:
        counts["item_links"] = 0

    # Upsert rights referenced by the selected items
    rights_ids_needed = {
        ir["rights_id"]
        for ir in seed_data.get("item_rights", [])
        if ir["item_id"] in item_ids
    }
    rights_rows = [
        r for r in seed_data.get("rights", []) if r["id"] in rights_ids_needed
    ]
    counts["rights"] = _upsert(session, Rights.__table__, rights_rows, ["id"])

    # Insert item-rights join rows (ignore duplicates)
    item_right_rows = [
        ir for ir in seed_data.get("item_rights", []) if ir["item_id"] in item_ids
    ]
    if item_right_rows:
        stmt = pg_insert(ITEM_RIGHTS).values(item_right_rows)
        stmt = stmt.on_conflict_do_nothing()
        result = session.execute(stmt)
        counts["item_rights"] = result.rowcount
    else:
        counts["item_rights"] = 0

    # Commit all staged upserts/inserts
    session.commit()
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Seed FRBR data into local/docker-compose Postgres."
    )
    parser.add_argument(
        "-e",
        "--env",
        choices=SUPPORTED_ENVS,
        default="docker-compose",
        help="Environment config to load (default: docker-compose)",
    )
    args = parser.parse_args()

    # Initialize environment variables and logger configuration
    env_file = f".env.{args.env}"
    load_env(Path("config") / env_file, raise_if_no_file=True)
    _assert_safe_db_target()

    # Load seeding payload
    with SEED_FILE_PATH.open("r", encoding="utf-8") as fh:
        seed_data = json.load(fh)

    # Execute DB upserts
    with DBManager() as db_manager:
        counts = seed(seed_data, db_manager)

    # Print summary of seeded data by edition
    edition_rows = seed_data.get("editions", [])
    edition_ids = sorted({e["id"] for e in edition_rows})
    item_rows = [
        i for i in seed_data.get("items", []) if i.get("edition_id") in set(edition_ids)
    ]
    print(f"FRBR seed complete for {len(edition_ids)} edition(s): {edition_ids}")
    for edition_id in edition_ids:
        edition_items = [i for i in item_rows if i["edition_id"] == edition_id]
        edition_record_ids = {
            i["record_id"] for i in edition_items if i.get("record_id") is not None
        }
        print(
            f"Edition {edition_id} seeded: items={len(edition_items)} "
            f"records={len(edition_record_ids)}"
        )

    print(f"Rows affected: {counts}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
