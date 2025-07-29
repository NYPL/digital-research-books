import pytest
import logging
import pandas as pd
import numpy as np
from sqlalchemy import inspect
from model.postgres.grin_status import GRINStatus
from model.postgres.grin_status import GRINState
from managers.db import DBManager


@pytest.fixture(scope="session")
def db_manager():
    with DBManager() as db_manager:
        yield db_manager
        db_manager.close_connection()
        db_manager.session.close()


@pytest.fixture(scope="session")
def df(db_manager):
    df = pd.read_sql_query("SELECT * FROM grin_statuses", con=db_manager.engine)

    return df


def test_grin_statuses_table_exists(db_manager, df):
    inspector = inspect(db_manager.engine)
    barcode_column = "barcode"
    record_id_column = "record_id"
    failed_download_column = "failed_download"
    state_column = "state"

    assert "grin_statuses" in inspector.get_table_names()
    assert not df.empty
    assert barcode_column in df.columns
    assert record_id_column in df.columns
    assert failed_download_column in df.columns
    assert state_column in df.columns


def test_grin_statuses_data_types(df):
    assert df["barcode"].dtype == "object"
    assert df["record_id"].dtype == "int64"
    assert df["failed_download"].dtype == "int64"
    assert df["state"].dtype == "object"


def test_grin_statuses_unique(df):
    df_nunique_cols_with_nan = df.nunique(dropna=False)

    assert df_nunique_cols_with_nan["barcode"] == len(df["barcode"]), (
        "Duplicate barcodes found in grin_statuses table!"
    )
    assert df["barcode"].nunique() == len(df["barcode"]), (
        "Duplicate barcodes found in grin_statuses table!"
    )

    assert df_nunique_cols_with_nan["record_id"] == len(df["record_id"]), (
        "Duplicate record_ids found in grin_statuses table!"
    )
    assert df["record_id"].nunique() == len(df["record_id"]), (
        "Duplicate record_ids found in grin_statuses table!"
    )


def test_grin_statuses_primary_keys(df, db_manager):
    inspector = inspect(db_manager.engine)
    pk_constraints = inspector.get_pk_constraint("grin_statuses")

    assert pk_constraints is not None
    assert ["barcode"] == pk_constraints["constrained_columns"]


def test_grin_statuses_foreign_keys(df, db_manager):
    inspector = inspect(db_manager.engine)
    fk_constraints = inspector.get_foreign_keys("grin_statuses")
    find_fk = False

    for fk in fk_constraints:
        if ["record_id"] == fk["constrained_columns"]:
            find_fk = True
            break
    assert find_fk, (
        "Foreign key constraint on record_id not found in grin_statuses table"
    )
    assert fk_constraints is not None
    assert len(fk_constraints) == 1
    assert fk_constraints[0]["referred_table"] == "records"
    assert fk_constraints[0]["constrained_columns"] == ["record_id"]


def test_not_null_constraints(df):
    assert df["barcode"].notnull().all()
    assert df["state"].notnull().all()
    assert df["record_id"].notnull().all()
    assert df["failed_download"].notnull().all()
