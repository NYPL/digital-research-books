import pytest
import logging
import pandas as pd
import numpy as np
from numpy import nan
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
    df = pd.read_sql_table("grin_statuses", con=db_manager.engine)

    return df


def test_grin_statuses_table_exists(db_manager, df):
    barcode_column = "barcode"
    record_id_column = "record_id"
    failed_download_column = "failed_download"
    state_column = "state"

    assert "grin_statuses" in db_manager.engine.table_names()
    assert not df.empty
    assert barcode_column in df.columns
    assert record_id_column in df.columns
    assert failed_download_column in df.columns
    assert state_column in df.columns


def test_grin_statuses_data_types(df):
    assert df["barcode"].dtype == "object"
    assert df["record_id"].dtype == "Int64"
    assert df["failed_download"].dtype == "Int64"
    assert df["state"].dtype == "object"


def test_not_null_constraints(df):
    assert df["barcode"].notnull().all()
    if df["barcode"].isnull().sum() > 0:
        logging.warning("Null values found in barcode column")

    assert df["state"].notnull().all()
    if df["state"].isnull().sum() > 0:
        logging.warning("Null values found in state column")

    assert df["record_id"].notnull().all()
    if df["record_id"].isnull().sum() > 0:
        logging.warning("Null values found in record_id column")

    assert df["failed_download"].notnull().all()
    if df["failed_download"].isnull().sum() > 0:
        logging.warning("Null values found in failed_download column")
