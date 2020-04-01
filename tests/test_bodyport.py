#!/usr/bin/env python

"""Tests for `bodyport` package."""
import pytest

from bodyport.config import (
    EXAMPLE_ECG_DIR_LATEST,
    EXAMPLE_ECG_DIR_NEW
)
from bodyport.load import DataWarehouseManager
from bodyport.orm import Base, Run, Subject
from sqlalchemy.orm import Session
from sqlalchemy import create_engine


@pytest.fixture("function")
def sqlite_memory_db():
    return 'sqlite:///:memory:'


@pytest.fixture("function")
def session():
    engine = create_engine('sqlite:///:memory:', echo=True)

    Base.metadata.create_all(engine)

    session = Session(bind=engine)
    return session


def test_subject_dirs_are_standard():
    """
    Sanity Check #1:
    Let's make sure subject_id's are what they appear:
    i.e. monotonically increasing from 1-80
    """

    n_expected_subjects = 80

    subject_dirs = [p.stem for p in EXAMPLE_ECG_DIR_LATEST.glob('subject_*')]

    expected_subject_dirs = [
        f"subject_{subject_id:02}"
        for subject_id
        in range(1, n_expected_subjects + 1)
    ]

    assert sorted(subject_dirs) == expected_subject_dirs


def test_orm_subject(session):
    new_subject = Subject(id=1, sex='male', birth_year=1950)
    session.add(new_subject)
    session.commit()

    result = session.query(Subject).all()
    assert len(result) == 1


def test_orm_run(session):
    sample_path = EXAMPLE_ECG_DIR_LATEST / 'subject_01' / 'run_1.csv'
    new_run = DataWarehouseManager.generate_run_from_path(run_path=sample_path)

    session.add(new_run)
    session.commit()
    saved_run = session.query(Run).first()
    assert saved_run.id == 1


def test_incremental_load_data_warehouse(sqlite_memory_db):

    data_warehouse = DataWarehouseManager(db_conn_string=sqlite_memory_db)

    data_warehouse.up()

    # load the new data_dir into the warehouse
    data_warehouse.load(data_dir=EXAMPLE_ECG_DIR_LATEST)
    runs = data_warehouse.pandas_query('select * from run;')

    initial_run_count = len(runs)

    print(f"We found {initial_run_count} runs in upload {EXAMPLE_ECG_DIR_LATEST.stem}")

    subjects = data_warehouse.pandas_query('select * from subject;')
    assert len(subjects) == 80

    # ensure all subjects have the same age.
    assert runs.groupby('subject_id')['age_at_run'].nunique().nunique() == 1

    # ensure all subjects have same sex in each run
    assert runs.groupby('subject_id')['sex'].nunique().nunique() == 1

    # new directory, EXAMPLE_ECG_DIR_NEW, contains 2 subjects with 2 runs each.
    # Subject 80 we've seen before, but Subject 81 is new in this upload
    new_runs = list(EXAMPLE_ECG_DIR_NEW.glob('*/*.csv'))
    assert len(new_runs) == 4   # just making sure the files are there

    # but only 3 of these "new runs" are unique (i.e. new)
    data_warehouse.load(data_dir=EXAMPLE_ECG_DIR_NEW)

    # we should have 3 more than we started with
    refreshed_runs = data_warehouse.pandas_query('select * from run;')
    assert len(refreshed_runs) ==  initial_run_count + 3

    # we also expect idempotency: i.e. re-running the fill command should not change the state at all
    data_warehouse.load(data_dir=EXAMPLE_ECG_DIR_NEW)

    refreshed_runs = data_warehouse.pandas_query('select * from run;')

    assert len(refreshed_runs) ==  initial_run_count + 3

    subjects = data_warehouse.pandas_query('select * from subject;')
    assert len(subjects) == 81

    data_warehouse.down()


