#!/usr/bin/env python

"""Tests for `bodyport` package."""
import pytest

from bodyport.config import EXAMPLE_ECG_DIR_LATEST
from bodyport.load import DataWarehouseManager
from bodyport.orm import Base, Run, Subject
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

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

def test_runs():
    """
    Each run has both a csv and a json file
    """
    pass


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

