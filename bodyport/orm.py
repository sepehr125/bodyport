import json
from typing import Dict

import pandas as pd
from sqlalchemy import Column, Integer, String, Date, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from bodyport.config import DB_CONN_STRING

Base = declarative_base()


def get_engine():
    return create_engine(DB_CONN_STRING)


def create_session() -> Session:
    return Session(bind=get_engine())


##############




class Subject(Base):
    """
    Represents a Subject in the Data Warehouse
    """

    __tablename__ = 'subject'

    id = Column(Integer, primary_key=True)
    sex = Column(String)
    birth_year = Column(Integer)

    def __repr__(self):
        return f"<Subject<id={self.id}>"


class Run(Base):

    """
    Represents a Run in the Data Warehouse
    """

    __tablename__ = 'run'

    # the run number isn't really an identifier of the run, because different subjects have the same run
    # the correct primary_key here would be a "subject_number-run_number". But for now we won't formalize
    # this constraint due to the fact that this is a toy example.
    id = Column(Integer, primary_key=True)
    subject_id = Column(Integer)
    number = Column(Integer)
    clinic_id = Column(String)
    measurement = Column(String)
    date = Column(Date)
    units = Column(String)
    fs = Column(Integer)
    raw_path = Column(String)
    meta_path = Column(String)
    processed_path = Column(String)
    processed_at = Column(DateTime)
    age_at_run = Column(Integer)
    sex = Column(String)
    run_hash = Column(String)

    def __repr__(self):
        return f"Run<subject_id={self.subject_id}, number: {self.number}, date: {self.date}>"

    @property
    def meta(self) -> Dict:
        with open(self.meta_path, 'r') as f:
            return json.load(f)

    @property
    def raw(self) -> pd.DataFrame:
        return pd.read_csv(self.raw_path)

