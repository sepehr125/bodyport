import hashlib
import json
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict

import pandas as pd

from bodyport.orm import Base, Subject, Run, create_session


class DataWarehouseManager:
    """
    Diff given data_dir with what is in the Data Warehouse
    and only process runs that have not yet been processed

    """

    def __init__(self, db_conn_string=None):
        self.data_dir = None
        self.current_time = None

        self.db_session = create_session(db_conn_string=db_conn_string)

        # get path to database
        self.sqlite_path = Path(self.db_session.bind.url.database)

    ###################
    # DB ADMIN METHODS
    ###################

    def down(self):
        # using sqlite, so just delete the file
        self.sqlite_path.unlink(missing_ok=True)

    def up(self):
        engine = self.db_session.bind
        Base.metadata.create_all(engine)

    def empty(self):
        self.down()
        self.up()

    def load(self, data_dir: Path):
        """
        process the given data directory,
        creating records for runs and their corresponding subjects.

        NB: Aside from the subject number in the path, there isn't anything
        else about the subject except what is in run metadata.

        So we populate all run metadata first, then update the subject table if needed.

        This way we can be sure to also catch if a subject_id is reused but the age/sex etc dramatically
        """
        # we'll timestamp all new records with the same time timestamp
        current_time = datetime.now()

        self.data_dir = data_dir

        self.update_runs(timestamp=current_time)
        self.update_subjects(timestamp=current_time)

    ##############
    # DB reconciliation and inserts
    ##############
    def update_runs(self, timestamp: datetime):
        """

        :param timestamp:
        :return:
        """
        for run_path in self.find_run_paths():
            maybe_new_run = self.generate_run_from_path(run_path)

            if not self.run_exists_in_db(maybe_new_run):
                maybe_new_run.created_at = timestamp
                self.db_session.add(maybe_new_run)
                self.db_session.commit()

    def update_subjects(self, timestamp: datetime):

        # crawl subject_id's for all runs
        maybe_new_subjects = self.db_session.query(
            Run.subject_id,
            Run.age_at_run,
            Run.sex,
            Run.date
        ).distinct()

        for subject in maybe_new_subjects:

            if not self.subject_exists_in_db(subject_id=subject.subject_id):
                new_subject = Subject(
                    id=subject.subject_id,
                    sex=subject.sex,
                    birth_year=subject.date.year - subject.age_at_run,
                    created_at=timestamp
                )
                self.db_session.add(new_subject)
                self.db_session.commit()

    ##############################
    # Filesystem Crawler Methods
    ##############################
    def find_run_paths(self) -> List[Path]:
        """
        Fetch all CSV paths in self.data_dir

        NB: this is really a "crawler" that should be refactored into its own class
        """
        return self.data_dir.glob('*/run_*.csv')

    @classmethod
    def get_run_csv_path(cls, run_path: Path) -> Path:
        """Whether the original path was CSV or JSON, this will return the CSV path"""
        file_name = f"run_{cls.parse_run_number(run_path)}.csv"
        return run_path.with_name(file_name)

    @classmethod
    def get_run_json_path(cls, run_path: Path) -> Path:
        """Whether the given path was CSV or JSON, this will return the JSON path"""
        file_name = f"run_{cls.parse_run_number(run_path)}_header.json"
        return run_path.with_name(file_name)

    #################################
    # DB lookup methods
    #################################

    def pandas_query(self, query):
        return pd.read_sql(query, con=self.db_session.bind)

    def run_exists_in_db(self, run: Run) -> bool:
        """
        How do we identify a unique run?

        We can consider subject_id x run_numbers to be a unique combination, but there is no guarantee
        that the clinic will keep track of run_numbers, or even subject numbers for that matter.

        The safest way is to create a "signature" of the raw data itself and check new files
        against it. The data sizes are small enough right now that this is a kind of safety
        we can afford to ensure data integrity. We may have to compromise later with different
        "identity resolution" techniques that scale better to large datasets.

        :param run:
        :return:
        """
        result = self.db_session.query(Run).filter_by(subject_id=run.subject_id, run_hash=run.run_hash)
        return result.count() > 0

    def subject_exists_in_db(self, subject_id: int):
        result = self.db_session.query(Subject).filter_by(id=subject_id)
        return result.count() > 0

    @classmethod
    def open_meta(cls, run_path: Path):
        meta_path = cls.get_run_json_path(run_path)
        with open(meta_path.as_posix(), 'r') as f:
            return json.load(f)

    @classmethod
    def generate_run_from_path(cls, run_path: Path):
        assert run_path.is_file(), f"File {run_path} does not exist"
        assert 'subject' in run_path.parent.stem

        meta = cls.open_meta(run_path)

        return Run(
            subject_id=cls.parse_subject_id(run_path),
            number=cls.parse_run_number(run_path),
            clinic_id=cls.parse_clinic_id(run_path),
            measurement=cls.parse_measurement(run_path),
            raw_path=run_path.as_posix(),
            meta_path=cls.get_run_json_path(run_path).as_posix(),
            date=cls.parse_date(meta),
            units=meta['units'],
            fs=meta['fs'],
            age_at_run=meta['age'],
            sex=meta['sex'],
            run_hash=cls.generate_hash_from_raw(run_path)
        )

    @classmethod
    def parse_date(cls, run_meta: Dict) -> date:
        return datetime.strptime(run_meta['date'], '%d.%m.%Y').date()

    @classmethod
    def parse_birth_year(cls, run_meta: Dict) -> int:
        age_at_run_date = int(run_meta['age'])
        run_date = cls.parse_date(run_meta)
        birth_year = run_date.year - age_at_run_date
        return birth_year

    @staticmethod
    def parse_run_number(run_path: Path) -> int:
        return int(run_path.stem.rstrip('_header').lstrip('run_'))

    @staticmethod
    def parse_clinic_id(run_path: Path) -> str:
        prefix = 'clinic='
        part: str = [part for part in run_path.parts if part.startswith(prefix)][0]
        return part.lstrip(prefix)

    @staticmethod
    def parse_measurement(run_path: Path) -> str:
        prefix = 'measurement='
        part: str = [part for part in run_path.parts if part.startswith(prefix)][0]
        return part.lstrip(prefix)

    @staticmethod
    def parse_subject_id(run_path: Path) -> int:
        return int(run_path.parent.stem.lstrip('subject_'))

    @staticmethod
    def generate_hash_from_raw(run_path: Path) -> str:
        with open(run_path.as_posix(), 'r') as f:
            contents = f.read().encode('utf-8')
        return hashlib.md5(contents).hexdigest()
