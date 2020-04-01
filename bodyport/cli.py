"""Console script for bodyport."""
import argparse
import sys

from bodyport.config import (
    EXAMPLE_ECG_DIR_LATEST,
    EXAMPLE_ECG_DIR_NEW,
    DB_CONN_STRING
)
from bodyport.load import DataWarehouseManager


def main():
    """Console script for bodyport."""
    parser = argparse.ArgumentParser(description="cli utilities for managing the data warehouse")
    parser.add_argument('_', nargs='*')
    args = parser.parse_args()

    if args._[0:2] == ['dw', 'demo']:
        run_demo()
    return 0


def run_demo():

    demo_db = DB_CONN_STRING + '.demo'

    print(f"""
        This demo will incrementally load the data warehouse at {demo_db}
        first with data in {EXAMPLE_ECG_DIR_LATEST}, and then
        reconciles it with new data in {EXAMPLE_ECG_DIR_NEW}
    """)

    print(f"Preparing empty database at {demo_db}")

    data_warehouse = DataWarehouseManager(db_conn_string=demo_db)
    data_warehouse.empty()

    print(f"""
        STEP 1: Load 1 directory into empty data warehouse
    """)
    print(f"Loading {EXAMPLE_ECG_DIR_LATEST}")
    print("populating `run` and `subject` tables")
    data_warehouse.load(data_dir=EXAMPLE_ECG_DIR_LATEST)

    print("Checking rows in data warehouse")
    runs = data_warehouse.pandas_query('select * from run;')
    initial_run_count = len(runs)
    print(f"{initial_run_count} run records were loaded into the data warehouse")

    subjects = data_warehouse.pandas_query('select * from subject;')
    initial_subject_count = len(subjects)
    print(f"{initial_subject_count} subject records were loaded into the data warehouse")
    assert initial_subject_count == 80

    print("""
        Step 2! Incremental Load
    """)
    print(f"Loading directory #2: {EXAMPLE_ECG_DIR_NEW} into the data warehouse")
    print("This directory has 4 runs, but 1 is a replica of previously seen subject")
    new_runs = list(EXAMPLE_ECG_DIR_NEW.glob('*/*.csv'))
    assert len(new_runs) == 4   # just making sure the files are there

    print(f"Loading directory #2: {EXAMPLE_ECG_DIR_NEW} into the data warehouse")
    data_warehouse.load(data_dir=EXAMPLE_ECG_DIR_NEW)

    print(f"Checking the database....")
    refreshed_runs = data_warehouse.pandas_query('select * from run;')
    assert len(refreshed_runs) == initial_run_count + 3

    subjects = data_warehouse.pandas_query('select * from subject;')
    assert len(subjects) == 81
    print(f"As expected, we now have 3 more run records than we had after the first load and 1 more subject.")

    print(f"""
        Step 3:
        Idempotency!
        Re-running load on an already processed directory should
        not do anything. Let's try it...
    """)
    data_warehouse.load(data_dir=EXAMPLE_ECG_DIR_NEW)
    refreshed_runs = data_warehouse.pandas_query('select * from run;')
    assert len(refreshed_runs) == initial_run_count + 3

    subjects = data_warehouse.pandas_query('select * from subject;')
    assert len(subjects) == 81
    print(f"Yes, run counts and subject counts are still the same as after the 2nd load")

    print("This concludes the demo!! You can inspect the sqlite database for yourself")


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
