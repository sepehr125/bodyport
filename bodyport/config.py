from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent

PARENT_DATA_DIR = PROJECT_DIR / 'data'

EXAMPLE_ECG_DIR = PARENT_DATA_DIR / 'incoming' / 'clinic=sf_state' / 'measurement=ecg'

EXAMPLE_ECG_DIR_LATEST = EXAMPLE_ECG_DIR / '2020-01-01'

EXAMPLE_ECG_DIR_NEW = EXAMPLE_ECG_DIR / '2020-12-01'

DB_PATH = PROJECT_DIR / 'data_warehouse.db'

DB_CONN_STRING = f"sqlite:///{DB_PATH}"
