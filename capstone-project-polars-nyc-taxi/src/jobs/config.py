from pathlib import Path

ROOT_FOLDER = Path(__file__).parent.parent.parent
DATASET_FOLDER = Path(__file__).parent.parent.parent / "dataset"
LOGGING_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
YEAR_FILTER = 2023
KM_MULTIPLIER = 1.609344
DECIMAL_PLACES = 2
TOP_N_RECORDS = 5
