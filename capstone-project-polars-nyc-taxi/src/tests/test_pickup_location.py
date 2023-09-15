from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from src.jobs.top_pickup_location import extract_top_pickup_locations_per_month, create_named_locations_dataframe, rank_and_filter


ROOT_FOLDER = Path(__file__).parent.parent.parent
LOCATION_NAMES = pl.read_csv(str(ROOT_FOLDER / "dataset" / "dictionaries" / "taxi_zone_lookup.csv"))


def test_create_named_locations_dataframe():
    dataset = pl.DataFrame({
        "PULocationID": [1, 2]
    })
    location_names = pl.DataFrame({
        "LocationID": [1, 2],
        "Borough": ["Manhattan", "Brooklyn"]
    })
    expected_output = pl.DataFrame({
        "PULocationID": [1, 2],
        "Borough": ["Manhattan", "Brooklyn"]
    })

    assert create_named_locations_dataframe(dataset, location_names).frame_equal(expected_output, null_equal=True)


def test_rank_and_filter():
    dataset = pl.DataFrame({
        "tpep_pickup_month": [1, 1, 2, 2],
        "Borough": ["Manhattan", "Brooklyn", "Manhattan", "Brooklyn"],
        "count": [10, 5, 15, 7]
    })
    expected_output = pl.DataFrame({
        "tpep_pickup_month": [1, 2],
        "Borough": ["Manhattan", "Manhattan"],
        "count": [10, 15],
        "rank": [1, 1]
    })

    assert rank_and_filter(dataset, 1).frame_equal(expected_output, null_equal=True)
