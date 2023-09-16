import pytest
from datetime import datetime

import polars as pl
from src.jobs.average_distance import create_monthly_average_trip_distance_query


def test_create_monthly_average_trip_distance_query():
    """
    Test function for create_monthly_average_trip_distance_query.
    Verifies the function's output given a sample dataset.
    """

    test_set = pl.LazyFrame(
        {
            "tpep_pickup_datetime": [
                datetime(2023, 1, 1),
                datetime(2023, 2, 2),
                datetime(2023, 3, 3),
                datetime(2023, 4, 4),
            ],
            "trip_distance": [1.0, 2.0, 3.0, 4.0],
        }
    )

    results = create_monthly_average_trip_distance_query(test_set).collect()

    test_results = pl.DataFrame(
        {
            "tpep_pickup_month": [1, 2, 3, 4],
            "avg_trip_distance_km": [1.61, 3.22, 4.83, 6.44],
        }
    )

    assert results.frame_equal(test_results, null_equal=True)
