from datetime import datetime
from pathlib import Path
import logging

import polars as pl

from src.jobs.config import (
    LOGGING_FORMAT,
    YEAR_FILTER,
    DATASET_FOLDER,
    KM_MULTIPLIER,
    DECIMAL_PLACES
)

# Set up logging format and level
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


def create_monthly_average_trip_distance_query(dataset: pl.LazyFrame) -> pl.LazyFrame:
    """
    This function creates a Polars expression that calculates the average trip distance per month.
    Args:
        dataset_path (str): Path to the dataset.

    Returns:
        pl.LazyFrame: Polars LazyFrame expression.
    """

    selected_columns = dataset.select("tpep_pickup_datetime", "trip_distance")

    with_extra_columns = selected_columns.with_columns(
        [
            pl.col("tpep_pickup_datetime").dt.month().alias("tpep_pickup_month"),
            (pl.col("trip_distance") * KM_MULTIPLIER).round(DECIMAL_PLACES).alias("trip_distance_km"),
        ]
    )

    filtered = with_extra_columns.filter(
        pl.col("tpep_pickup_datetime").dt.year() >= YEAR_FILTER
    )

    grouped = filtered.groupby("tpep_pickup_month")

    aggregated = grouped.agg(
        pl.col("trip_distance_km").mean().round(DECIMAL_PLACES).alias("avg_trip_distance_km")
    )

    aggregated_results = aggregated.sort("tpep_pickup_month", descending=False)

    return aggregated_results


if __name__ == "__main__":

    try:
        # Lazy evaluation
        df = pl.scan_parquet(str(DATASET_FOLDER / "yellow_*.parquet"))
        query = create_monthly_average_trip_distance_query(df)

        # We only force the computation here
        results = query.collect()

        # Using lazy % formatting
        logger.info("First 5 rows of result:\n %s", results.head(5))

    except Exception as e:
        logger.error("An error occurred: %s", str(e))
