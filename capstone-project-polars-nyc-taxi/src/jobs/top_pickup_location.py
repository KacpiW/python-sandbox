from pathlib import Path
import logging
import polars as pl
from polars import (
    LazyFrame,
    read_csv,
    scan_csv,
    scan_parquet,
    Config,
    col,
    Int64,
    DataFrame,
)

from src.jobs.config import ROOT_FOLDER, LOGGING_FORMAT, YEAR_FILTER, DATASET_FOLDER

LOCATION_NAMES = scan_csv(
    str(ROOT_FOLDER / "dataset" / "dictionaries" / "taxi_zone_lookup.csv")
)
TOP_N_RECORDS = 5

logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


def create_named_locations_dataframe(
    dataset: LazyFrame, location_names: LazyFrame = LOCATION_NAMES
) -> LazyFrame:
    """
    Joins location names with borough names.
    """
    return dataset.join(
        location_names, left_on="PULocationID", right_on="LocationID", how="left"
    )


def rank_and_filter(aggregated_values: LazyFrame, n: int) -> LazyFrame:
    """
    Ranks and filters the top n locations.
    """
    return (
        aggregated_values.with_columns(
            pl.col("count")
            .rank("dense", descending=True)
            .over("tpep_pickup_month")
            .alias("rank")
        )
        .filter(col("rank") <= n)
        .sort(["tpep_pickup_month", "count"], descending=[False, True])
    )


def extract_top_pickup_locations_per_month(dataset: LazyFrame, n: int) -> LazyFrame:
    """
    Extracts the top pickup locations per month.
    """
    selected_columns = dataset.select("tpep_pickup_datetime", "PULocationID")
    with_extra_columns = selected_columns.with_columns(
        col("tpep_pickup_datetime").dt.month().alias("tpep_pickup_month")
    )
    filtered = with_extra_columns.filter(
        col("tpep_pickup_datetime").dt.year() >= YEAR_FILTER
    ).with_columns(col("PULocationID").cast(Int64))
    named_locations = create_named_locations_dataframe(filtered)
    aggregated_values = named_locations.groupby(
        ["tpep_pickup_month", "Borough"]
    ).count()

    return rank_and_filter(aggregated_values, n)


def main():
    """
    Main entry point of the script.
    """
    try:
        with Config(tbl_rows=100):
            dataset = scan_parquet(str(DATASET_FOLDER / "yellow_*.parquet"))
            query = extract_top_pickup_locations_per_month(dataset, TOP_N_RECORDS)

            results = query.collect()
            logger.info("Results: %s", results.head(50))

    except Exception as e:
        logger.error("An error occurred: %s", str(e))


if __name__ == "__main__":
    main()
