from typing import List
import logging

import polars as pl

from config import DATASET_FOLDER, YEAR_FILTER, TOP_N_RECORDS


def extract_time(dataset: pl.LazyFrame, time_types: List[str]) -> pl.LazyFrame:
    """
    Extracts specified time components from the 'tpep_pickup_datetime' column of the given LazyFrame.

    Args:
        dataset (LazyFrame): The input dataset. Must contain a column named 'tpep_pickup_datetime'.
        time_types (List[str]): List of time components to extract from the 'tpep_pickup_datetime' column.
            Valid options are 'year', 'month', 'day', 'hour', 'minute', 'second'.

    Returns:
        LazyFrame: The input dataset with additional columns for each requested time component.

    Raises:
        ValueError: If any of the requested time components is not valid (i.e., not one of 'hour', 'minute', 'second').
    """

    time_dict = {
        "hour": pl.col("tpep_pickup_datetime").dt.hour().alias("hour"),
        "minute": pl.col("tpep_pickup_datetime").dt.minute().alias("minute"),
        "second": pl.col("tpep_pickup_datetime").dt.second().alias("second"),
        "day": pl.col("tpep_pickup_datetime").dt.day().alias("day"),
        "month": pl.col("tpep_pickup_datetime").dt.month().alias("month"),
        "year": pl.col("tpep_pickup_datetime").dt.year().alias("year"),
    }

    for time_type in time_types:
        if time_type not in time_dict:
            raise ValueError(
                f"Invalid time_type argument. Expected one of: {list(time_dict.keys())}"
            )

    return dataset.with_columns(*[time_dict[time_type] for time_type in time_types])


def rank_by_column(
    dataset: pl.LazyFrame, column_name: str, over: str, n: int
) -> pl.LazyFrame:
    """
    Ranks and filters the top n locations.
    """
    return (
        dataset.with_columns(
            pl.col(column_name).rank("dense", descending=True).over(over).alias("rank")
        )
        .filter(pl.col("rank") <= n)
        .sort(["month", "count"], descending=[False, True])
    )


if __name__ == "__main__":
    try:
        with pl.Config(tbl_rows=100):
            taxi_dataset = pl.scan_parquet(
                str(DATASET_FOLDER / "yellow_*.parquet")
            ).select("tpep_pickup_datetime")

            time_taxi_dataset = extract_time(taxi_dataset, ["month", "hour"])

            filtered_dataset = time_taxi_dataset.filter(
                pl.col("tpep_pickup_datetime").dt.year() >= YEAR_FILTER
            )

            record_count_dataset = filtered_dataset.groupby(["month", "hour"]).count()

            ranked_data = rank_by_column(
                record_count_dataset, "count", "month", TOP_N_RECORDS
            )

            results = ranked_data.collect()

            print(results.head(50))

    except FileNotFoundError as file_exception:
        logging.error("There's an error %s", str(file_exception))
