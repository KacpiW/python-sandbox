from pathlib import PosixPath

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    ceil,
    col,
    concat_ws,
    desc,
    hour,
    minute,
    month,
    row_number,
)
from pyspark.sql.window import Window

DATASET_FOLDER = PosixPath(__file__).parent.parent / "dataset"
N_RECORDS = 5

# Initialize SparkSession
with SparkSession.builder.appName("PeakTime").getOrCreate() as spark:
    # Load dataset from parquet file
    taxi_data = spark.read.parquet(str(DATASET_FOLDER))

    if taxi_data.rdd.isEmpty():
        print("Data file is empty.")
    else:
        try:
            # Add hour_minute and month columns
            taxi_data_hour_minute = taxi_data.withColumn(
                "hour_minute",
                concat_ws(
                    ":",
                    hour(col("tpep_pickup_datetime")),
                    ceil(minute(col("tpep_pickup_datetime")) / 30) * 30,
                ),
            ).withColumn("month", month(col("tpep_pickup_datetime")))

            # Filter records by date range and select required columns
            filtered_taxi_data_hour_minute = (
                taxi_data_hour_minute.filter(
                    col("tpep_pickup_datetime").between("2023-01-01", "2023-12-31")
                ).select("month", "hour_minute")
            )

            # Window specification for row_number operation
            window_spec = Window.partitionBy("month").orderBy(desc("count"))

            # Group data by month and hour_minute, add rank column
            results = (
                filtered_taxi_data_hour_minute.groupBy("month", "hour_minute")
                .count()
                .withColumn("rank", row_number().over(window_spec))
                .filter(col("rank") <= N_RECORDS)
                .orderBy("month", "rank")
            )

            for result in results.collect():
                print(
                    f"Month {result['month']} | Hour {result['hour_minute']} "
                    f"| Count {result['count']} | Rank {result['rank']}"
                )

        except Exception as e:
            print("An error occured: ", e)
