from datetime import datetime
from pathlib import PosixPath
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, broadcast, max, min
from pyspark.sql.types import FloatType, TimestampType

# Set up constants
DATASET_FOLDER = PosixPath(__file__).parent.parent / "dataset"

# Set up logging format and level
logging_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=logging_format)
logger = logging.getLogger(__name__)


def calculate_travel_time(start: TimestampType, end: TimestampType) -> int:
    """Calculate travel time between pickup and dropoff locations."""
    start_travel = datetime.strptime(str(start), "%Y-%m-%d %H:%M:%S")
    end_travel = datetime.strptime(str(end), "%Y-%m-%d %H:%M:%S")
    delta = (end_travel - start_travel).total_seconds()
    return delta


udf_calculate_travel_time = udf(calculate_travel_time, FloatType())

# Start SparkSession
with SparkSession.builder.appName("Taxi Efficiency").getOrCreate() as spark:
    try:
        # Load data
        taxi_data = spark.read.parquet(
            str(DATASET_FOLDER / "yellow_tripdata_january.parquet")
        )
        taxi_zone = (
            spark.read.option("header", "true")
            .csv(str(DATASET_FOLDER / "dictionaries" / "taxi_zone_lookup.csv"))
            .select("LocationID", "Borough")
        )

        # Proceed if data is non-empty
        if taxi_data.count() == 0:
            logger.error("Data file is empty.")
        else:
            # Calculate travel time and convert trip distance to meters
            taxi_data_with_time = (
                taxi_data.withColumn(
                    "travel_time",
                    udf_calculate_travel_time(
                        taxi_data.tpep_pickup_datetime, taxi_data.tpep_dropoff_datetime
                    ),
                )
                .withColumn("trip_distance", taxi_data.trip_distance * 1609.34)
            )

            # Calculate trip speed
            taxi_data_with_speed = taxi_data_with_time.withColumn(
                "trip_speed",
                taxi_data_with_time.trip_distance / taxi_data_with_time.travel_time,
            )

            # Calculate efficiency
            taxi_data_with_efficiency = taxi_data_with_speed.withColumn(
                "efficiency",
                taxi_data_with_speed.trip_distance / taxi_data_with_speed.fare_amount,
            )

            # Most and least efficient trip among those who paid > 0
            taxi_data_with_efficiency.filter(col("fare_amount") > 0).select(
                max(taxi_data_with_efficiency.efficiency).alias("max_efficiency"),
                min(taxi_data_with_efficiency.efficiency).alias("min_efficiency"),
            ).show()

            # Join data with taxi zone data for more location details
            joined_taxi_zones = (
                taxi_data_with_efficiency.join(
                    broadcast(taxi_zone),
                    taxi_data_with_efficiency.PULocationID == taxi_zone.LocationID,
                    how="left",
                )
                .withColumnRenamed("Borough", "PickupLocation")
                .drop("LocationID")
                .join(
                    broadcast(taxi_zone),
                    taxi_data_with_efficiency.DOLocationID == taxi_zone.LocationID,
                    how="left",
                )
                .withColumnRenamed("Borough", "DropoffLocation")
            )

            # Further analysis or visualization could be done here, such as
            # visualizing trip speed and efficiency, or creating a map of
            # pickup locations

    except Exception as e:
        logger.error("Error while loading data", exc_info=e)
