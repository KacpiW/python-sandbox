from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year, round, col
from pathlib import PosixPath

DATASET_FOLDER = PosixPath(__file__).parent.parent / "dataset"

with SparkSession.builder.appName("AverageDistance").getOrCreate() as spark:
    try:
        # Load data
        taxi_data = (
            spark.read.format("parquet")
            .option("header", "true")
            .load(str(DATASET_FOLDER))
        )

        # Check number of records
        num_records = taxi_data.count()
        print("Number of records: ", num_records)

        # Proceed if data is non-empty
        if num_records > 0:
            # Ensure trip distance is numeric
            taxi_data = taxi_data.withColumn(
                "trip_distance", col("trip_distance").cast("float")
            )

            # Extract month from pickup datetime and convert trip distance to kilometers
            taxi_data = taxi_data.withColumn(
                "month", month("tpep_pickup_datetime")
            ).withColumn("trip_distance", col("trip_distance") * 1.609344)

            # Calculate average distance per month
            average_distance_per_month = (
                taxi_data.filter(year("tpep_pickup_datetime") >= 2023)
                .groupBy("month")
                .avg("trip_distance")
                .withColumnRenamed("avg(trip_distance)", "average_distance_km")
                .withColumn("average_distance_km", round("average_distance_km", 2))
                .orderBy("month")
            )

            # Print results
            for distance in average_distance_per_month.collect():
                print(
                    f"Month: {distance.month}, Average Distance: {distance.average_distance_km} km"
                )
                # Month: 1, Average Distance: 6.19 km
                # Month: 2, Average Distance: 6.22 km
                # Month: 3, Average Distance: 6.28 km
                # Month: 4, Average Distance: 4.73 km

        else:
            print("Data file is empty.")

    except Exception as e:
        print("An error occured: ", e)
