from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, row_number, col, desc, broadcast
from pyspark.sql.window import Window
from pathlib import PosixPath


DATASET_FOLDER = PosixPath(__file__).parent.parent / "dataset"


with SparkSession.builder.appName("PickupLocations").getOrCreate() as spark:
    try:
        # Load data
        taxi_data = spark.read.parquet(str(DATASET_FOLDER))

        # Proceed if data is non-empty
        if taxi_data.rdd.isEmpty():
            print("Data file is empty.")
        else:
            # Extract month from pickup location
            taxi_data = taxi_data.withColumn("month", month("tpep_pickup_datetime"))

            # Get amount of pickup per location per month
            pickup_per_month = (
                taxi_data.filter(year("tpep_pickup_datetime") == 2023)
                .groupBy("month", "PULocationID")
                .count()
                .withColumnRenamed("count", "PickupLocationCount")
            )

            # Get 5 most popular pickup location per month
            window = Window.partitionBy("month").orderBy(desc("PickupLocationCount"))

            top_five_pickup = (
                pickup_per_month.withColumn("rank", row_number().over(window))
                .filter(col("rank") <= 5)
                .drop("rank")
            )

            # Load pickup location names
            location_names = (
                spark.read.option("header", "true")
                .csv(str(DATASET_FOLDER / "dictionaries"))
                .select("LocationID", "Zone")
            )

            # Join top pickup locations with zone names
            top_five_pickup_with_name = top_five_pickup.join(
                broadcast(location_names),
                top_five_pickup.PULocationID == location_names.LocationID,
                how="left",
            ).select("month", "Zone", "PickupLocationCount")

            for results in top_five_pickup_with_name.collect():
                print(
                    f"Month: {results.month}, Zone: {results.Zone}, "
                    f"Pickup Amount: {results.PickupLocationCount}"
                )

                # Month: 1, Zone: JFK Airport, Pickup Amount: 160025
                # Month: 1, Zone: Upper East Side South, Pickup Amount: 148074
                # Month: 1, Zone: Upper East Side North, Pickup Amount: 138391
                # Month: 1, Zone: Midtown Center, Pickup Amount: 135415
                # Month: 1, Zone: Penn Station/Madison Sq West, Pickup Amount: 109226
                # Month: 2, Zone: Midtown Center, Pickup Amount: 135334
                # Month: 2, Zone: Upper East Side South, Pickup Amount: 134946
                # Month: 2, Zone: JFK Airport, Pickup Amount: 132098
                # Month: 2, Zone: Upper East Side North, Pickup Amount: 126256
                # Month: 2, Zone: Penn Station/Madison Sq West, Pickup Amount: 104574
                # Month: 3, Zone: JFK Airport, Pickup Amount: 169830
                # Month: 3, Zone: Midtown Center, Pickup Amount: 160945
                # Month: 3, Zone: Upper East Side South, Pickup Amount: 153935
                # Month: 3, Zone: Upper East Side North, Pickup Amount: 139496
                # Month: 3, Zone: Midtown East, Pickup Amount: 125221
                # Month: 4, Zone: Lincoln Square East, Pickup Amount: 13
                # Month: 4, Zone: Times Sq/Theatre District, Pickup Amount: 6
                # Month: 4, Zone: Upper East Side North, Pickup Amount: 5
                # Month: 4, Zone: Central Park, Pickup Amount: 4
                # Month: 4, Zone: Midtown East, Pickup Amount: 4

    except Exception as e:
        print("An error occurred:", e)
