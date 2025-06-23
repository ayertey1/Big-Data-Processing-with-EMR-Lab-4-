from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark
spark = SparkSession.builder.appName("VehicleLocationMetrics").getOrCreate()

# Load raw data
vehicles = spark.read.option("header", True).csv("s3://car-rental-datastore/raw/vehicles/")
locations = spark.read.option("header", True).csv("s3://car-rental-datastore/raw/locations/")
transactions = spark.read.option("header", True).csv("s3://car-rental-datastore/raw/rental_transactions/")

# Cast numeric fields
transactions = transactions.withColumn("total_amount", col("total_amount").cast("double"))

# Join data
df = transactions \
    .join(vehicles.select("vehicle_id", "brand", "vehicle_type"), on="vehicle_id", how="left") \
    .join(locations.withColumnRenamed("location_id", "pickup_location"), on="pickup_location", how="left")

# Metrics by location
location_metrics = df.groupBy("pickup_location", "city", "state").agg(
    sum("total_amount").alias("total_revenue"),
    count("*").alias("total_transactions"),
    avg("total_amount").alias("avg_transaction"),
    max("total_amount").alias("max_transaction"),
    min("total_amount").alias("min_transaction"),
    countDistinct("vehicle_id").alias("unique_vehicles")
)

# Metrics by vehicle type
vehicle_metrics = df.withColumn(
    "rental_duration_hours",
    (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600
).groupBy("vehicle_type").agg(
    sum("rental_duration_hours").alias("total_rental_hours"),
    sum("total_amount").alias("vehicle_type_revenue")
)

# Write outputs to S3 in Parquet
location_metrics.write.mode("overwrite").parquet("s3://car-rental-datastore/processed/vehicle_location_metrics/")
vehicle_metrics.write.mode("overwrite").parquet("s3://car-rental-datastore/processed/vehicle_type_metrics/")

spark.stop()
