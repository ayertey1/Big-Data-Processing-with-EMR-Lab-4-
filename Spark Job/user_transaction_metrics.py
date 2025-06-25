from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark
spark = SparkSession.builder.appName("UserTransactionMetrics").getOrCreate()

# Load raw data
users = spark.read.option("header", True).csv("s3://car-rental-datastore/raw/users/")
transactions = spark.read.option("header", True).csv("s3://car-rental-datastore/raw/rental_transactions/")

# Cast fields
transactions = transactions \
    .withColumn("total_amount", col("total_amount").cast("double")) \
    .withColumn("rental_start_time", to_timestamp("rental_start_time"))

# Join users and transactions
df = transactions.join(users.select("user_id", "first_name", "last_name", "is_active"), on="user_id", how="left")

# Daily metrics
daily_metrics = df.withColumn("rental_date", to_date("rental_start_time")).groupBy("rental_date").agg(
    count("*").alias("total_transactions"),
    sum("total_amount").alias("daily_revenue")
)

# User-specific metrics
user_metrics = df.groupBy("user_id", "first_name", "last_name").agg(
    count("*").alias("user_total_transactions"),
    sum("total_amount").alias("user_total_spent"),
    max("total_amount").alias("user_max_spent"),
    min("total_amount").alias("user_min_spent")
)

# Rental duration per user
df = df.withColumn(
    "rental_duration_hours",
    (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600
)

user_duration = df.groupBy("user_id").agg(
    sum("rental_duration_hours").alias("total_rental_hours")
)

# Write outputs to S3
daily_metrics.write.mode("overwrite").parquet("s3://car-rental-datastore/processed/daily_metrics/")
user_metrics.write.mode("overwrite").parquet("s3://car-rental-datastore/processed/user_metrics/")
user_duration.write.mode("overwrite").parquet("s3://car-rental-datastore/processed/user_duration/")

spark.stop()
