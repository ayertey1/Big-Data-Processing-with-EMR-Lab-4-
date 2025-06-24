# Car Rental Analytics Data Pipeline

## Overview

This project implements a scalable, automated big data processing pipeline for a car rental marketplace using AWS services. It enables extraction, transformation, analysis, and visualization of business-critical metrics such as revenue trends, user engagement, and vehicle/location performance.

The pipeline is designed to be production-grade with idempotency, error handling, and cost optimization in mind.

---

## Architecture Diagram

```
S3 (raw data)  -->  EMR (Spark Jobs)  -->  S3 (processed parquet)
                                   \
                                    --> AWS Glue Crawlers --> AWS Glue Catalog --> Athena
                                                                          \
                                                                           --> Lambda (Athena Query)
                                                                                       \
                                                                                        --> Step Functions (Orchestration)
```

---

## Project Objectives

* Process raw vehicle, rental, user, and location datasets using Spark on EMR
* Derive key metrics like revenue by location, rental duration by vehicle type, and user spending behavior
* Store clean, transformed data in Parquet format on S3
* Register datasets using AWS Glue Crawlers
* Enable querying and analytics using AWS Athena
* Orchestrate the entire pipeline using AWS Step Functions

---

## Dataset Description

All input data is stored in Amazon S3 under `s3://car-rental-datastore/raw/`:

| Dataset                | Description                                                        |
| ---------------------- | ------------------------------------------------------------------ |
| `vehicles/`            | Vehicle metadata including type and brand                          |
| `users/`               | Customer profiles with signup data and license info                |
| `locations/`           | Rental pickup and drop-off location master data                    |
| `rental_transactions/` | Detailed logs of vehicle rentals including timestamps and payments |

---

## Key Components

### 1. **Spark Jobs on EMR**

* **vehicle\_location\_metrics.py**

  * Calculates revenue per location, transaction volumes, and vehicle performance.
  * Outputs to `s3://car-rental-datastore/processed/vehicle_location_metrics/`

* **user\_transaction\_metrics.py**

  * Computes user-specific metrics (e.g. total spent, duration, daily trends).
  * Outputs to:

    * `processed/user_metrics/`
    * `processed/daily_metrics/`
    * `processed/user_duration/`

### 2. **Glue Crawlers**

* Crawlers scan the `processed/` directories and register schema in the Glue Data Catalog.
* Crawlers:

  * `vehicle_location_metrics`
  * `user_transaction_metrics_crawler`
  * `daily_metrics_crawler`

### 3. **Athena Querying**

* Example queries:

  * Top locations by revenue
  * Most active users
  * Vehicle types with longest rental durations

### 4. **Step Functions Orchestration**

* Fully automated orchestration of:

  * EMR cluster provisioning
  * Spark job execution
  * Glue crawler execution
  * Athena query
  * Cluster termination

---

## Lambda Functions

### `check_s3_output`

* Ensures idempotency by checking if expected output already exists in S3

### `run_athena_query`

* Executes a predefined SQL query on Athena and optionally handles results

---

## Best Practices Implemented

* **Idempotency**: Output checks prevent redundant processing
* **Fault Tolerance**: Catch blocks in Step Functions handle failures and clean up
* **Resource Optimization**: EMR cluster is terminated immediately after processing
* **Automation**: Fully event-driven with manual and future event triggers
* **Modularity**: Spark code is modular and stored externally in S3

---

## How to Run the Pipeline

1. Upload raw data to:

   * `s3://car-rental-datastore/raw/`
2. Deploy Lambda functions:

   * `check_s3_output`
   * `run_athena_query`
3. Create the Glue Crawlers for each processed dataset
4. Deploy and start the Step Function state machine
5. View output data in Athena or S3

---

## Outputs

Transformed data is stored in S3:

```
s3://car-rental-datastore/processed/
  |- vehicle_location_metrics/
  |- vehicle_type_metrics/
  |- user_metrics/
  |- daily_metrics/
  |- user_duration/
```

Athena queries use the Glue catalog registered under:

* Database: `car_rental_analytics`

---

## Authors

* **Peter Caleb Ayertey** – Data Engineering Trainee, Amalitech
---

## License

This project is for educational and demonstration purposes only. For commercial use, ensure appropriate access control, data encryption, and compliance with organizational security policies.
