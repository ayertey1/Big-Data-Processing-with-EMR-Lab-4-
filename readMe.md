# Car Rental Analytics Data Pipeline

## Overview

This project implements a scalable, automated big data processing pipeline for a car rental marketplace using AWS services. It enables extraction, transformation, analysis, and visualization of business-critical metrics such as revenue trends, user engagement, and vehicle/location performance.

The pipeline is designed to be production-grade with idempotency, error handling, retry logic, validation gates, and cost optimization in mind. It includes mechanisms for automated triggers, state tracking, and modular data transformations.

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
* Ensure reliability, automation, and cost-efficiency

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

* **vehicle_location_metrics.py**
  * Calculates revenue per location, transaction volumes, and vehicle performance.
  * Outputs to `s3://car-rental-datastore/processed/vehicle_location_metrics/`

* **user_transaction_metrics.py**
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

* Athena allows querying of processed metrics using SQL
* Example queries:
  * Top locations by revenue
  * Most active users
  * Vehicle types with longest rental durations
  * Daily revenue trends

### 4. **Step Functions Orchestration**

* Fully automated orchestration of:
  * Lambda validation (check if scripts + raw data exist)
  * EMR cluster provisioning
  * Spark job execution
  * Glue crawler execution
  * Athena query
  * EMR cluster termination
  * Graceful exit if validation fails

---

## Lambda Functions

### `validate_assets`
* Ensures both the required Spark scripts and raw data directories exist in S3
* Used as the first step in the Step Function workflow
* Supports retry if validation fails

### `check_s3_output`
* Ensures idempotency by checking if expected output already exists in S3

### `run_athena_query`
* Executes a predefined SQL query on Athena and moves result files to descriptive paths in S3

---

## Best Practices Implemented

* **Idempotency**: Output checks prevent redundant processing
* **Fault Tolerance**: Catch blocks in Step Functions handle failures and clean up
* **Retry Logic**: Validation step waits and retries before failing
* **Resource Optimization**: EMR cluster is terminated immediately after processing
* **Automation**: EventBridge triggers Step Functions on new S3 data arrival
* **Modularity**: Spark code is modular and stored externally in S3
* **Monitoring**: Logs available in CloudWatch for Lambda and EMR jobs

---

## How to Run the Pipeline

### Prerequisites
* AWS CLI or Console access
* IAM roles:
  * EMR_DefaultRole, EMR_EC2_DefaultRole
  * Lambda execution roles with S3, Glue, and Athena access
* S3 bucket: `car-rental-datastore` with appropriate folders

### Setup Steps
1. **Upload Raw Data**
   - Place CSVs in:
     - `raw/vehicles/`
     - `raw/users/`
     - `raw/locations/`
     - `raw/rental_transactions/`

2. **Upload Spark Scripts to S3**
   - `scripts/vehicle_location_metrics.py`
   - `scripts/user_transaction_metrics.py`

3. **Deploy Lambda Functions**
   - `validate_assets`
   - `check_s3_output`
   - `run_athena_query`

4. **Create Glue Crawlers**
   - Target processed S3 locations
   - Register tables under `car_rental_analytics`

5. **Create Step Function State Machine**
   - Use the JSON definition with validation, retries, EMR steps, crawlers, Athena, and cleanup

6. **Set Up EventBridge Rule**
   - Trigger Step Function on object creation in `raw/`

7. **Run / Monitor**
   - Use AWS Console to monitor Step Function execution
   - View Athena outputs in `s3://car-rental-datastore/athena-exports/`

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

Athena queries output to:
```
s3://car-rental-datastore/athena-exports/
  |- top_locations.csv
  |- top_vehicle_types.csv
  |- top_spenders.csv
  |- daily_revenue_trends.csv
```

Tables are registered under:
* Database: `car_rental_analytics`

---

## Development Process Documentation

* Spark scripts tested locally using PySpark before S3 deployment
* Lambda scripts tested using AWS Lambda console test events
* S3 validation and retry behavior simulated using custom events
* Glue Crawlers manually validated on first run, then integrated
* Step Function logic built incrementally and tested with CloudWatch logs
* IAM roles verified with least privilege principle
* All code versioned in Git with branch protection for pipeline logic

---

## Authors

* **Peter Caleb Ayertey** – Data Engineering Trainee, Amalitech

---

## License

This project is for educational and demonstration purposes only. For commercial use, ensure appropriate access control, data encryption, and compliance with organizational security policies.
