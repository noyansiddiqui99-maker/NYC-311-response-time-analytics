# NYC 311 Service Requests Data Engineering Pipeline

## 1. Project Overview

This project implements an end-to-end data engineering pipeline for the NYC 311 Service Requests dataset using PySpark and Databricks Free Edition. The pipeline follows the Medallion Architecture and separates raw ingestion from data cleaning and validation.

The main objectives are to:

* Load NYC 311 data using an explicitly defined schema.
* Store source records and ingestion metadata in the Bronze layer.
* Clean, validate, and transform records in the Silver layer.
* Handle duplicate records and invalid data.
* Support full loads, incremental processing, and historical backfills.
* Use Delta Lake merge operations to insert new records and update existing records.
* Record pipeline execution details for monitoring and troubleshooting.

## 2. Dataset

* **Dataset:** NYC 311 Service Requests from 2020 to Present
* **Source:** NYC Open Data
* **Dataset ID:** `erm2-nwe9`
* **API endpoint:** https://data.cityofnewyork.us/resource/erm2-nwe9.json

The dataset contains service requests submitted by New York City residents. It includes request identifiers, dates, responding agencies, complaint types, addresses, boroughs, statuses, and geographic information.

## 3. Technologies Used

* Python
* PySpark
* Databricks Free Edition
* Delta Lake
* NYC Open Data API
* Git and GitHub

## 4. Project Structure

```text
nyc311-data-engineering/
├── layers_implementation/
│   ├── bronze_implementation_final.ipynb
│   └── silver_implementation_final.ipynb
├── schemas/
│   └── schema definitions
├── api_call.py
├── workflow
└── README.md
```

* **`layers_implementation/`** contains the Bronze and Silver processing notebooks.
* **`schemas/`** contains schema definitions used to control data types.
* **`api_call.py`** retrieves records from the NYC Open Data API.
* **`workflow`** organizes or configures pipeline execution.
* **`README.md`** documents the architecture, data models, and execution process.

## 5. Medallion Architecture

The pipeline uses two primary layers:

1. **Bronze:** Stores ingested source records with batch and ingestion metadata.
2. **Silver:** Stores cleaned, validated, deduplicated, and typed records.

The layers are stored separately so that the original ingested data remains available for validation and reprocessing.

## 6. Bronze Layer — Raw Data

**Table:** `workspace.default.bronze_nyc311`

### Purpose

The Bronze layer stores records ingested from the NYC 311 source. An explicit PySpark schema defines the expected columns and data types rather than relying on automatic schema inference.

The layer preserves source-level information and adds metadata that identifies where and when a record was ingested.

### Main operations

* Read raw JSON data from the configured source path.
* Apply an explicit schema.
* Attach `source_file`, `batch_id`, and `load_timestamp` metadata.
* Support full and incremental batch processing.
* Detect unexpected columns and handle schema drift according to the implemented ingestion logic.
* Separate malformed records for data-quality inspection.
* Retain records needed for downstream Silver processing.

### Bronze data dictionary

The following table documents the expected source schema and ingestion metadata. Source fields are stored as strings unless otherwise specified.

| Column name                      | Data type | Primary key?       |
| -------------------------------- | --------- | ------------------ |
| `unique_key`                     | STRING    | Yes, composite key |
| `created_date`                   | TIMESTAMP | No                 |
| `closed_date`                    | TIMESTAMP | No                 |
| `name`                           | STRING    | No                 |
| `agency`                         | STRING    | No                 |
| `agency_name`                    | STRING    | No                 |
| `complaint_type`                 | STRING    | No                 |
| `descriptor`                     | STRING    | No                 |
| `descriptor_2`                   | STRING    | No                 |
| `location_type`                  | STRING    | No                 |
| `incident_zip`                   | STRING    | No                 |
| `incident_address`               | STRING    | No                 |
| `street_name`                    | STRING    | No                 |
| `cross_street_1`                 | STRING    | No                 |
| `cross_street_2`                 | STRING    | No                 |
| `intersection_street_1`          | STRING    | No                 |
| `intersection_street_2`          | STRING    | No                 |
| `address_type`                   | STRING    | No                 |
| `city`                           | STRING    | No                 |
| `landmark`                       | STRING    | No                 |
| `facility_type`                  | STRING    | No                 |
| `status`                         | STRING    | No                 |
| `due_date`                       | TIMESTAMP | No                 |
| `resolution_description`         | STRING    | No                 |
| `resolution_action_updated_date` | TIMESTAMP | No                 |
| `community_board`                | STRING    | No                 |
| `council_district`               | STRING    | No                 |
| `fbicr_code`                     | STRING    | No                 |
| `police_precinct`                | STRING    | No                 |
| `latitude`                       | STRING    | No                 |
| `bbl`                            | STRING    | No                 |
| `borough`                        | STRING    | No                 |
| `location`                       | STRING    | No                 |
| `x_coordinate`                   | STRING    | No                 |
| `y_coordinate`                   | STRING    | No                 |
| `longitude`                      | STRING    | No                 |
| `open_data_channel_type`         | STRING    | No                 |
| `park_facility_name`             | STRING    | No                 |
| `park_borough`                   | STRING    | No                 |
| `vehicle_type`                   | STRING    | No                 |
| `taxi_company_borough`           | STRING    | No                 |
| `taxi_pick_up_location`          | STRING    | No                 |
| `bridge_highway_name`            | STRING    | No                 |
| `bridge_highway_direction`       | STRING    | No                 |
| `road_ramp`                      | STRING    | No                 |
| `bridge_highway_segment`         | STRING    | No                 |
| `source_file`                    | STRING    | No                 |
| `batch_id`                       | STRING    | Yes, composite key |
| `load_timestamp`                 | TIMESTAMP | No                 |

### Bronze primary key

The configured Bronze key is the composite of `unique_key` and `batch_id`.

* `unique_key` identifies the NYC 311 service request.
* `batch_id` identifies the ingestion batch in which the record was processed.

This allows the Bronze layer to associate a request with a particular batch. The actual Delta table does not automatically enforce primary-key constraints merely because a key is documented here.

## 7. Silver Layer — Cleaned Data

**Table:** `workspace.default.silver_nyc311`

### Purpose

The Silver layer converts ingested data into a consistent format that is suitable for analysis. It uses a separate schema to define the output columns and their expected data types.

### Main operations

* Remove records with missing `unique_key` values.
* Trim whitespace from selected text columns.
* Convert date fields to timestamps.
* Convert `council_district` to an integer.
* Convert latitude, longitude, and coordinate fields to numeric types.
* Validate conversions and identify invalid records.
* Deduplicate records using `unique_key`.
* Select the most recent valid record version.
* Use Delta Lake `MERGE INTO` logic to insert new requests and update existing requests when the incoming record is newer.
* Attach ingestion and source metadata to the processed records.

### Silver data dictionary

The table below documents the expected Silver schema. Fields that require cleaning or conversion use the types defined by the Silver transformation.

| Column name                      | Data type | Primary key? |
| -------------------------------- | --------- | ------------ |
| `unique_key`                     | STRING    | Yes          |
| `created_date`                   | TIMESTAMP | No           |
| `closed_date`                    | TIMESTAMP | No           |
| `name`                           | STRING    | No           |
| `agency`                         | STRING    | No           |
| `agency_name`                    | STRING    | No           |
| `complaint_type`                 | STRING    | No           |
| `descriptor`                     | STRING    | No           |
| `descriptor_2`                   | STRING    | No           |
| `location_type`                  | STRING    | No           |
| `incident_zip`                   | STRING    | No           |
| `incident_address`               | STRING    | No           |
| `street_name`                    | STRING    | No           |
| `cross_street_1`                 | STRING    | No           |
| `cross_street_2`                 | STRING    | No           |
| `intersection_street_1`          | STRING    | No           |
| `intersection_street_2`          | STRING    | No           |
| `address_type`                   | STRING    | No           |
| `city`                           | STRING    | No           |
| `landmark`                       | STRING    | No           |
| `facility_type`                  | STRING    | No           |
| `status`                         | STRING    | No           |
| `due_date`                       | TIMESTAMP | No           |
| `resolution_description`         | STRING    | No           |
| `resolution_action_updated_date` | TIMESTAMP | No           |
| `community_board`                | STRING    | No           |
| `council_district`               | INT       | No           |
| `fbicr_code`                     | STRING    | No           |
| `police_precinct`                | STRING    | No           |
| `latitude`                       | DOUBLE    | No           |
| `bbl`                            | STRING    | No           |
| `borough`                        | STRING    | No           |
| `location`                       | STRING    | No           |
| `x_coordinate`                   | DOUBLE    | No           |
| `y_coordinate`                   | DOUBLE    | No           |
| `longitude`                      | DOUBLE    | No           |
| `open_data_channel_type`         | STRING    | No           |
| `park_facility_name`             | STRING    | No           |
| `park_borough`                   | STRING    | No           |
| `vehicle_type`                   | STRING    | No           |
| `taxi_company_borough`           | STRING    | No           |
| `taxi_pick_up_location`          | STRING    | No           |
| `bridge_highway_name`            | STRING    | No           |
| `bridge_highway_direction`       | STRING    | No           |
| `road_ramp`                      | STRING    | No           |
| `bridge_highway_segment`         | STRING    | No           |
| `source_file`                    | STRING    | No           |
| `batch_id`                       | STRING    | No           |
| `load_timestamp`                 | TIMESTAMP | No           |
| `record_version`                 | TIMESTAMP | No           |

### Silver primary key

The Silver primary key is `unique_key`. The merge operation matches source and target records using this field.

When an incoming record has a newer `record_version`, it can replace the existing version. When the request does not already exist in Silver, the record is inserted.

The intended result is one current record per service request.

## 8. Data Loading Strategy

### Full load

A full load processes the historical data available from the configured raw-data source. It is used for initial population or rebuilding the target data when required.

### Incremental load

An incremental load processes a selected batch or newly extracted data rather than reprocessing the entire historical dataset.

The `batch_id` parameter identifies the batch to process. The source path identifies the folder or file from which the records are read.

### Backfill

A backfill reprocesses an earlier batch or a larger historical range.

For example, a previous batch can be selected again by supplying its `batch_id`. The same batch identifier can be reused when retrying or reprocessing that batch.

The Silver `batch`, `backfill_all`, and `full_rebuild` modes control whether a selected batch is processed, all Bronze batches are processed, or the Silver target is rebuilt.

### Idempotency

The pipeline is designed to support repeatable execution.

* Bronze processing replaces the selected batch where configured.
* Silver processing uses `unique_key` to match existing requests.
* The merge condition prevents an older record version from replacing a newer version.
* Duplicate-key checks can be used to verify that Silver contains only one record per request.

## 9. Schema Enforcement and Data Quality

The pipeline uses explicit schemas to avoid relying on automatic type inference.

The Silver layer performs the following checks and transformations:

* Required-key validation.
* Duplicate detection and removal.
* Timestamp parsing.
* Numeric type conversion.
* Text trimming.
* Invalid-record identification and quarantine.
* Schema-drift handling during ingestion.

Unexpected fields and invalid values must be checked against the implemented schema-evolution and quarantine logic. Fields added dynamically to Bronze are not necessarily part of the fixed Silver schema unless the Silver transformation explicitly handles them.

## 10. Pipeline Execution Logs

The pipeline records execution details in the configured audit table:

`workspace.default.pipeline_execution_logs_new`

The log is intended to capture information such as:

| Column name        | Data type | Description                            |
| ------------------ | --------- | -------------------------------------- |
| `layer`            | STRING    | Pipeline layer being processed         |
| `load_type`        | STRING    | Full or incremental processing type    |
| `parameter`        | STRING    | File, path, or batch parameter         |
| `start_time`       | TIMESTAMP | Execution start time                   |
| `end_time`         | TIMESTAMP | Execution end time                     |
| `status`           | STRING    | Run status                             |
| `rows_read`        | BIGINT    | Number of rows read                    |
| `rows_inserted`    | BIGINT    | Number of rows inserted                |
| `rows_updated`     | BIGINT    | Number of rows updated                 |
| `rows_quarantined` | BIGINT    | Number of rows rejected or quarantined |
| `error_message`    | STRING    | Error details, when applicable         |

The exact audit-table column names and types must match the schema defined by the logging implementation. If the implementation uses different names, this table should be updated accordingly.

The log helps identify failed runs, review processing counts, and troubleshoot data-quality issues.

## 11. Execution Workflow

1. Retrieve NYC 311 records through `api_call.py`.
2. Save the raw JSON files to the configured storage location.
3. Run the Bronze notebook with the required source path and batch parameters.
4. Validate ingestion results and review any quarantined records.
5. Run the Silver notebook in the required processing mode.
6. Check data types, record counts, and duplicate keys.
7. Review the execution log for run status and processing counts.

## 12. Verification Queries

The following Databricks SQL queries can be used to inspect the output tables.

### Check Bronze schema

```sql
DESCRIBE TABLE workspace.default.bronze_nyc311;
```

### Check Silver schema

```sql
DESCRIBE TABLE workspace.default.silver_nyc311;
```

### Check duplicate Silver keys

```sql
SELECT unique_key, COUNT(*) AS record_count
FROM workspace.default.silver_nyc311
GROUP BY unique_key
HAVING COUNT(*) > 1;
```

### Check missing Silver keys

```sql
SELECT COUNT(*) AS missing_keys
FROM workspace.default.silver_nyc311
WHERE unique_key IS NULL;
```

### Review pipeline logs

```sql
SELECT *
FROM workspace.default.pipeline_execution_logs_new
ORDER BY start_time DESC;
```

The verification queries should be run after the relevant tables have been created and populated.

## 13. Data Source

[NYC 311 Service Requests from 2020 to Present](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9)

## 14. Contributors

| Name           | Roll Number |
| -------------- | ----------- |
| Noyan Siddiqui | 24L-2593    |
| Abdul Rehman   | 24L-2613    |
