# NYC 311 Service Requests Data Engineering Pipeline

## Project Overview

This project implements a data engineering pipeline for the NYC 311 Service Requests dataset using PySpark and Databricks Free Edition. It follows the Medallion Architecture to ingest, validate, and clean service request data.

The pipeline focuses on explicit schema enforcement, full and incremental data loading, duplicate handling, data quality, and reliable processing.

## Dataset

* **Dataset:** NYC 311 Service Requests from 2020 to Present
* **Source:** NYC Open Data
* **Dataset ID:** `erm2-nwe9`
* **API Endpoint:** https://data.cityofnewyork.us/resource/erm2-nwe9.json

The dataset contains information about service requests submitted by New York City residents, including complaint types, agencies, dates, boroughs, statuses, and geographic locations.

## Technologies Used

* Python
* PySpark
* Databricks Free Edition
* NYC Open Data API
* Git and GitHub

## Project Structure

```text
nyc311-data-engineering/
├── layers_implementation/
│   └── ...
├── schemas/
│   └── ...
├── api_call.py
├── workflow
└── README.md
```

* **`layers_implementation/`** — Contains the Bronze and Silver layer implementations.
* **`schemas/`** — Contains explicit schema definitions for the data layers.
* **`api_call.py`** — Retrieves NYC 311 data from the public API.
* **`workflow`** — Defines or organizes the pipeline execution workflow.

## Architecture

The project follows the Medallion Architecture, with Bronze and Silver layers.

### 1. Bronze Layer — Raw Data

**Table:** `workspace.default.bronze_nyc311`

The Bronze layer stores ingested NYC 311 records while preserving the source fields and structure as much as possible. An explicit PySpark schema is used instead of automatic schema inference.

**Main operations:**

* Ingest records from the NYC Open Data API.
* Apply the defined Bronze schema.
* Preserve source fields for downstream processing.
* Add ingestion metadata, such as ingestion timestamp and source file, where supported by the implementation.
* Support full and incremental ingestion.

#### Bronze Data Model

| Attribute              | Description                                            |
| ---------------------- | ------------------------------------------------------ |
| Primary identifier     | `unique_key`                                           |
| Date fields            | `created_date`, `closed_date`                          |
| Agency information     | `agency`, `agency_name`                                |
| Request classification | `complaint_type`, `descriptor`                         |
| Location information   | `incident_zip`, `borough`, `latitude`, `longitude`     |
| Request status         | `status`                                               |
| Other source fields    | Additional fields provided by the NYC 311 dataset      |
| Ingestion metadata     | Ingestion timestamp and source file, where implemented |

**Key:** `unique_key` identifies an individual service request.

The Bronze table is intended to retain source-level information so that the data can be validated and transformed in the Silver layer.

### 2. Silver Layer — Cleaned Data

**Table:** Separate Silver table defined by the project implementation.

The Silver layer transforms Bronze records into a cleaner and more consistent dataset suitable for analysis.

**Main operations:**

* Remove records with missing `unique_key` values.
* Remove duplicate records based on `unique_key`.
* Convert date columns to timestamp data types.
* Convert numeric fields, such as district identifiers and geographic coordinates, to appropriate data types.
* Trim unnecessary whitespace from selected text columns.
* Apply validation and data quality rules.
* Store the processed records separately from the Bronze layer.

#### Silver Data Model

| Attribute             | Description                                                     |
| --------------------- | --------------------------------------------------------------- |
| `unique_key`          | Unique identifier for a service request                         |
| `created_date`        | Date and time the request was created                           |
| `closed_date`         | Date and time the request was closed, when available            |
| `agency`              | Agency responsible for the request                              |
| `agency_name`         | Name of the responsible agency                                  |
| `complaint_type`      | Category of the service request                                 |
| `descriptor`          | Additional classification or description                        |
| `borough`             | Borough associated with the request                             |
| `incident_zip`        | ZIP code associated with the incident                           |
| `council_district`    | Council district, stored as an integer where valid              |
| `latitude`            | Latitude of the request location                                |
| `longitude`           | Longitude of the request location                               |
| `status`              | Current request status                                          |
| Other retained fields | Additional source fields preserved by the Silver transformation |

The exact Silver columns depend on the fields retained in the implemented transformation. Date, numeric, and text columns are converted or cleaned according to the defined schema and transformation rules.

## Data Loading Strategy

### Full Load

The initial load retrieves historical service request records and writes them to the Bronze layer.

### Incremental Load

Subsequent runs retrieve newly created or updated records using a date-based watermark and, where configured, a lookback window.

The `unique_key` field supports duplicate identification. Updating existing records requires appropriate merge or upsert logic.

### Idempotency

The pipeline aims to prevent duplicate records when the same batch is processed more than once. Deduplication and, where implemented, merge operations support repeatable execution.

## Data Quality and Audit Logging

The pipeline supports or aims to support the following checks:

* Required identifier validation
* Duplicate detection
* Explicit schema enforcement
* Date and numeric type conversion
* Text cleaning
* Pipeline execution status and error logging
* Processed record counts, where recorded

Audit logging and incremental update handling should be documented as completed features only when implemented in the workflow.

## Execution Workflow

1. Retrieve source data through `api_call.py`.
2. Load records using the defined schema.
3. Execute the Bronze layer pipeline.
4. Validate and transform the Bronze records into Silver.
5. Run the configured workflow.
6. Verify output tables and data quality results.

## Data Source

[NYC 311 Service Requests from 2020 to Present](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9)

## Contributors

Add project team members and student IDs here.
