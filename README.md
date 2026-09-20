# Snowflake ELT CDC Pipeline

## Project Overview

This project demonstrates an end-to-end data pipeline for ingesting bookstore data from Amazon S3 into Snowflake, performing data cleaning and validation, handling invalid records, and incrementally loading validated data into a production table.

The pipeline uses Snowflake **Snowpipe, Streams, Tasks, and MERGE** to automate ingestion and incremental data processing.

### Architecture

```text
CSV Files
    │
    ▼
Amazon S3
    │
    ▼
Storage Integration
    │
    ▼
External Stage
    │
    ▼
Snowpipe (Auto Ingest)
    │
    ▼
RAW_BOOKS
    │
    ▼
Data Cleaning & Validation
    │
    ├───────────────┐
    ▼               ▼
CLEAN_BOOKS     ERROR_BOOKS
    │
    ▼
BOOKS_STREAM
    │
    ▼
BOOKS_TASK_AUTO
    │
    ▼
MERGE
    │
    ▼
PROD_BOOKS
```

---

## Project Objective

The objective of this project is to build a Snowflake-based ELT pipeline that demonstrates:

* Automated ingestion from Amazon S3
* Raw data landing
* Data type conversion and cleansing
* Data quality validation
* Invalid record handling
* Incremental change processing
* Automated production table updates
* Pipeline monitoring and validation

---

## Technologies

* Snowflake
* SQL
* Amazon S3
* Snowpipe
* Snowflake Streams
* Snowflake Tasks
* SQL MERGE
* Snowflake Information Schema

---

## Pipeline Components

### 1. Amazon S3 — Source

Bookstore data is provided as CSV files stored in an Amazon S3 location.

The S3 location acts as the external source for the pipeline.

---

### 2. Storage Integration

A Snowflake Storage Integration is used to establish secure access between Snowflake and Amazon S3.

The actual AWS account-specific configuration is not included in this public repository.

---

### 3. External Stage

The Snowflake external stage points to the S3 location containing the bookstore files.

```text
S3 Bucket
    ↓
External Stage
```

The stage provides Snowflake with access to the source files.

---

### 4. File Format

A CSV file format defines how Snowflake should interpret the incoming files.

Configuration includes:

* CSV delimiter
* Header handling
* Optional quotation marks
* NULL handling
* Column-count validation behavior

---

### 5. Snowpipe — Automated Ingestion

Snowpipe is used to automatically load newly arriving files from S3 into the raw table.

```text
S3
 ↓
Snowpipe
 ↓
RAW_BOOKS
```

The raw table stores the incoming values using flexible data types.

---

### 6. RAW_BOOKS — Landing Layer

`RAW_BOOKS` acts as the landing/raw layer.

The data is stored before business transformations are applied.

Example fields:

```text
book_id
title
author
price
category
publish_date
rating
stock
```

The raw layer provides a source-level representation of the incoming data.

---

### 7. Data Cleaning and Validation

The raw data is transformed inside Snowflake.

Examples include:

* Converting string values to numeric types
* Converting strings to dates
* Removing unwanted spaces using `TRIM()`
* Handling NULL and empty values
* Validating required fields
* Rejecting invalid prices
* Rejecting invalid dates
* Rejecting invalid book IDs

Snowflake `TRY_TO_*` functions are used so invalid values can be detected without causing the transformation process to fail.

Example:

```sql
TRY_TO_NUMBER(book_id)
TRY_TO_DECIMAL(price, 10, 2)
TRY_TO_DATE(publish_date)
TRY_TO_DOUBLE(rating)
```

---

### 8. CLEAN_BOOKS — Validated Layer

Records that satisfy the validation rules are stored in `CLEAN_BOOKS`.

This table contains properly typed and validated bookstore data.

Example:

```text
RAW_BOOKS
    ↓
Validation
    ↓
CLEAN_BOOKS
```

---

### 9. ERROR_BOOKS — Invalid Records

Records that fail the validation rules are separated into `ERROR_BOOKS`.

Examples include:

* Invalid book ID
* Missing title
* Invalid price
* Invalid publication date

This prevents invalid records from entering the production table while retaining them for investigation.

---

### 10. Streams — Incremental Change Data Capture

A Snowflake Stream is used to track DML changes.

The stream captures metadata such as:

```text
METADATA$ACTION
METADATA$ISUPDATE
```

This allows the pipeline to process incremental changes instead of repeatedly processing the entire dataset.

---

### 11. Tasks — Automation

A Snowflake Task automates the incremental processing.

The task checks whether the stream contains new data using:

```sql
SYSTEM$STREAM_HAS_DATA()
```

Processing occurs only when changes are available.

---

### 12. MERGE — Incremental Loading

The Task uses `MERGE` to synchronize changes into the production table.

The logic handles:

```text
INSERT
UPDATE
DELETE
```

Example flow:

```text
STREAM
   │
   ▼
TASK
   │
   ▼
MERGE
   │
   ▼
PROD_BOOKS
```

---

### 13. PROD_BOOKS — Production Layer

`PROD_BOOKS` represents the final structured table used by downstream consumers.

It contains validated bookstore records and receives incremental changes through the Stream + Task + MERGE process.

---

## Data Quality

The pipeline includes several data quality checks:

* Data type validation
* Required field validation
* Positive price validation
* Date validation
* NULL handling
* Whitespace cleanup
* Invalid record separation
* Duplicate checks

---

## Monitoring and Validation

The project also includes Snowflake monitoring and validation queries.

Examples:

### Snowpipe monitoring

```sql
SHOW PIPES;

SELECT SYSTEM$PIPE_STATUS('book_pipe');
```

### Load history

```sql
SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.LOAD_HISTORY(
        START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
    )
);
```

### Task history

```sql
SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START =>
            DATEADD(HOUR, -1, CURRENT_TIMESTAMP()),
        RESULT_LIMIT => 100
    )
);
```

### Duplicate validation

 
SELECT
    book_id,
    COUNT(*) AS record_count
FROM raw_books
GROUP BY book_id
HAVING COUNT(*) > 1;
End-to-End Flow
Amazon S3
   │
   │ CSV
   ▼
Snowpipe
   │
   ▼
RAW_BOOKS
   │
   │ Cleaning + Validation
   ├───────────────┐
   ▼               ▼
CLEAN_BOOKS     ERROR_BOOKS
   │
   ▼
STREAM
   │
   ▼
TASK
   │
   ▼
MERGE
   │
   ▼
PROD_BOOKS
Key Concepts Demonstrated
Snowflake Storage Integration
External Stages
CSV File Formats
Snowpipe
Raw/Landing Layers
ELT transformations
Data quality validation
Error/rejected record handling
Snowflake Streams
Change Data Capture
Snowflake Tasks
Conditional task execution
SQL MERGE
Incremental loading
Pipeline validation
Load and task monitoring
Snowflake roles and permissions
Project Outcome

This project demonstrates an end-to-end Snowflake data pipeline that moves data from an external S3 source into a structured production table while incorporating automated ingestion, data quality validation, incremental processing, CDC, and monitoring.


This is the one I would use as the **main `README.md` for `snowflake-elt-cdc-pipeline`**. It describes what you actually practiced without pretending it was a production system.
# snowflake-elt-cdc-pipeline
Automated Snowflake ELT Pipeline with Data Quality and CDC
