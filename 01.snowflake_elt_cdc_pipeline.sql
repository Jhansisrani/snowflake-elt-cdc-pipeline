```sql
/* ============================================================
   AUTOMATED SNOWFLAKE ELT PIPELINE - BOOKSTORE

   S3
    ↓
   Snowpipe
    ↓
   RAW_BOOKS
    ↓
   CLEAN_BOOKS + ERROR_BOOKS
    ↓
   STREAM
    ↓
   TASK + MERGE
    ↓
   PROD_BOOKS
   ============================================================ */


/* ============================================================
   1. DATABASE AND SCHEMA
   ============================================================ */

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS amazon;

USE DATABASE amazon;

CREATE SCHEMA IF NOT EXISTS bookstore;

USE SCHEMA bookstore;


/* ============================================================
   2. STORAGE INTEGRATION
   ============================================================ */

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION aws_sf_py_data
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<YOUR_AWS_IAM_ROLE_ARN>'
    STORAGE_ALLOWED_LOCATIONS =
        ('s3://<YOUR_S3_BUCKET>/');

DESC INTEGRATION aws_sf_py_data;


/* ============================================================
   3. CSV FILE FORMAT
   ============================================================ */

CREATE OR REPLACE FILE FORMAT book_dataset_csv
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;


/* ============================================================
   4. EXTERNAL STAGE
   ============================================================ */

CREATE OR REPLACE STAGE stg_books_s3_auto 
    URL = 's3://<YOUR_S3_BUCKET>/<YOUR_S3_FOLDER>/'
    STORAGE_INTEGRATION = aws_sf_py_data
    FILE_FORMAT = book_dataset_csv;

LIST @<STAGE_NAME>;


/* ============================================================
   5. RAW LANDING TABLE
   ============================================================ */

CREATE OR REPLACE TABLE raw_books (
    book_id       STRING,
    title         STRING,
    author        STRING,
    price         STRING,
    category      STRING,
    publish_date  STRING,
    rating        STRING,
    stock         STRING
);


/* ============================================================
   6. SNOWPIPE
   ============================================================ */

CREATE OR REPLACE PIPE book_pipe  AUTO_INGEST = TRUE
AS
COPY INTO raw_books FROM @stg_books_s3_auto  ON_ERROR = CONTINUE;

SHOW PIPES;


/* ============================================================
   7. MANUAL PIPE REFRESH FOR TESTING
   ============================================================ */

ALTER PIPE book_pipe REFRESH;


/* ============================================================
   8. CLEAN TABLE
   ============================================================ */

CREATE OR REPLACE TABLE clean_books (
    book_id       NUMBER NOT NULL,
    title         STRING NOT NULL,
    author        STRING,
    price         DECIMAL(10,2),
    category      VARCHAR,
    publish_date  DATE NOT NULL,
    rating        FLOAT,
    stock         NUMBER
);


/* ============================================================
   9. DATA CLEANING AND VALIDATION
   ============================================================ */

INSERT INTO clean_books
SELECT
    TRY_TO_NUMBER(book_id) AS book_id,

    TRIM(title) AS title,

    NULLIF(TRIM(author), '') AS author,

    TRY_TO_DECIMAL(price, 10, 2) AS price,

    TRIM(category) AS category,

    TRY_TO_DATE(publish_date) AS publish_date,

    TRY_TO_DOUBLE(rating) AS rating,

    TRY_TO_NUMBER(stock) AS stock

FROM raw_books

WHERE
    TRY_TO_NUMBER(book_id) IS NOT NULL

    AND title IS NOT NULL
    AND TRIM(title) <> ''

    AND TRY_TO_DECIMAL(price, 10, 2) IS NOT NULL
    AND TRY_TO_DECIMAL(price, 10, 2) > 0

    AND TRY_TO_DATE(publish_date) IS NOT NULL;


/* ============================================================
   10. HANDLE MISSING AUTHOR
   ============================================================ */

UPDATE clean_books
SET author = COALESCE(author, 'UNKNOWN')
WHERE author IS NULL;


/* ============================================================
   11. ERROR / REJECTED RECORD TABLE
   ============================================================ */

CREATE OR REPLACE TABLE error_books AS
SELECT * FROM raw_books
WHERE 1 = 0;


/* ============================================================
   12. LOAD INVALID RECORDS
   ============================================================ */

INSERT INTO error_books
SELECT *
FROM raw_books

WHERE
       TRY_TO_DATE(publish_date) IS NULL

    OR TRY_TO_NUMBER(book_id) IS NULL

    OR title IS NULL
    OR TRIM(title) = ''

    OR TRY_TO_DECIMAL(price, 10, 2) IS NULL;


/* ============================================================
   13. PRODUCTION TABLE
   ============================================================ */

CREATE OR REPLACE TABLE prod_books (
    book_id       INT PRIMARY KEY,
    title         STRING,
    author        STRING,
    price         NUMBER(10,2),
    category      STRING,
    publish_date  DATE,
    rating        NUMBER(3,2),
    stock         INT
);


/* ============================================================
   14. STREAM
   Captures incremental DML changes from CLEAN_BOOKS.
   ============================================================ */

CREATE OR REPLACE STREAM books_stream
ON TABLE clean_books;


/* ============================================================
   15. TASK
   Runs every 5 minutes only when stream has data.
   ============================================================ */

CREATE OR REPLACE TASK books_task_auto

    WAREHOUSE = COMPUTE_WH

    SCHEDULE = '5 MINUTE'

    WHEN SYSTEM$STREAM_HAS_DATA('books_stream')

AS

MERGE INTO prod_books AS pb

USING books_stream AS bs

ON pb.book_id = bs.book_id


/* DELETE */

WHEN MATCHED
     AND bs.METADATA$ACTION = 'DELETE'
THEN DELETE


/* UPDATE */

WHEN MATCHED
     AND bs.METADATA$ISUPDATE = TRUE
THEN UPDATE SET

    pb.book_id       = bs.book_id,
    pb.title         = bs.title,
    pb.author        = bs.author,
    pb.price         = bs.price,
    pb.category      = bs.category,
    pb.publish_date  = bs.publish_date,
    pb.rating        = bs.rating,
    pb.stock         = bs.stock


/* INSERT */

WHEN NOT MATCHED
     AND bs.METADATA$ACTION = 'INSERT'
THEN INSERT
(
    book_id,
    title,
    author,
    price,
    category,
    publish_date,
    rating,
    stock
)

VALUES
(
    bs.book_id,
    bs.title,
    bs.author,
    bs.price,
    bs.category,
    bs.publish_date,
    bs.rating,
    bs.stock
);


/* ============================================================
   16. ENABLE TASK
   ============================================================ */

ALTER TASK books_task_auto RESUME;

SHOW TASKS;


/* ============================================================
   17. DATA VALIDATION
   ============================================================ */

SELECT * FROM raw_books;

SELECT * FROM clean_books;

SELECT * FROM error_books;

SELECT * FROM prod_books;

SELECT * FROM books_stream;


/* ============================================================
   18. DUPLICATE CHECK
   ============================================================ */

SELECT
    book_id,
    COUNT(*) AS record_count
FROM raw_books
GROUP BY book_id
HAVING COUNT(*) > 1;


SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT book_id) AS distinct_book_ids
FROM raw_books;


/* ============================================================
   19. SNOWPIPE MONITORING
   ============================================================ */

SHOW PIPES;

DESC PIPE book_pipe;

SELECT SYSTEM$PIPE_STATUS('book_pipe');

ALTER PIPE book_pipe REFRESH;

/* ============================================================
   20. LOAD HISTORY
   ============================================================ */

SELECT *
FROM TABLE
(
    INFORMATION_SCHEMA.LOAD_HISTORY
    (
        START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
    )
)
ORDER BY LAST_LOAD_TIME DESC;


/* ============================================================
   21. FAILED LOAD CHECK
   ============================================================ */

SELECT *
FROM TABLE
(
    INFORMATION_SCHEMA.LOAD_HISTORY
    (
        START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
    )
)

WHERE
       STATUS = 'LOAD_FAILED'
    OR ERROR_COUNT > 0;


/* ============================================================
   22. TASK HISTORY
   ============================================================ */

SELECT *
FROM TABLE
(
    INFORMATION_SCHEMA.TASK_HISTORY
    (
        SCHEDULED_TIME_RANGE_START =>
            DATEADD(HOUR, -1, CURRENT_TIMESTAMP()),

        RESULT_LIMIT => 100
    )
);


/* ============================================================
   23. CURRENT DATABASE / SCHEMA
   ============================================================ */

SELECT
    CURRENT_DATABASE(),
    CURRENT_SCHEMA();
```
