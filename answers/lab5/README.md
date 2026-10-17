# Lab 5 – Bronze to Silver with dbt and DuckDB: answers

Setup: Onyxia `vscode-pyspark`, dbt-core 1.12.6, dbt-duckdb 1.11.0. The dbt project is in [`lab_medallion/`](../../lab_medallion/). DuckDB reads S3 with the persistent secret `s3_onyxia_connection`.

## Bronze layer

### Q1. The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months later?

Bronze keeps the raw data exactly as received. If I find a bug in `stg_users` six months later, I fix the SQL model and run `dbt build` again: the whole silver layer is rebuilt from bronze, also for the old data (backfill). I do not need to extract the data again from the source systems, which may have changed, deleted the old records, or not be available. Without bronze, the wrong silver data would be the only copy left, and the bug could not be fixed for the past. This also needs idempotent models: running them again on the same bronze gives the same result.

### Q2. The source reads the whole file on every query. Which other format and layout would you choose if the orders were ingested every hour for years?

I would store the orders in Parquet (columnar, compressed, with types) instead of CSV, and partition the files by date, for example `bronze/orders/date=2026-10-17/part-*.parquet` (Hive-style partitioning), with one partition per day, not per hour, to avoid too many small files. Then a query on one period reads only the partitions it needs (partition pruning) and only the columns it needs. A table format like Iceberg adds a list of files and snapshots on top. In dbt, the silver model can also be `incremental`, so it only processes the new partitions and not the full history.

## Build

### Q3. The silver models are views. What happens when a view is queried, and when is it preferable to a table?

_TODO_

### Q4. `UUID` values are stored in 16 bytes. How many bytes does the `VARCHAR` representation use?

_TODO_

## Data tests

_TODO_

## Singular tests

### Q5. Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the sales per region?

_TODO_

### Q6. The addresses associate states with random zip codes, `KY 01352` for example. Which test would detect it, and which reference data would it require?

_TODO_

## Export the silver layer to Parquet

_TODO_

## Exercises

### E1. Add a cleaned column `username_normalized`

_TODO_

### E2. Improve order validation

_TODO_

### E3. Clean invalid birthdates

_TODO_

### E4. Detect invalid ZIP codes

_TODO_

### E5. Materialization experiment

_TODO_
