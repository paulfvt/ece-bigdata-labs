# Lab 5 – Bronze to Silver with dbt and DuckDB: answers

Setup: Onyxia `vscode-pyspark`, dbt-core 1.12.6, dbt-duckdb 1.11.0. The dbt project is in [`lab_medallion/`](../../lab_medallion/). DuckDB reads S3 with the persistent secret `s3_onyxia_connection`.

## Bronze layer

### Q1. The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months later?

_TODO_

### Q2. The source reads the whole file on every query. Which other format and layout would you choose if the orders were ingested every hour for years?

_TODO_

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
