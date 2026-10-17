# Lab 5 – Bronze to Silver with dbt and DuckDB: answers

Setup: Onyxia `vscode-pyspark`, dbt-core 1.12.6, dbt-duckdb 1.11.0. The dbt project is in [`lab_medallion/`](../../lab_medallion/). DuckDB reads S3 with the persistent secret `s3_onyxia_connection`.

## Bronze layer

### Q1. The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months later?

Bronze keeps the raw data exactly as received. If I find a bug in `stg_users` six months later, I fix the SQL model and run `dbt build` again: the whole silver layer is rebuilt from bronze, also for the old data (backfill). I do not need to extract the data again from the source systems, which may have changed, deleted the old records, or not be available. Without bronze, the wrong silver data would be the only copy left, and the bug could not be fixed for the past. This also needs idempotent models: running them again on the same bronze gives the same result.

### Q2. The source reads the whole file on every query. Which other format and layout would you choose if the orders were ingested every hour for years?

I would store the orders in Parquet (columnar, compressed, with types) instead of CSV, and partition the files by date, for example `bronze/orders/date=2026-10-17/part-*.parquet` (Hive-style partitioning), with one partition per day, not per hour, to avoid too many small files. Then a query on one period reads only the partitions it needs (partition pruning) and only the columns it needs. A table format like Iceberg adds a list of files and snapshots on top. In dbt, the silver model can also be `incremental`, so it only processes the new partitions and not the full history.

## Build

### Q3. The silver models are views. What happens when a view is queried, and when is it preferable to a table?

A view stores only the query, not the data. Each time the view is queried, DuckDB runs the full query again: here it reads the CSV file on S3, parses it, casts the columns and splits the address. `dbt run` is fast because it only creates the view (`create view ... as (select ...)` in `target/run/`), but every read pays the full cost. A view is better when the model is light, is read rarely, or must always show the latest data of the source without a rebuild. A table is better when the model is read many times (dashboards), when the query is expensive (joins, aggregations, reading remote files), or when the source can disappear or change: the result is computed once, at `dbt run`, and then read fast.

### Q4. `UUID` values are stored in 16 bytes. How many bytes does the `VARCHAR` representation use?

The text form of a UUID is 36 characters (32 hex digits + 4 hyphens), for example `bdd640fb-0667-4ad1-9c80-317fa3b1799d`, so 36 bytes in ASCII, more than twice the 16 bytes of the `UUID` type (and DuckDB also adds a 16-byte string header for long strings). The `UUID` type also checks the format and makes comparisons and joins faster. In the Spark lab, Spark has no UUID type, so `order_id` and `user_id` stayed `string`.

## Data tests

Generic tests are declared in `models/silver/silver.yml`: `unique` + `not_null` on the keys, `accepted_values` on `sex`, `quantity` (`quote: false`, numbers) and `product`, and `relationships` from `stg_orders.user_id` to `stg_users.user_id`. Each test compiles to a query that returns the bad rows: 0 rows = PASS. Result: `PASS=13` (12 generic tests + the singular test `assert_orders_have_users`).

`assert_orders_have_users` (`LEFT JOIN ... WHERE u.user_id IS NULL`) checks the same thing as the generic `relationships` test.

Change from the lab: I removed the `relationships` test from `state` to `ref('states')`, because no `states` model or seed exists in the lab ("we will not use seeds"), so dbt would fail at compile time. State validation is done in exercise 4.

With `--store-failures`, dbt writes the bad rows of each test in the `main_dbt_test__audit` schema (14 tables, one per test). With my data all generic tests pass, so I do not get the `donuts` / `dring` example of the lab.

## Singular tests

### Q5. Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the sales per region?

The singular test `assert_orders_after_user_birth` fails with 288 orders. They come from 5 users: `ronaldstephens` (79), `cgrimes` (63), `dayderek` (52), `nicolejohnson` (49), `charleslandry` (45), born between 2022 and 2026 while their orders are in 2020 (Faker makes birthdates relative to today). I set `severity='warn'`: the pipeline goes on, but the problem is still reported on each run.

For a dashboard about the **age** of customers, the bad birthdates directly give a wrong result (negative or very small ages). I would replace them with `NULL` in silver and add a flag `birthdate_is_valid`, so these users are excluded from age statistics but kept for everything else, and ask the data producer to fix the generator.

For a dashboard about **sales per region**, the birthdate is not used at all. The orders and the state are correct, so I keep all the rows and just monitor the issue with the warning. Rejecting these users would remove 288 real orders and give wrong sales numbers.

### Q6. The addresses associate states with random zip codes, `KY 01352` for example. Which test would detect it, and which reference data would it require?

A test that checks that each zip code belongs to its state, for example a singular test that joins `stg_users` with a reference table and returns the users whose `zip_code` is not in the range of their `state`. `KY 01352` would fail because zip codes starting with `0` belong to the north-east (Massachusetts, Connecticut...), while Kentucky zip codes are `40000`-`42799`.

It needs reference data: the list of US states, territories and military codes (AA, AE, AP), with the valid zip code ranges or the 3-digit zip prefixes of each one. The official source is USPS. It can be stored in the project as a dbt seed (`seeds/zip_ranges.csv`), because it is small and changes rarely, and versioned in Git. This is exercise 4.

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
