# Lab 7 – Bronze to silver with Spark DataFrames: answers

Setup: Onyxia `vscode-pyspark` (4 CPU, 4 GiB), Spark 4.1.1, Scala 2.13, `hadoop-aws-3.4.2`, Iceberg `iceberg-spark-runtime-4.1_2.13:1.11.0`. The session is created from `python3` (not the `pyspark` shell), so the builder really creates it and all the S3A and Iceberg settings are used. `defaultParallelism = 4`.

## SparkSession with S3 access

### Q1. The credentials are temporary. What happens to a running Spark job if the service is restarted mid-lab, and why?

The job is lost. In `local[*]` mode the driver and the executors run inside the Onyxia service, so a restart kills the Spark process, and everything not written to S3 (cache, temp views, DataFrames) is lost. The new service also gets new credentials, so I must create the S3 profile and the session again. Also, even without a restart, the token has an end time: when it expires, the next S3 call fails with an `ExpiredToken` / 403 error, because the session keeps the old values it read at start.

### Q2. Why does a plain `SimpleAWSCredentialsProvider` fail here, while it worked in setups without a session token?

Onyxia gives temporary STS credentials: access key + secret key + session token. The three go together. `SimpleAWSCredentialsProvider` only sends the access key and the secret key, it ignores the token, so S3 refuses the request (access denied). It works with permanent keys, which have no token. `TemporaryAWSCredentialsProvider` also sends `fs.s3a.session.token`.

## Bronze layer

Read directly from S3 with `s3a://`, with `header=True, inferSchema=True, multiLine=True`. Result: 50 users and 2829 orders. With `sc.textFile()` in lab 6 I had 101 elements for users, because each address has a line break. Here `multiLine=True` keeps each user in one row, with all 7 columns. `inferSchema` gives `birthdate` the `date` type; `uuid` stays a `string` (Spark has no UUID type).

## Silver layer

### Q3. Explain the output of the code that was just run.

The query counts the users per state and the users with no city. The 6 users with no city are only on military codes: AE (3 of 3), AP (2 of 2), and one more lower in the list (probably AA). These are US Army addresses like `DPO AP 09617`: they have no city, so the city regex finds nothing. All real states have `missing_city = 0`, so the address parsing works. Also, `state` is not only US states: FM and VI are territories. And with only 50 users many states have 2 users, so the order between equal rows is not fixed and can change between runs.

### Q4. Compare the DataFrame and SQL cells. Which one for a one-off exploration, and which for a model reused by several colleagues? Why?

Both give the same Catalyst plan, so the speed is the same. For a one-off exploration I would use SQL: it is quick to write and easy to read, also for analysts. For a model reused by colleagues I would use the DataFrame API: I can put the steps in Python functions with parameters, reuse them, test them with unit tests and keep them in Git like normal code. Typos in column names are also found earlier. But a SQL model in a tool like dbt is also a good choice for a shared model.

### Q5. `regexp_extract()` returned an empty string instead of `NULL` on no match. Where else could that same behavior silently produce a wrong result?

The same problem is on `state` and `zip_code`: if the second address line is missing or has another format, `regexp_extract()` gives `""`, not `NULL`. Then `""` is counted as one more state in a `groupBy`, and a `not_null` test does not see it. The same can happen with `F.trim()`: a value with only spaces becomes `""`, not `NULL` (for `username`, `name`, `email`, `sex`). The fix is the same: change `""` into `NULL` with `F.when(... == "", None)` or `F.nullif`.

## Write the silver layer as Parquet

### Q6. List the objects written under `silver/spark/users.parquet/`. How many part-files are there, and why?

_TODO_

### Q7. `coalesce(1)` before `.write` would produce a single file. What do you lose by doing that on a larger dataset?

_TODO_

## Write the silver layer as Iceberg tables

### Q8. Compare `silver/spark/orders.parquet/` and `warehouse/silver/orders/`. Which files hold the data, and which ones describe the table?

_TODO_

### Q9. A second job overwrites the Parquet directory while a query is reading it. What does the reader see? What changes with an Iceberg table?

_TODO_

### Q10. With this catalog, the pointer to the current version of a table is a file in the bucket. What can go wrong if two jobs commit at the same time, and what would a catalog service add?

_TODO_

## Compare with the dbt lab

_Pending: the dbt lab (module 05) is not done yet, so `silver/dbt/` does not exist. This section will be done after it._

## Exercises

### E1. `birthdate_is_valid`, with the DataFrame API and with Spark SQL

_TODO_

### E2. `zip_code` with a Python UDF, a `pandas_udf` and `regexp_extract()`, timed

_TODO_

### E3. Cache the bronze DataFrames and check the Spark UI

_TODO_

### E4. Append to the Iceberg table, snapshots, `VERSION AS OF`, compare with Parquet

_TODO_
