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

The query counts the users per state and the users with no city. The 6 users with no city are only on military codes: AE (3 of 3), AP (2 of 2), and one more lower in the list (not shown in the top 10). These are US Army addresses like `DPO AP 09617`: they have no city, so the city regex finds nothing. All real states have `missing_city = 0`, so the address parsing works. Also, `state` is not only US states: FM and VI are territories. And with only 50 users many states have 2 users, so the order between equal rows is not fixed and can change between runs.

### Q4. Compare the DataFrame and SQL cells. Which one for a one-off exploration, and which for a model reused by several colleagues? Why?

Both give the same Catalyst plan, so the speed is the same. For a one-off exploration I would use SQL: it is quick to write and easy to read, also for analysts. For a model reused by colleagues I would use the DataFrame API: I can put the steps in Python functions with parameters, reuse them, test them with unit tests and keep them in Git like normal code. Typos in column names are also found earlier. But a SQL model in a tool like dbt is also a good choice for a shared model.

### Q5. `regexp_extract()` returned an empty string instead of `NULL` on no match. Where else could that same behavior silently produce a wrong result?

The same problem is on `state` and `zip_code`: if the second address line is missing or has another format, `regexp_extract()` gives `""`, not `NULL`. Then `""` is counted as one more state in a `groupBy`, and a `not_null` test does not see it. The same can happen with `F.trim()`: a value with only spaces becomes `""`, not `NULL` (for `username`, `name`, `email`, `sex`). The fix is the same: change `""` into `NULL` with `F.when(... == "", None)` or `F.nullif`.

## Write the silver layer as Parquet

### Q6. List the objects written under `silver/spark/users.parquet/`. How many part-files are there, and why?

Each directory has only one part-file (`part-00000-...snappy.parquet`), plus a `_SUCCESS` file and a 0-byte directory marker. Spark writes one file per partition of the DataFrame, and here each DataFrame has only 1 partition (`getNumPartitions()` = 1). `stg_users` comes from a 7 KB file, smaller than `spark.sql.files.maxPartitionBytes` (128 MB), so it is read in 1 partition. `stg_orders` has a shuffle (the window `partitionBy`), which makes 200 partitions by default, but AQE merges them into 1 because the data is very small. The files use Snappy compression: `orders` is 139 KB in Parquet vs 331 KB in CSV. `_SUCCESS` is written at the end, to show that the write is complete.

### Q7. `coalesce(1)` before `.write` would produce a single file. What do you lose by doing that on a larger dataset?

With `coalesce(1)`, all the data goes into one partition, so only one task writes the file, on one executor. On a big dataset this is slow (no parallel writing), this one executor can run out of memory or disk, and the next job that reads the file can only start with few tasks too, because one big file is harder to split. Also, `coalesce(1)` can push the work before it (the transformations) into fewer tasks. It is better to keep many files of a good size (around 128 MB to 1 GB each), or use `repartition(n)` with a sensible `n`.

## Write the silver layer as Iceberg tables

### Q8. Compare `silver/spark/orders.parquet/` and `warehouse/silver/orders/`. Which files hold the data, and which ones describe the table?

In `silver/spark/orders.parquet/` there are only data files (`part-00000-...snappy.parquet`) and `_SUCCESS`. Nothing says which files are the table or which version I read: a reader just lists the directory.

In `warehouse/silver/orders/`, the data is in `data/` (one Parquet file, 84 KB, zstd compression by default in Iceberg vs 139 KB with Snappy). The table is described by `metadata/`, which is a chain of pointers:
`version-hint.text` (contains `1`) → `v1.metadata.json` (schema, partition spec, list of snapshots, current snapshot id) → `snap-<id>-....avro` (manifest list of the snapshot) → `...-m0.avro` (manifest: list of data files with row counts and min/max stats per column) → `data/*.parquet`.
The tables `lab.silver.orders.snapshots` and `.files` show the same information in SQL: 1 snapshot, 1 data file with 2829 records.

### Q9. A second job overwrites the Parquet directory while a query is reading it. What does the reader see? What changes with an Iceberg table?

With a plain Parquet directory, the overwrite deletes old files and writes new ones in the same place, and there is no transaction. A reader that lists the directory during this time can see a mix of old and new files, miss files that are already deleted (error "file not found"), or read files that are only half written. So the result can be wrong or the query can fail.

With Iceberg, the writer writes new data files and new metadata files, and the change becomes visible only at the commit, when the pointer moves to the new version (atomic commit). A reader keeps the snapshot it opened at the start, so it sees the old table complete, or the new table complete, never a mix. Old snapshots stay until they are expired, so I can also read an old version (time travel).

### Q10. With this catalog, the pointer to the current version of a table is a file in the bucket. What can go wrong if two jobs commit at the same time, and what would a catalog service add?

With the `hadoop` catalog, a commit means writing a new `vN.metadata.json` and updating `version-hint.text`. Object storage like S3 has no atomic rename and no lock. If two jobs commit at the same time, both can start from version 1 and both write version 2: one commit overwrites the other, and its changes are lost without any error (or the table can become inconsistent).

A catalog service (REST catalog, Apache Polaris, Nessie, Hive Metastore, AWS Glue) keeps the pointer to the current metadata file and changes it with an atomic "compare and swap": the second job sees that the version changed, its commit fails, and it retries on top of the new version. It also gives one central place to find tables, with access control.

## Compare with the dbt lab

Done after the dbt lab (lab 5). dbt output: `silver/dbt/users.parquet` and `silver/dbt/orders.parquet` (the lab text uses `stg_*.parquet` and paths without `dbt/` / `spark/`, I used the real paths).

| | dbt + DuckDB | Spark |
|---|---|---|
| Orders | 2829, quantity 8615 | 2830, quantity 8618 |
| Users | 50 | 50 |

The difference is one order: `00000000-0000-0000-0000-00000000e004` (quantity 3), the order I appended to the Spark Parquet directory in exercise 4. Without it, both engines give the same 2829 orders and 8615 items.

### Q11. What differs between the two schemas? Spark has no `UUID` type: what did you do about it, and what is the consequence for the consumers of the silver layer?

```text
dbt:   order_id uuid,    user_id uuid,    ordered_at timestamp with time zone, quantity integer, product varchar
Spark: order_id varchar, user_id varchar, ordered_at timestamp,                quantity integer, product varchar
```

Spark has no UUID type, so I cast the ids to `string`. Consequences: the ids take 36 bytes instead of 16, the format is not checked, and a consumer that joins the two outputs must cast one side (`cast(order_id as varchar)`), as I did to find the extra order. `ordered_at` is also different: Spark writes timestamps in Parquet as INT96 by default, which DuckDB reads as a `timestamp` without time zone; the values are UTC but the schema does not say it anymore. The same silver layer should have one schema contract, whatever the engine.

### Q12. Compare the output layout. Which one is easier for a consumer to read? Which one scales?

dbt/DuckDB writes one object per model (`orders.parquet`, 73 KB). Spark writes a directory with one `part-` file per partition, plus `_SUCCESS` (`orders.parquet/part-00000-...`, 139 KB). One file is easier to read: one path, no glob (`*.parquet`), any tool can open it. The directory scales: each task writes its file in parallel and a reader can read the files in parallel, while one single file must be written by one process.

### Q13. The dbt lab validates the silver layer with tests. What did this lab use instead, and what would you need to write to get the same guarantees?

The Spark lab only uses a few `assert` on row counts (bronze vs silver, Parquet vs DataFrame). They do not check uniqueness, nulls, accepted values or foreign keys, and they stop the script with a Python error, without a report. To get the same guarantees I would need to write each check as a query that returns the bad rows (for example `groupBy("order_id").count().filter("count > 1")` for `unique`, a left anti join for `relationships`), run them after each build, and stop the next steps on failure. Or use a data quality tool (Great Expectations, Soda) or run dbt with the `dbt-spark` adapter.

### Q14. `ref()` and `source()` give dbt the dependency graph of the models. What plays this role in the notebook?

The order of the cells and the Python variables (`stg_orders` is built from `orders_typed`, built from `orders_bronze`). The dependencies are only implicit: nothing checks them, a cell run out of order uses old variables, and there is no lineage graph or documentation. Spark builds a DAG, but only inside one query plan, not between the datasets of the project.

### Q15. Compare the time to build the silver layer with `dbt build` and with the cells of this lab. Why are these numbers not directly comparable?

`dbt build` of the whole project (seed, 2 views, 21 tests) took about 4.6 s. For Spark I did not measure the full silver build in one number. They are not comparable: dbt only creates views (no data is written; the work happens later at query time), while Spark computes and writes Parquet and Iceberg files; the Spark time includes starting the JVM, the session and downloading the Iceberg jar; the dbt time includes the tests; and with 2829 rows both times are mostly fixed costs, not real processing.

### Q16. Would you choose dbt with DuckDB or Spark for: a daily batch of 10 GB, a daily batch of 10 TB, a team of analysts who only know SQL?

- **10 GB per day**: dbt + DuckDB. It fits on one machine, it is simple, cheap and fast, and it has tests and lineage.
- **10 TB per day**: Spark (or a distributed warehouse). One machine cannot process this in time; it needs a cluster.
- **Analysts who only know SQL**: dbt, because everything is SQL with tests and docs. If the data is big, dbt can still use Spark or a warehouse as the engine (`dbt-spark`, `dbt-databricks`).

## Exercises

### E1. `birthdate_is_valid`, with the DataFrame API and with Spark SQL

```python
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_order_at"))
users_checked_df = (stg_users.join(first_orders, on="user_id", how="left")
    .withColumn("birthdate_is_valid",
                F.coalesce(F.col("birthdate") <= F.to_date("first_order_at"), F.lit(True))))
```

```sql
WITH first_orders AS (
    SELECT user_id, min(ordered_at) AS first_order_at FROM stg_orders GROUP BY user_id
)
SELECT u.*, f.first_order_at,
       coalesce(u.birthdate <= to_date(f.first_order_at), true) AS birthdate_is_valid
FROM stg_users u LEFT JOIN first_orders f ON u.user_id = f.user_id
```

The two versions give the same result: `exceptAll` in both directions returns 0 rows. Result: 45 users valid, 5 not valid (`nicolejohnson`, `dayderek`, `ronaldstephens`, `cgrimes`, `charleslandry`), born between 2022 and 2026 while their first order is in 2020.

Choices: I use a left join to keep users with no order; for them `first_order_at` is NULL and the comparison gives NULL, so I use `coalesce(..., true)` (no order = no proof that the date is wrong). I compare dates with `to_date()` because `birthdate` is a date and `ordered_at` a timestamp.

Compared with the dbt lab: the singular test `assert_orders_after_user_birth` failed on the orders (one row per bad order) and stopped the pipeline unless `severity: warn`. Here I keep all the rows and add a flag column per user, so the consumers can filter. The cause is the same: Faker makes birthdates relative to today, but the orders are in 2020. The source is wrong, so I do not change Bronze, I only flag it in Silver.

### E2. `zip_code` with a Python UDF, a `pandas_udf` and `regexp_extract()`, timed

Test data: the 50 addresses x 20 000 = 1 000 000 rows, cached. To be sure the column is really computed, I write with the `noop` format (a `count()` could let Catalyst drop the unused column). Best of 3 runs:

| Method | Best time | Runs |
|---|---|---|
| `regexp_extract()` (native) | 1.10 s | 1.76, 1.10, 1.18 |
| `pandas_udf` | 2.87 s | 6.64, 2.87, 2.93 |
| Python `udf` | 3.53 s | 4.28, 3.53, 3.73 |

The three methods give the same zip codes on the 50 users.

The native function is about 3x faster: it runs inside the JVM and Catalyst can optimize it. With a Python UDF, each row is serialized, sent to a Python worker process, computed and sent back, row by row. The `pandas_udf` sends batches of rows with Apache Arrow, so it is faster than the plain UDF, but only by about 20% here: pandas string functions (`.str.split`, `.str.extract`) still loop in Python. A `pandas_udf` helps more for numeric work. The first run is slower because Spark must start the Python workers and Arrow.

Rule: first use the built-in functions of `pyspark.sql.functions`; if a UDF is really needed, use a `pandas_udf`. One difference to know: when there is no match, `regexp_extract()` returns `""` but my UDFs return `None`.

### E3. Cache the bronze DataFrames and check the Spark UI

```python
users_bronze.cache()
orders_bronze.cache()
```

Time for `stg_users.count()` + `stg_orders.count()`:

| Run | Time |
|---|---|
| no cache #1 | 1.13 s |
| no cache #2 | 0.64 s |
| cache fill (first action after `cache()`) | 0.84 s |
| cache #1 | 0.48 s |
| cache #2 | 0.48 s |

Without cache, each action reads the CSV from S3 again. The plan shows that `orders.csv` is even read **twice** per action: one `FileScan csv` for each side of the `Union` (all orders + the `limit(3)` duplicates). With the cache, both become `InMemoryTableScan` over an `InMemoryRelation`, and the time goes down from 0.64 s to 0.48 s (about 25%, compared with the second run without cache; the first run at 1.13 s also includes the start-up cost). The `FileScan` stays under the `InMemoryRelation`: it is the recipe to rebuild the cache if a partition is lost. The default level for a DataFrame is `MEMORY_AND_DISK` (for an RDD, `cache()` is `MEMORY_ONLY`). The Storage tab of the Spark UI shows the two cached DataFrames after the first action.

Trap: `stg_orders.explain()` still showed `FileScan csv` after `cache()`, because `stg_orders` had already computed its plan before the cache and `explain()` shows this old plan. A new query (`stg_orders.select("*").explain()`, or the `count()`) builds a new plan and uses the cache.

### E4. Append to the Iceberg table, snapshots, `VERSION AS OF`, compare with Parquet

```python
before_id = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]
new_order.writeTo("lab.silver.orders").append()
spark.sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {before_id}")
new_order.write.mode("append").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
```

`new_order` is one order made with `spark.createDataFrame(..., schema=stg_orders.schema)`, so the types are the same as the table (`quantity` stays `int`).

| | Before | After append | Old version |
|---|---|---|---|
| Iceberg `lab.silver.orders` | 2829 | 2830 | 2829 with `VERSION AS OF 4870172577030688146` |
| Parquet directory | 2829 | 2830 | not possible |

Snapshots after the append:

| snapshot_id | parent_id | operation |
|---|---|---|
| 4870172577030688146 | NULL | overwrite |
| 7610126210941041453 | 4870172577030688146 | append |

Iceberg does not change old files: the append writes a new data file, a new manifest, a new snapshot and a `v2.metadata.json`, then moves the pointer. The old snapshot still lists the old files, so I can read the table as it was (time travel), like a Git history (`parent_id`). With a plain Parquet directory, the append only adds one more `part-` file in the folder. There is no version and no history: after the append, the old state is gone, and a reader during the write could see a partial result.
