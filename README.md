# Big Data labs – ECE Paris

My labs for the **Big Data Frameworks** course (ING4, Data & AI major, fall 2026).
The course material is in the [adaltas/ece-bigdata-2026-fall](https://github.com/adaltas/ece-bigdata-2026-fall) repository.

All the labs use the same Python project. Each lab adds something to it.

## What is inside

- `src/chapitre3_labs1_uv/`: the Python code
  - `dataset_users.py` and `dataset_orders.py` create a fake dataset of users and orders (lab uv)
  - `orders_report.py` prints the orders of each month with DuckDB (lab 4)
- `answers/lab3/`: my answers for the S3 lab, and the Kubernetes Job that uploads the data
- `answers/lab4/`: my answers for the DuckDB lab, my SQL queries and their results
- `answers/lab5/`: my answers for the dbt lab
- `lab_medallion/`: the dbt project of lab 5 (models, tests, seed)
- `answers/lab6/`: answers of the RDD lab
- `answers/lab7/`: answers of the Spark DataFrames lab
- `warehouse/silver/*/metadata/`: metadata of the Iceberg tables of lab 7 (the data files are not in Git)

## Setup

The labs run in an Onyxia `vscode-pyspark` service. The project uses [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/paulfvt/ece-bigdata-labs.git
cd ece-bigdata-labs
uv sync
```

## Usage

Create the dataset:

```bash
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
```

Print the monthly orders report. The CSV file must be in the `bronze/` folder of your S3 bucket:

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
uv run orders-report
uv run orders-report -p cookie   # only one product
```

Run the SQL queries of lab 4 with the DuckDB CLI:

```bash
echo "SET VARIABLE bucket = 's3://$LAB_BUCKET_NAME';" > init.sql
duckdb analytics.duckdb -init init.sql -c ".read answers/lab4/queries.sql"
```

## Labs

- Lab uv: Python project and dataset generator
- Lab 3: object storage with S3, upload of the bronze layer
- Lab 4: SQL analytics with DuckDB, CSV vs. Parquet
- Lab 5: bronze to silver with dbt and DuckDB, data tests, seeds and lineage
- Lab 6: Spark RDD API (partitions, lazy evaluation, reduceByKey vs groupByKey, joins)
- Lab 7: bronze to silver with Spark DataFrames, Spark SQL, Parquet and Iceberg
