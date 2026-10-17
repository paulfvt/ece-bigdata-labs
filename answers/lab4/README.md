# Lab 4 – SQL analytics with DuckDB: answers

## Q1. The persistent secret is stored in a file of the home directory. What are the risks, and why are they limited on Onyxia?

The file contains my S3 keys in clear text. Any program running as my user can read it (a bad script, a compromised Python package), and I could push it to GitHub by mistake. With these keys, someone can read, change or delete everything in my bucket.

On Onyxia the risk is limited: the keys are temporary and expire after a few hours, they only give access to my own bucket, and the pod is only used by me.

## Q2. How would you restrict the access to S3 for a Kubernetes Job?

Only the Job should get the credentials. I put them in a Kubernetes Secret mounted only in this Job, with an S3 policy that gives the minimum rights (for example read-only on `bronze/`). In production, it is better to give the Job its own identity (a service account) and let the platform give it short credentials, renewed automatically, instead of copying keys.

## Q3. The dataset has 50 users and 2829 unique orders. Why does `approx_unique` return 40 and 3167?

`approx_unique` does not count the distinct values exactly. To count exactly, DuckDB would need to keep every value in memory, which is too expensive on a big file. Instead it uses a probabilistic algorithm (HyperLogLog): it hashes each value and estimates the number of distinct values with a small, fixed amount of memory. The result is an estimate with a random error, so it can be lower (40 instead of 50) or higher (3167 instead of 2829). For an exact result, we use `count(DISTINCT ...)`.

## Q4. The types are inferred from a sample of the file. Which issue may occur with a large file whose first rows are not representative?

DuckDB only reads the first rows (a sample) to guess the type of each column. If the first rows of a column only contain integers, the column becomes `BIGINT`. If a value like `2.5` or `N/A` appears later in the file, it cannot be converted and the query fails in the middle of the file (or the row is skipped or set to NULL if errors are ignored). To avoid this, we can read the whole file for detection (`sample_size = -1`), give the types explicitly (`types = {...}`), or use a format with a schema like Parquet.

## Q5. Why is the `uuid` column barely compressed?

Every `uuid` is random and unique. There is no repeated value, so dictionary encoding is useless, and there is no pattern for the Snappy compression to find. Only the fact that the characters are hexadecimal gives a small gain (113 KB to 102 KB).

## Q6. Why is the compressed size of some columns larger than their uncompressed size?

Compression adds a small overhead (headers of the compressed blocks). The `date`, `quantity` and `product` columns are already compact before compression: binary integers and timestamps, and dictionary encoding for `product`. Snappy cannot reduce them more, so the overhead makes them a few bytes larger.

## Q7. Why is partitioning by `uuid` a bad idea?

It creates one directory and one small file per order: millions of tiny objects on a big dataset. On object storage, every file is a separate HTTP request with its own latency, listing many objects is slow, and each Parquet file has its own footer and compresses badly. Queries almost never filter on a single `uuid`, so the partitioning gives no benefit.

## Q8. Which partition column would you choose for a dataset of orders growing every day?

A date column, for example the day or the month of the order (`date=2026-10-10`). Each day, new data is written in a new partition without touching the old files, and most queries filter on a time range, so the other partitions are skipped. The granularity must keep files big enough: if a day has few orders, the month is better.

## Q9. How many row groups does the file contain, and how many are skipped by the filter `date >= '2100-01-01'`?

With `-u 5000`, the file has about 252,000 orders over 3 row groups, with dates from 2020 to 2048. The max date of every row group is before 2100, so the 3 row groups are skipped: DuckDB only reads the footer. (The lab shows 999,490 rows and dates after 2100, which does not match `-u 5000`.)

## Q10. The orders are sorted by date. What would happen to the efficiency of the filter if they were shuffled?

Each row group would contain dates from the whole period, so its min and max would cover the filter range. The statistics could not exclude any row group, and DuckDB would read the `date` column of all of them. Sorting (or clustering) the data on the filtered column makes the statistics useful.

## Q11. Compare the execution time of the CSV and Parquet queries. Which part of the difference is due to the network, which part to the parsing of the CSV file?

My results (`scale_results.txt`), for the same query (`sum(quantity)` per product):

- CSV: 28.2 MiB downloaded, 1.15 s
- Parquet: 203.5 KiB downloaded, 0.358 s

Parquet is about 3 times faster and downloads about 140 times less data.

- Network: DuckDB downloads the whole CSV file, even if the query uses only 2 columns. With Parquet, it only downloads the footer and the 2 columns it needs, and they are compressed.
- Parsing: CSV is text. DuckDB must cut every line and convert every value to a number or a date. Parquet is binary and already typed, so there is almost nothing to convert.

My measures do not split the 2 parts. To do it, I can run the CSV query on a local copy of the file: this time is the parsing only, and the rest of the 1.15 s is the network.
