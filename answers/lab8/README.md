# Lab 8 – Explore Iceberg tables with Spark: answers

Setup: Onyxia `vscode-pyspark`, Spark 4.1.1, Scala 2.13, Iceberg `iceberg-spark-runtime-4.1_2.13:1.11.0`, Hadoop catalog `lab` in `s3a://<bucket>/warehouse`. The session is created from `python3`. The tables `lab.silver.orders` and `lab.silver.users` come from lab 7: `orders` already has 2 snapshots (create + the append of lab 7 exercise 4) and 2830 rows, so my numbers differ from the lab text (5000 orders).

## Create an Iceberg table

The table `lab.silver.orders` already exists (created in lab 7), so I did not run `createOrReplace()` again: it would replace my history. `SHOW TABLES IN lab.silver` shows `orders` and `users`. Query through the catalog: 2830 orders, quantity 8618 (2829 orders from the silver layer + 1 order appended in lab 7 exercise 4). Unlike a Parquet directory, `lab.silver.orders` is a table known by the catalog: I use its name, not a path.

## Inspect the files

### Q1. Which files contain the actual rows?

The Parquet files in `data/`: `00000-69-...parquet` (84 KB, 2829 rows, written when the table was created) and `00003-203-...parquet` (1.9 KB, 1 row, written by the append).

### Q2. Which files describe the table?

The files in `metadata/`:

- `version-hint.text`: the number of the current version (`2`), used by the Hadoop catalog;
- `v1.metadata.json`, `v2.metadata.json`: one per commit, with the schema, the partition spec, the properties, the list of snapshots and the current snapshot id;
- `snap-<snapshot id>-....avro`: the manifest list of each snapshot (2 files);
- `<uuid>-m0.avro`: the manifests, which list the data files with their row count and min/max/null stats per column (2 files, one per write).

A reader follows the chain: `version-hint.text` → `v2.metadata.json` → manifest list → manifests → data files.

### Q3. Why does an Iceberg table need more than the Parquet files?

Parquet files are only data, with a schema per file. They do not say which files are part of the table now, which version is the current one, or what changed. Without metadata a reader must list the directory, and it can see half-written files during a write. Iceberg adds a list of the exact files of each version (snapshots), atomic commits, the history (time travel), one schema for the whole table that can evolve, and stats per file to skip files when reading.

## Explore Iceberg metadata

### Q4. How many snapshots does the table currently have?

2 snapshots: `4870172577030688146` (`overwrite`, 2829 rows added, total 2829, the creation in lab 7) and `7610126210941041453` (`append`, 1 row added, total 2830), whose `parent_id` is the first one.

### Q5. How many data files belong to the current table?

2 data files: one with 2829 rows (84 055 bytes) and one with 1 row (1 923 bytes). The `files` table only lists the files of the current snapshot.

### Q6. What information does the `snapshots` table contain that the Parquet files do not?

For each version of the table: the snapshot id, the parent snapshot (the history chain), the commit time, the operation (`overwrite`, `append`...) and a summary (rows and files added or deleted, total rows, total size), and the manifest list. A Parquet file only knows its own rows and schema: it does not know which version of the table it belongs to, when it was added, or what other files form the table.

## Create a second snapshot

### Q7. How many snapshots are there now?

3 snapshots: `4870172577030688146` (overwrite, 2829), `7610126210941041453` (append 1 → 2830) and the new `1310469305448366123` (append 3 → 2833), each with the previous one as `parent_id`. The 3 new orders are copies of existing rows with a new `order_id` (`uuid()`), so the table keeps one row per order: 2833 rows and 2833 distinct ids.

### Q8. Did the original Parquet files disappear?

No. The append wrote one new data file (`00000-5-...parquet`, 3 rows) next to the two old ones (2829 rows and 1 row), which are not changed. The new snapshot lists the 3 files; the old snapshots still list only their own files. Iceberg never modifies a data file: it only adds files and new metadata.

### Q9. Why does Iceberg create a new snapshot instead of simply replacing the table?

Because a commit must be atomic and safe for readers. The new snapshot is written next to the old one, then one pointer moves to it: a reader that started before the commit keeps reading the old snapshot, and a new reader sees the new one complete, never a half-written table. Keeping the old snapshots also gives the history: I can read an old version (time travel), roll back after a bad write, or see what changed. And it is cheap: the new snapshot reuses the old data files and only adds the new ones, it does not copy the table.

## Time travel

### Q10. What does time travel read?

Time travel reads the table as it was at an old snapshot. Iceberg opens the metadata of that snapshot (its manifest list and manifests) and reads only the data files listed there. Results with `VERSION AS OF <snapshot_id>`: 2829 rows (first snapshot), 2830 (second), 2833 (third = current). With `TIMESTAMP AS OF '2026-10-13 00:00:00'`, Iceberg takes the last snapshot committed before this date: 2830 rows. `lab.silver.orders.files VERSION AS OF <first snapshot>` shows that the first version only uses one file, the one with 2829 rows.

### Q11. Why can Iceberg access an older version without making a complete copy of the table?

Because snapshots share the data files. A new snapshot does not copy the table: it reuses the old files and only lists the new ones in its manifests. The data files are never changed, so the files of an old snapshot are still there, and the old snapshot is only a few small metadata files (here about 4 KB of manifest list + 7 KB of manifest) pointing to them. Reading an old version only means following another pointer.

### Q12. What would happen if the old snapshot's metadata and data files were physically deleted?

The old version could not be read anymore: `VERSION AS OF` on this snapshot would fail with a "file not found" error, or the snapshot would not exist anymore in the metadata. If only data files were deleted by hand, the metadata would point to missing files and even the current table could break, when these files are still used by the current snapshot (here the 2829-row file is used by all 3 snapshots). This is why old versions must be removed with Iceberg itself: `expire_snapshots` removes old snapshots and deletes only the files that no other snapshot uses, and `remove_orphan_files` cleans the files not listed anywhere. After that, time travel is only possible for the snapshots that are kept.

## Schema evolution

### Q13. Did the existing Parquet files have to be rewritten?

No. `ALTER TABLE lab.silver.orders ADD COLUMN source STRING` only wrote a new metadata file (`v4.metadata.json`): no data file was rewritten and no snapshot was created, because the data did not change. The 3 old Parquet files have no `source` column; when Spark reads them, Iceberg returns `NULL` for this column (2833 rows with `source = NULL`). The new row written with `INSERT` (`v5.metadata.json`, new snapshot) is in a 4th file that has the column (`source = 'spark'`).

Change from the lab: the `INSERT` of the lab copies an existing row with the same `order_id`, which makes a duplicate in a table with one row per order. I used `uuid()` for the new row: 2834 rows and 2834 distinct `order_id`.

### Q14. Where is the new schema recorded?

In the table metadata file, `vN.metadata.json`. `v5.metadata.json` keeps the list of all the schemas and the current one: schema 0 = `order_id, user_id, ordered_at, quantity, product`, schema 1 = the same + `source`, and `current-schema-id: 1`. Each snapshot also records the schema id used when it was written. Iceberg identifies columns by a field id, not by name or position, so a column can be added, renamed or reordered without breaking the old files.

### Q15. Why is schema evolution a table-level operation rather than simply changing a Parquet file?

Because a table is many files written at different times, and all of them must be read with one schema. If I only changed one Parquet file, the other files would keep the old schema, and readers would see different schemas, or fail. Iceberg changes the schema once, in the table metadata, with an atomic commit, and maps the old files to the new schema when reading (missing columns = `NULL`, matched by field id). So adding a column is instant, the old files are not rewritten, and every reader and engine sees the same schema at the same time. It also keeps the old schemas, so time travel reads old snapshots with their own schema.

## Compare Iceberg with Parquet

### Q16. Which one is simply a collection of Parquet files?

`silver/spark/orders.parquet/`: it only has `part-...snappy.parquet` files (one per partition written, plus the extra file of the lab 7 append) and a `_SUCCESS` marker. Nothing says which files form the table or which version is current: a reader lists the directory and reads everything.

### Q17. Which one has table metadata and snapshots?

`warehouse/silver/orders/`: the Parquet files in `data/` plus `metadata/` with `v1` to `v5.metadata.json` (schema, snapshots, current snapshot id), one manifest list per snapshot (`snap-*.avro`), the manifests (`*-m0.avro`, list of data files with stats), and `version-hint.text`. The table has 4 snapshots and 2 schemas.

### Q18. How would you safely replace a Parquet directory while another query is reading it?

Never overwrite the files in place. Write the new version in a new directory (for example `orders.parquet/v=2026-10-17/`), check it, then switch the readers to it in one step: change a pointer that readers use (a view, a symlink-like "current" path stored in a small file, or a Hive Metastore table location with `ALTER TABLE ... SET LOCATION`). The old directory is deleted only later, when no query uses it. Even so, there is no real transaction: this is exactly what Iceberg does for us.

### Q19. How does Iceberg make the table state visible atomically?

A writer first writes all the new data files and the new metadata (manifests, manifest list, `vN+1.metadata.json`). Readers do not see these files yet, because no metadata points to them. Then the commit changes one single pointer to the new metadata file (here `version-hint.text`; with a catalog service, an atomic update in the catalog). Before this switch readers see the old version, after it they see the new one, never a mix. If the commit fails, the new files are just orphans and the table is unchanged.

### Q20. Why is a catalog needed once several engines need to share the same Iceberg table?

With the Hadoop catalog, the pointer to the current version is a file in the bucket, and object storage has no atomic "compare and swap": two engines (Spark, Trino, DuckDB, Flink) that commit at the same time can overwrite each other's commit. A catalog service (REST catalog, Apache Polaris, Nessie, Hive Metastore, AWS Glue) keeps the pointer of each table and updates it atomically: a conflicting commit fails and is retried. It is also the one place where all engines find the tables by name (`lab.silver.orders`) instead of a path, with the same view of the current version, and where access rights are managed.

## Exercises

### E1. Create `lab.silver.users` as an Iceberg table

`lab.silver.users` was already created in lab 7 (`stg_users.writeTo("lab.silver.users").createOrReplace()`), so I checked it instead of replacing it (that would replace its history): `SHOW TABLES IN lab.silver` lists it, 10 columns (`user_id, username, name, sex, email, birthdate, street, city, state, zip_code`), 50 users, 1 snapshot (`overwrite`, 50 rows).

### E2. Append a new batch of users and inspect the snapshots

```python
new_users = (users.limit(3)
             .withColumn("user_id", F.expr("uuid()"))
             .withColumn("username", F.concat(F.col("username"), F.lit("_batch2"))))
new_users.writeTo("lab.silver.users").append()
```

New `user_id` and `username` so the table keeps one row per user. Snapshots: `overwrite` 50 (lab 7) → `append` +3 → total 53. The append wrote one new data file and a new snapshot; the first file is not changed.

### E3. Query the table before and after the append using time travel

With the first and the last snapshot ids: `VERSION AS OF <first>` → 50 users, `VERSION AS OF <after the append>` → 53 users. `SELECT username ... VERSION AS OF <after> EXCEPT SELECT username ... VERSION AS OF <before>` returns the 3 new usernames ending with `_batch2`. Time travel reads each version from its own list of files, so both versions can be read at the same time without any copy.

### E4. Add a `country` column and write new data using the evolved schema

```python
spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
users = spark.table("lab.silver.users")   # read again AFTER the ALTER
(users.limit(1)
     .withColumn("user_id", F.expr("uuid()"))
     .withColumn("username", F.lit("newuser_country"))
     .withColumn("country", F.lit("US"))
     .writeTo("lab.silver.users").append())
```

Result: 11 columns, 53 users with `country = NULL` (old files, not rewritten) and 1 with `US`; 54 users, 54 distinct ids; snapshot row totals 50 → 53 → 54 (3 snapshots). The `ALTER TABLE` made no snapshot, only a new metadata file.

First try failed with `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH`: my `users` DataFrame was created before the `ALTER TABLE`, so its plan still had 10 columns. Spark 4 checks that the plan matches the table at write time and refuses to write with an old schema. Reading the table again (`spark.table(...)`) after the schema change fixed it, and nothing was written by the failed try (still 53 rows).

### E5. Compare the S3 layout of the users and orders tables

Both tables have the same layout under `warehouse/silver/<table>/`: `data/` with the Parquet files, and `metadata/` with `vN.metadata.json`, one manifest list `snap-<id>-*.avro` per snapshot, the manifests `*-m0.avro`, and `version-hint.text`.

- `users`: 3 data files (50 + 3 + 1 rows), 3 snapshots, metadata `v1` to `v4` (create, append, `ALTER TABLE`, append).
- `orders`: 4 data files (2829 + 1 + 3 + 1 rows), 4 snapshots, metadata `v1` to `v5`.

The number of data files and snapshots follows the number of writes, not the size of the table: each append adds a small file. With many small appends this makes many small files, which slows down reads; Iceberg has maintenance procedures for this (`rewrite_data_files` to compact the files, `expire_snapshots` to remove old snapshots).

### E6. Explain the difference between Parquet, Iceberg, and an Iceberg catalog

- **Parquet** is a file format: a columnar, compressed file with its own schema and stats. A directory of Parquet files is just files: no list of which files form the table, no versions, no transactions; a reader lists the directory, and a write in progress can be seen half done.
- **Iceberg** is a table format on top of data files (usually Parquet): metadata files (`vN.metadata.json`, manifest lists, manifests) list exactly which files form each version of the table (snapshot), with one schema for the whole table. This gives atomic commits, time travel, schema evolution without rewriting files, and file stats to skip data.
- **An Iceberg catalog** keeps, for each table name (`lab.silver.orders`), the pointer to its current metadata file, and changes it atomically at each commit. This is what makes commits safe when several jobs or engines write the same table, and lets every engine (Spark, Trino, DuckDB, Flink) find the tables by name. Here the catalog `lab` is of type `hadoop`: the pointer is `version-hint.text` in the bucket, simple but without real locking; a REST catalog (Polaris, Nessie, Glue...) is needed for concurrent writers.
