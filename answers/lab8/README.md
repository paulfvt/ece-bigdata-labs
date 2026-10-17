# Lab 8 – Explore Iceberg tables with Spark: answers

Setup: Onyxia `vscode-pyspark`, Spark 4.1.1, Scala 2.13, Iceberg `iceberg-spark-runtime-4.1_2.13:1.11.0`, Hadoop catalog `lab` in `s3a://<bucket>/warehouse`. The session is created from `python3`. The tables `lab.silver.orders` and `lab.silver.users` come from lab 7: `orders` already has 2 snapshots (create + the append of lab 7 exercise 4) and 2830 rows, so my numbers differ from the lab text (5000 orders).

## Create an Iceberg table

_TODO_

## Inspect the files

### Q1. Which files contain the actual rows?

_TODO_

### Q2. Which files describe the table?

_TODO_

### Q3. Why does an Iceberg table need more than the Parquet files?

_TODO_

## Explore Iceberg metadata

### Q4. How many snapshots does the table currently have?

_TODO_

### Q5. How many data files belong to the current table?

_TODO_

### Q6. What information does the `snapshots` table contain that the Parquet files do not?

_TODO_

## Create a second snapshot

### Q7. How many snapshots are there now?

_TODO_

### Q8. Did the original Parquet files disappear?

_TODO_

### Q9. Why does Iceberg create a new snapshot instead of simply replacing the table?

_TODO_

## Time travel

### Q10. What does time travel read?

_TODO_

### Q11. Why can Iceberg access an older version without making a complete copy of the table?

_TODO_

### Q12. What would happen if the old snapshot's metadata and data files were physically deleted?

_TODO_

## Schema evolution

### Q13. Did the existing Parquet files have to be rewritten?

_TODO_

### Q14. Where is the new schema recorded?

_TODO_

### Q15. Why is schema evolution a table-level operation rather than simply changing a Parquet file?

_TODO_

## Compare Iceberg with Parquet

### Q16. Which one is simply a collection of Parquet files?

_TODO_

### Q17. Which one has table metadata and snapshots?

_TODO_

### Q18. How would you safely replace a Parquet directory while another query is reading it?

_TODO_

### Q19. How does Iceberg make the table state visible atomically?

_TODO_

### Q20. Why is a catalog needed once several engines need to share the same Iceberg table?

_TODO_

## Exercises

### E1. Create `lab.silver.users` as an Iceberg table

_TODO_

### E2. Append a new batch of users and inspect the snapshots

_TODO_

### E3. Query the table before and after the append using time travel

_TODO_

### E4. Add a `country` column and write new data using the evolved schema

_TODO_

### E5. Compare the S3 layout of the users and orders tables

_TODO_

### E6. Explain the difference between Parquet, Iceberg, and an Iceberg catalog

_TODO_
