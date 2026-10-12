# Lab 6 – Introduction to Spark's RDD API: answers

Setup: Onyxia `vscode-pyspark`, Spark 4.1.1, `pyspark --master local[*]` (2 CPU, so `sc.defaultParallelism = 2`).

## RDD creation and partitions

Results: `users_lines` → 2 partitions, `orders_lines` → 2 partitions, `orders_lines.repartition(8)` → 8 partitions.

### Q1. What decides the number of partitions `textFile()` picks by default, for a file this size?

My files are very small (330 KB and 7 KB). One HDFS block is 128 MB, so if Spark only cut by blocks, I would get 1 partition. But I get 2. This is because `textFile()` has a `minPartitions` setting. By default it is `sc.defaultMinPartitions`, which is `min(defaultParallelism, 2)`. Here it is 2. So for small files, this minimum decides, not the file size. For a big file (for example 1 GB), the block size decides: about 1 GB / 128 MB = 8 partitions.

### Q2. `repartition()` and `coalesce()` both change the partition count. Which one shuffles data, and which one doesn't?

`repartition()` does a full shuffle: all the data moves between partitions. It can make more partitions or fewer. `coalesce()` does not shuffle: it only puts some partitions together. So it is cheaper, but it can only reduce the number. For example, `orders_lines.coalesce(8)` stays at 2 partitions. I use `repartition()` to get more parallelism, and `coalesce()` to get fewer partitions, for example before writing files.

## Lazy evaluation and lineage

### Q3. Call `orders_data.count()` a second time. Does the Spark UI show a new stage? Why?

`toDebugString()` shows the lineage: `HadoopRDD` (read the file) → `MapPartitionsRDD` (text lines) → `PythonRDD` (my filter), with 2 partitions. Nothing is computed at this point. `count()` returns 2829 lines.

Yes. Each time I call `count()`, the Spark UI shows a new job with its stage and 2 tasks. Spark does not keep the result of the first count. A transformation is only a plan, so for each action Spark reads the file again and runs the filter again.

### Q4. At which point in this notebook would `.cache()` change that answer?

I must call `orders_data.cache()` before the first action, for example just after the `filter` line. `cache()` is lazy too: the first `count()` computes the data and keeps it in memory. The second `count()` still creates a new job, but it reads the partitions from memory, not from the file. I can see the cached RDD in the Storage tab. If I call `cache()` after the counts, nothing changes until the next action.

## The cost of no schema: the multiline address

### Q5. Why does `spark.read.csv(multiLine=True)` not have this problem, while `sc.textFile()` does?

`users_lines.count()` returns 101: 1 header line + 50 users x 2 lines. The address is in quotes and has a line break inside, so each user is cut in two elements. The second element has no uuid, so I cannot know which user it belongs to.

`textFile()` only cuts the file at each new line. It does not know the CSV format. `spark.read.csv(multiLine=True)` reads the CSV syntax: it sees the quotes and knows that a line break inside quotes is part of the value, not the end of the record.

### Q6. What has to be true of a file format for a line-oriented reader like `textFile()` to be safe to use on it?

One record must always be on one line: no line break inside a value. Formats like JSON Lines or simple logs are safe. Also the reader should not need to know the structure of the file (quotes, header) to find where a record starts and ends.

Workaround of the lab: keep only lines starting with a UUID, so `users_records.count()` = 50. But this filter drops the second half of each user (end of address, mail, birthdate), so only `uuid`, `username`, `name` and `sex` are usable.

## Manual parsing of orders

### Q7. What would happen to this cell if one line in `orders.csv` had an extra comma inside `product`?

_TODO_

### Q8. Where does the DataFrame API perform the equivalent check, and when?

_TODO_

## Key-value RDDs: total quantity per user

### Q9. What does `groupByKey` shuffle instead of `reduceByKey`, and why is that more expensive?

_TODO_

### Q10. For which aggregations would `groupByKey` be unavoidable?

_TODO_

## Manual deduplication

### Q11. What plays the role of `partitionBy()` here?

_TODO_

### Q12. Which version states the intent more directly: the window function, or `reduceByKey` with a custom combiner?

_TODO_

## Joining orders with users

### Q13. What would need to be true of how `orders_kv` and `users_kv` are partitioned for Spark to skip the shuffle?

_TODO_

### Q14. Replace `join()` with `leftOuterJoin()`. What changes if a `user_id` in `orders` has no matching user?

_TODO_

### Q15. What is the risk of calling `.collect()` on `enriched` directly, on a much larger dataset?

_TODO_

## Exercises

### E1. `user_quantity` with `aggregateByKey`, then the average order quantity per user

_TODO_

### E2. Fake `user_id`, `leftOuterJoin()`, count the orders with no matching user

_TODO_

### E3. Same aggregation with the DataFrame API, compare `.explain()` and `.toDebugString()`

_TODO_

### E4. Timing with and without `.cache()`, Storage tab of the Spark UI

_TODO_
