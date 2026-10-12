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

With one more comma, `split(",")` gives 6 values instead of 5, so the line `order_id, user_id, date, quantity, product = ...` raises `ValueError: too many values to unpack`. But not when I write the `map`: it is a lazy transformation. The error comes at the first action that reads this line, in the Python worker of an executor, and the whole job fails. A `take(3)` can even work if the bad line is later in the file. Also, `date` stays a string: nothing checks its format, and a bad `quantity` would also fail only at the action.

### Q8. Where does the DataFrame API perform the equivalent check, and when?

In the DataFrame API, the CSV reader does this work. It understands the CSV format (quotes, header), so a comma inside quotes is not a problem. The check is done when the file is parsed against the schema (inferred or given with `StructType`), when an action runs (with `inferSchema=True`, Spark also reads the file once when I call `spark.read.csv`). A bad value does not crash the job by default: with `mode="PERMISSIVE"` it becomes `null`. I can choose `DROPMALFORMED` to drop the line, or `FAILFAST` to stop with an error.

## Key-value RDDs: total quantity per user

### Q9. What does `groupByKey` shuffle instead of `reduceByKey`, and why is that more expensive?

Both give the same result (for example `('ce9e1a11-...', 253)`), and both jobs have 2 stages, because a shuffle is needed: all the values of one `user_id` must go to the same partition.

`reduceByKey` first adds the quantities inside each partition, then it shuffles only one value per user and per partition (here max 50 pairs per partition). `groupByKey` shuffles all the pairs (here the 2829 pairs), and the sum is done only after. So more data goes on the network and on disk, and one executor must keep all the values of a key in memory. In the Spark UI, Shuffle Write was <X> for `reduceByKey` and <Y> for `groupByKey`.

### Q10. For which aggregations would `groupByKey` be unavoidable?

When the result needs all the values of a key together, and cannot be built by combining two values at a time. For example: the median quantity per user, the full sorted list of orders of each user, or the most common product of each user.

## Manual deduplication

### Q11. What plays the role of `partitionBy()` here?

Here `reduceByKey` plays this role. Its shuffle sends all the orders with the same `order_id` to the same partition, like `PARTITION BY order_id`. Then `most_recent` compares them two by two and keeps the newest one, like `ORDER BY ... DESC` + `row_number() = 1`. Result: `2829 2829`, because the generator makes no duplicates.

Note: `date` is a string, not a timestamp. The `>` comparison works only because the dates use the ISO format (`2020-01-01 00:02:17...+00:00`) with the same time zone, so alphabetical order = time order. With another format or mixed time zones, the result would be wrong without any error.

### Q12. Which version states the intent more directly: the window function, or `reduceByKey` with a custom combiner?

The window function is clearer. `row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) = 1` says directly "keep the newest row of each order". With `reduceByKey`, I must write a custom function, build key-value pairs and take `.values()`, and the reader must understand the code to see the goal. Also Spark cannot optimize my Python function.

## Joining orders with users

### Q13. What would need to be true of how `orders_kv` and `users_kv` are partitioned for Spark to skip the shuffle?

Result: `enriched.count()` = 2829, the same as the number of orders. Each element is `(user_id, (order, username))`. All orders found their user, and no order was duplicated because `users_kv` has only one line per `user_id`.

To skip the shuffle, the two RDDs must already be partitioned in the same way: same key (`user_id`), same partitioner and same number of partitions, for example both with `.partitionBy(8)` before the join (and cached, so this is not done again). Then the rows with the same key are already in the same partition, and the join becomes a narrow transformation. If only one RDD is partitioned like this, Spark shuffles only the other one.

### Q14. Replace `join()` with `leftOuterJoin()`. What changes if a `user_id` in `orders` has no matching user?

`join()` is an inner join: an order with an unknown `user_id` disappears from the result. With `leftOuterJoin()`, the order stays, and the user side is `None`: `(user_id, (order, None))`. Here nothing changes (still 2829), because all the orders have a user. `leftOuterJoin()` is useful to find these orphan orders, with a filter on `None`.

### Q15. What is the risk of calling `.collect()` on `enriched` directly, on a much larger dataset?

`collect()` sends all the rows of all the partitions to the driver, into one Python list. On a big dataset this does not fit in the driver memory, so the driver crashes (out of memory). A join can also make more rows than the inputs, if a key is not unique. It is better to filter or aggregate first, and to use `take(n)`, `count()`, or write the result to storage.

## Exercises

### E1. `user_quantity` with `aggregateByKey`, then the average order quantity per user

```python
pairs = orders_rdd.map(lambda o: (o["user_id"], o["quantity"]))

user_stats = pairs.aggregateByKey(
    (0, 0),                                    # start: (order_count, total_quantity)
    lambda acc, q: (acc[0] + 1, acc[1] + q),   # inside one partition: add one order
    lambda a, b: (a[0] + b[0], a[1] + b[1]),   # after the shuffle: merge two partitions
)
user_avg = user_stats.mapValues(lambda acc: round(acc[1] / acc[0], 2))
```

Results (first 3 users): `(4, 12)` → 3.0, `(12, 40)` → 3.33, `(85, 253)` → 2.98. The totals are the same as with `reduceByKey`.

`reduceByKey` needs the same type in and out (quantity + quantity). Here the input is a quantity but the result is a pair `(count, total)`, so I use `aggregateByKey`. Like `reduceByKey`, it combines inside each partition before the shuffle, so only one pair per user and per partition goes on the network. I divide only at the end: an average of partial averages would be wrong.

### E2. Fake `user_id`, `leftOuterJoin()`, count the orders with no matching user

```python
fake_order = {"order_id": "00000000-0000-0000-0000-000000000001",
              "user_id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
              "date": "2020-06-01 12:00:00.000000+00:00", "quantity": 3, "product": "bread"}

orders_with_fake = orders_rdd.union(sc.parallelize([fake_order]))   # 2830
joined = orders_with_fake.map(lambda o: (o["user_id"], o)).leftOuterJoin(users_kv)
orphans = joined.filter(lambda kv: kv[1][1] is None)                # kv = (user_id, (order, username))
orphans.count()                                                     # 1
```

Results: 2830 orders with the fake one, 1 orphan order with `leftOuterJoin` (the username is `None`), and 2829 with a normal `join`.

An RDD is immutable, so I do not change `orders_rdd`: I make a new RDD with `union`, which is narrow (no shuffle). The normal `join` removes the orphan order without any error, so I can lose data without seeing it. `leftOuterJoin` + filter on `None` finds it. This is the same check as the dbt test `assert_orders_have_users` (`LEFT JOIN ... WHERE u.user_id IS NULL`).

### E3. Same aggregation with the DataFrame API, compare `.explain()` and `.toDebugString()`

_TODO_

### E4. Timing with and without `.cache()`, Storage tab of the Spark UI

_TODO_
