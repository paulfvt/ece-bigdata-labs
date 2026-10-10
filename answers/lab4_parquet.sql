-- Parquet export
COPY orders TO (getvariable('bucket') || '/analytics/orders.parquet') (FORMAT parquet);
SELECT path_in_schema, type, compression, total_compressed_size, total_uncompressed_size
FROM parquet_metadata(getvariable('bucket') || '/analytics/orders.parquet');

-- Hive partitioning
COPY orders TO (getvariable('bucket') || '/analytics/orders_by_product') (FORMAT parquet, PARTITION_BY (product));
SELECT file FROM glob(getvariable('bucket') || '/analytics/orders_by_product/**');
EXPLAIN ANALYZE
SELECT count(*)
FROM read_parquet(getvariable('bucket') || '/analytics/orders_by_product/*/*.parquet')
WHERE product = 'cookie';

-- CSV vs. Parquet at scale
COPY (FROM read_csv(getvariable('bucket') || '/large/orders.csv', strict_mode = false))
TO (getvariable('bucket') || '/large/orders.parquet') (FORMAT parquet);
SELECT count(*) FROM read_parquet(getvariable('bucket') || '/large/orders.parquet');
SELECT row_group_id, row_group_num_rows, stats_min, stats_max
FROM parquet_metadata(getvariable('bucket') || '/large/orders.parquet')
WHERE path_in_schema = 'date';
