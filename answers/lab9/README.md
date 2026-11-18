# Lab 9 – Govern Iceberg tables with Polaris: answers

Setup: Onyxia `vscode-pyspark`, Spark 4.1.1, Scala 2.13, Iceberg `iceberg-spark-runtime-4.1_2.13:1.11.0`, Apache Polaris 1.8.0 deployed with Helm in my namespace (lab 1). The Spark session is created from `python3`, not from the `pyspark` shell: in the shell a session already exists, so `getOrCreate()` returns it and ignores part of the configuration. The session code of the lab is in `~/polaris_session.py` and the principal is chosen when it starts:

```bash
POLARIS_PRINCIPAL=dev python3 -i ~/polaris_session.py      # or analyst, reader
```

My tables come from labs 7 and 8, so my numbers differ from the lab text: `silver.orders` has 2834 rows and 4 snapshots (not 5000), its current metadata is `v5.metadata.json` (not `v1`), and `silver.users` already has a `country` column (added in lab 8, exercise 4).

## Lab 1 – Deploy Polaris (Helm)

Changes from the lab, because of my namespace quota (`requests.cpu 200m`, `requests.memory 200Mi`):

| Pod | Requests in the lab | My requests | Limits (unchanged) |
| --- | --- | --- | --- |
| VS Code | 50m / 200Mi | 10m / 32Mi | 2000m / 2500Mi |
| PostgreSQL | 50m / 128Mi | 10m / 48Mi | 250m / 256Mi |
| Polaris | 100m / 256Mi | 10m / 64Mi | 500m / 512Mi |
| Polaris init container | 50m / 128Mi | 10m / 48Mi | 500m / 512Mi |
| Bootstrap job | 50m / 32Mi | 10m / 16Mi | 200m / 56Mi |

With the requests of the lab, Polaris + PostgreSQL + VS Code need about 250m and 580Mi, more than the quota. A request is only a reservation used by the scheduler to place the pod; the limit is what the pod can really use. So I kept the limits of the lab and lowered only the requests: total 70m / 162Mi. The lower CPU requests also helped the pods to be scheduled while the cluster was full (`0/9 nodes are available: 3 Insufficient cpu`). The trade-off: pods with small requests are the first evicted if a node runs out of memory. The deployment files are in `polaris/deployment/`.

Other notes:

- The first `helm upgrade` changed the requests of pods that were still `Pending`. The rolling update could not start the new Polaris pod next to the old one (quota), so I uninstalled the release and installed it again. `helm upgrade --install ... --wait --timeout 5h` was run with `nohup` in the background, because the bootstrap job is a `post-install` hook: it only runs if the install succeeds.
- Result: `curl http://polaris-mgmt:8182/q/health` → `"status": "UP"`, init container log `Realm 'POLARIS' successfully bootstrapped`, release `deployed`, job `polaris-bootstrap` `Completed`.
- The credentials of `dev` and `analyst` are printed only once, in the logs of the bootstrap job. I saved them in `/home/onyxia/.polaris-credentials.env` and on my computer, never in Git.

## SparkSession with the Polaris catalog

`SHOW NAMESPACES IN lab` → `silver`. `SHOW NAMESPACES IN polaris` → empty.

### Q1. Which component answered each of the two `SHOW NAMESPACES` commands? How can you tell?

`lab` is a Hadoop catalog: there is no server, Spark itself lists the folders under `s3a://<bucket>/warehouse/` in S3 and finds `silver`. `polaris` is a REST catalog: Spark sends an HTTP request to the Polaris server (`http://polaris:8181/api/catalog`), and Polaris answers from its own database (PostgreSQL), where my catalog had no namespace yet. I can tell because the two commands look at the same bucket but give different results: `silver` exists in S3 but not in Polaris. Polaris does not list the bucket, it only knows what was created or registered through it.

### Q2. Where is the credential used: in the Spark session, in the bucket, or in a request to Polaris?

In a request to Polaris. The credential (`clientId:clientSecret` of `dev`) is set in the Spark session configuration (`spark.sql.catalog.polaris.credential`), but it is only used by the Iceberg REST client: it sends it to Polaris (`/api/catalog/v1/oauth/tokens`) to get an OAuth token, then sends this token with every catalog request. Polaris uses the token to know the principal and check its privileges. The bucket never sees this credential: Spark reads and writes the S3 files with my S3A keys (`AWS_ACCESS_KEY_ID`...), because there is no credential vending on this storage (`io-impl = HadoopFileIO`).

## Register the Silver table in Polaris

`version-hint.text` contained `5`, so I registered `v5.metadata.json` (the lab uses `v1`, which would point Polaris to an old version of the table):

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
spark.sql(f"""
    CALL polaris.system.register_table(
      table => 'silver.orders',
      metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v5.metadata.json'
    )
""").show()
```

Result: `current_snapshot_id = 439826305722934575`, `total_records_count = 2834`, `total_data_files_count = 4`. `count(*)` gives 2834 through `polaris` and through `lab`.

### Q3. What did `register_table` write, in Polaris and in the bucket?

In Polaris: the namespace `silver` and a table entity `silver.orders` whose pointer is the path of `v5.metadata.json`. This is stored in the Polaris database (PostgreSQL). In the bucket: nothing. `warehouse/silver/orders/` had 18 objects before and 18 after. No data file and no metadata file was copied or written: Polaris adopts the existing metadata file, with all its history (the 4 snapshots of labs 7 and 8 are visible in `polaris.silver.orders.snapshots`).

### Q4. Why is it dangerous to keep writing to this table through the `lab` catalog once it is registered in `polaris`?

Because now the table has two independent pointers to its current version: `version-hint.text` for `lab`, and the Polaris database for `polaris`. A commit through `lab` writes `v6.metadata.json` and moves `version-hint.text`, but Polaris still points to `v5` and does not see the new data. If someone then commits through `polaris`, Polaris writes a new metadata file based on `v5`: the history splits in two, and the commit made through `lab` is lost for everyone who reads through Polaris. Maintenance can make it worse: `expire_snapshots` run from one catalog can delete files that the other one still uses. A table must have one catalog that owns its commits, so after the registration I write only through `polaris`.

## Create tables as `dev`

`SHOW TABLES IN polaris.silver` → `orders`. Snapshots of `orders` through Polaris: the 4 snapshots of labs 7 and 8 (`4870172577030688146` overwrite, `7610126210941041453`, `1310469305448366123`, `439826305722934575` append). Then `polaris.silver.users` was created from `lab.silver.users` with `writeTo(...).createOrReplace()`: 54 users.

### Q5. Where did Polaris store the files for `silver.users`, and why?

In `s3://<bucket>/warehouse/silver/users/`: Polaris builds the location from the `default-base-location` of the catalog (`s3://<bucket>/warehouse/`, set by the bootstrap job) + the namespace + the table name. The new metadata file has the naming of a REST catalog, `00000-677fa757-....metadata.json` (version number + UUID), and Polaris does not update `version-hint.text`, because its pointer is in its database.

This is the same folder as the old `lab.silver.users` of lab 7 (its `v1` to `v4.metadata.json` and `version-hint.text` are still there). So two different Iceberg tables, one per catalog, share the same folder. It works because each catalog reads only the files listed in its own metadata, but it is fragile: a `DROP TABLE ... PURGE` from one catalog could delete files of the other one. The lab does not mention it.

### Q6. Both tables are in your bucket. What is the difference between "registered" and "created through Polaris" for the data files?

In both cases the data files are in my bucket and Polaris never copies data: it only keeps a pointer to a metadata file. The difference is who wrote the files and who owns the table:

- `silver.orders` (registered): the data files and metadata files were written before, by Spark with the Hadoop catalog. Polaris only adopted the existing `v5.metadata.json`, at the location chosen by the Hadoop catalog. The files were not changed.
- `silver.users` (created through Polaris): Spark wrote the Parquet files, but the commit went through Polaris, which chose the location and wrote the `metadata.json` itself, with its own S3 credentials (secret `polaris-s3`). The table belongs to Polaris from its first version.

## Schema change as `dev`

`ALTER TABLE polaris.silver.users ADD COLUMN country STRING` failed with `FIELD_ALREADY_EXISTS`: the column already existed in `lab.silver.users` (lab 8, exercise 4) and was copied with the table. Spark refused it during analysis, before calling Polaris. I added `country_code` instead:

```python
spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country_code STRING")
```

### Q7. Which metadata file records the change? Is it the same file that `register_table` pointed to?

A new file, `00001-be00ab80-....metadata.json`, written by Polaris next to `00000-677fa757-....metadata.json`. It is not the file that `register_table` pointed to: that one was `v5.metadata.json`, of another table (`orders`), and Iceberg never modifies a metadata file. Each commit writes a new metadata file, and Polaris moves its pointer to it.

### Q8. Which privilege does Polaris check for this statement?

Polaris checks the operation `ADD_TABLE_SCHEMA`, a change of the table metadata. It is covered by `TABLE_WRITE_PROPERTIES` on the table, the privilege that allows all the metadata changes of a table. `dev` has it through `CATALOG_MANAGE_CONTENT` (granted to `catalog_contributor`), which includes the table read and write privileges for the whole catalog. The same statement as `analyst` fails with `is not authorized for op ADD_TABLE_SCHEMA` (see below).

## Read as the analyst

As `analyst`: `SHOW TABLES IN polaris.silver` → `orders`, `users`, and the `GROUP BY product` works (6 products, 2834 orders). The writes fail:

```
INSERT INTO polaris.silver.orders ...
ForbiddenException: Forbidden: Principal 'analyst' with activated PrincipalRoles '[data_analyst]' and activated grants via '[data_analyst, catalog_reader]' is not authorized for op ADD_TABLE_SNAPSHOT

ALTER TABLE polaris.silver.users ADD COLUMN phone STRING
ForbiddenException: ... is not authorized for op ADD_TABLE_SCHEMA
```

### Q9. Which privilege is missing for the `INSERT`? Which privileges does `catalog_reader` provide?

The `INSERT` adds a snapshot to the table (`ADD_TABLE_SNAPSHOT`), which needs `TABLE_WRITE_DATA`. `catalog_reader` only gives `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and `CATALOG_READ_PROPERTIES`: reading the data and the metadata of the tables, and the properties of the catalog. The error message shows the whole RBAC chain: principal `analyst` → principal role `data_analyst` → catalog role `catalog_reader`.

### Q10. Check the snapshots of `silver.orders` as `dev`. Did the failed `INSERT` create one?

No. As `dev`, `silver.orders` still has the same 4 snapshots (last one `439826305722934575`), 2834 rows, and the same 4 Parquet files in `data/`.

The order of events is important. The log shows `IcebergBatchWrite ... is aborting`: Spark first wrote the new Parquet file directly to S3 with my S3A keys, without asking Polaris, then asked Polaris to commit the new snapshot, and got the 403. Then Spark aborted the write and deleted its file. Polaris refused the commit, and the commit is the only step that makes a new version exist. But the cleanup of the file was done by Spark itself, not by Polaris: a client that does not clean up would leave an orphan file.

## What Polaris enforces, and what it cannot enforce here

Polaris controls the catalog: who can see a table, load its metadata, and move its pointer to a new version (commits, schema changes, create and drop). It cannot control the storage: here there is no credential vending, every principal reads and writes the files with the same S3 keys (mine). With these keys, `analyst` cannot commit through Polaris, but can still read any file of the bucket directly, write files, or delete data files and break the tables. To really enforce the rights on the data, Polaris must hand out short-lived S3 credentials limited to the location of each table (credential vending, with STS), and the users must not have their own keys to the bucket.

## Commit the Iceberg table metadata

```bash
aws s3 --profile default sync "s3://$LAB_BUCKET_NAME/warehouse/" ./warehouse/
git add ./warehouse/silver/orders/metadata ./warehouse/silver/users/metadata
git commit -m "feat(polaris-lab): add Polaris table metadata"
```

23 metadata files added: `v2` to `v5.metadata.json`, manifests and manifest lists of `orders`; `00000-` and `00001-....metadata.json`, manifests and manifest lists of `users`. The Parquet files are not in Git (`warehouse/**/data/` in `.gitignore`). The Polaris deployment was committed separately, without the downloaded charts (`polaris/deployment/charts/` in `.gitignore`).

## Exercises

### E1. Remove the column and check the change in the snapshots

`country` is a real column of my data (lab 8), so I removed the column I added in this lab, `country_code`:

```python
spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country_code")
```

- Snapshots: still 1 (`1239157569529666872`, the creation). No new snapshot.
- Metadata: a new file `00002-f1c0feb2-....metadata.json`.
- Data: the Parquet file of the table is not changed (`00000-5-89af...parquet`).
- Schemas in `00002-....metadata.json`: schema 0 = the 11 original columns (`1:user_id` ... `11:country`), schema 1 = the same + `12:country_code`, and `current-schema-id: 0`. After the drop, the schema was the same as schema 0, so Iceberg did not create a third schema: it moved the current schema back to schema 0. Schema 1 stays in the history.

Why it is a metadata operation: Iceberg identifies the columns by their field id, not by their name or position, and the Parquet files store the field ids. Dropping a column only writes a new current schema without the field id 12. When reading, the columns of the files that are not in the current schema are ignored, and there was no data for `country_code` anyway. No data file is rewritten and the data did not change, so no snapshot is created: only a new metadata file and a moved pointer.

### E2. A `silver_reader` role limited to the `silver` namespace

`catalog_reader` has its privileges on the whole catalog. A privilege can also be granted on a namespace: then it is valid only for the tables of this namespace. I created the full chain for a new principal, with the management API and the `root` token:

```
principal reader → principal role silver_data_reader → catalog role silver_reader
                                                       └─ grants on namespace silver only
```

First, as `dev`, a `gold` namespace with a table to test:

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
spark.sql("""
    CREATE OR REPLACE TABLE polaris.gold.orders_by_product AS
    SELECT product, count(*) AS nb_orders FROM polaris.silver.orders GROUP BY product
""")
```

Then the RBAC chain:

```bash
API="$POLARIS_URI/api/management/v1"
TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
api() { curl -s -X "$1" "$API$2" -H "Authorization: Bearer $TOKEN" \
          -H "Content-Type: application/json" ${3:+-d "$3"}; echo; }

# catalog role
api POST "/catalogs/$POLARIS_CATALOG/catalog-roles" '{"catalogRole":{"name":"silver_reader"}}'
# privileges on the namespace silver only
for p in TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA NAMESPACE_READ_PROPERTIES; do
  api PUT "/catalogs/$POLARIS_CATALOG/catalog-roles/silver_reader/grants" \
    "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$p\"}}"
done
# read the catalog properties (no table access), as catalog_reader
api PUT "/catalogs/$POLARIS_CATALOG/catalog-roles/silver_reader/grants" \
  '{"grant":{"type":"catalog","privilege":"CATALOG_READ_PROPERTIES"}}'
# principal role, linked to the catalog role
api POST "/principal-roles" '{"principalRole":{"name":"silver_data_reader"}}'
api PUT "/principal-roles/silver_data_reader/catalog-roles/$POLARIS_CATALOG" \
  '{"catalogRole":{"name":"silver_reader"}}'
# principal (credentials returned only once, saved outside Git)
api POST "/principals" '{"principal":{"name":"reader","properties":{}},"credentialRotationRequired":false}'
api PUT "/principals/reader/principal-roles" '{"principalRole":{"name":"silver_data_reader"}}'
```

The grants of `silver_reader`: 4 grants of type `namespace` on `["silver"]` (`TABLE_LIST`, `TABLE_READ_PROPERTIES`, `TABLE_READ_DATA`, `NAMESPACE_READ_PROPERTIES`) + `CATALOG_READ_PROPERTIES` on the catalog. I added this last one as a precaution because Spark reads the catalog configuration when it connects; I did not test whether it is required.

Test with `POLARIS_PRINCIPAL=reader`:

- `SHOW TABLES IN polaris.silver` → `orders`, `users`; `SELECT count(*) FROM polaris.silver.orders` → 2834.
- `SELECT * FROM polaris.gold.orders_by_product` → `ForbiddenException: Principal 'reader' with activated PrincipalRoles '[silver_data_reader]' and activated grants via '[silver_data_reader, silver_reader]' is not authorized for op LOAD_TABLE`
- `SHOW TABLES IN polaris.gold` → `... is not authorized for op LIST_TABLES`

So `reader` has the same rights as `analyst` on `silver` and none on `gold`. As for `analyst`, this is only enforced by the catalog: with the shared S3 keys, the files of `gold` can still be read directly in the bucket.

## Cleanup

As `dev`:

```python
spark.sql("DROP TABLE IF EXISTS polaris.silver.users")
spark.sql("DROP TABLE IF EXISTS polaris.gold.orders_by_product")
spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
```

`SHOW NAMESPACES IN polaris` → `silver`; `SHOW TABLES IN polaris.silver` → `orders`. `silver.orders` stays registered for the Trino lab, and Polaris stays deployed. A `DROP TABLE` without `PURGE` only removes the entry from the catalog: the files stay in the bucket (good here, because the folder `users/` also has the files of `lab.silver.users`).

## Notes on the lab material

- The cleanup drops the namespace `gold` without dropping the table of exercise 2 first: Polaris refuses to drop a namespace that is not empty, so the table must be dropped before.
- `register_table` uses `v1.metadata.json`: it must be the version in `version-hint.text` (`v5` for me), otherwise Polaris points to an old version of the table.
- The lab starts the `pyspark` shell and then calls `SparkSession.builder...getOrCreate()`, which returns the existing session of the shell; it also talks about restarting a notebook kernel. Running the code with `python3` avoids both problems.
- Lab 1 links to `./lab.md`, but the file is `lab-2.md`. Its expected output for the principal roles shows `data_scientist`, but the bootstrap job creates `data_analyst`.
- The session comment says "No credential vending on Ceph RGW", but the S3 endpoint of the platform is SeaweedFS.
- `polaris.silver.users` is created in the same folder as `lab.silver.users` (see Q5), which the lab does not mention.
