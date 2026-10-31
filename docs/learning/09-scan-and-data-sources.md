# 09 · Scan & Data Sources

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [08 · Shuffle](08-shuffle.md) · Next: [10 · Native write](10-native-write.md)

---

## TL;DR

- Native scans read Parquet/ORC directly in C++ with **filters pushed into the reader**, so rows that don't match are never materialized.
- Spark still lists files and plans splits; Gluten passes the file list (paths, offsets, partition values) to Velox for each task.
- Scans are validated for file format, filesystem and types. Anything unsupported falls back to Spark's reader.
- Lake formats (Iceberg, Delta, Hudi, Paimon) plug in as components; see [16](16-delta-and-iceberg-integration.md) for Delta and Iceberg.

**Technical summary**

- Scans are usually the biggest win: Velox reads Parquet/ORC **directly into columnar vectors** with vectorized decoding, filter pushdown, column pruning, async IO and optional caching — no row conversion.
- Spark's `FileSourceScanExec` (DSv1), `BatchScanExec` (DSv2) and Hive table scans are offloaded to `FileSourceScanExecTransformer`, `BatchScanExecTransformer`, `HiveTableScanExecTransformer` (via `ScanTransformerFactory`). Data-lake components (Iceberg, Delta, Hudi, Paimon) add their own scan transformers.
- **Spark still plans the files.** Gluten reuses Spark's partition pruning (incl. DPP), file listing and split planning (`FilePartition`s); it then converts each task's files into a **`SplitInfo` (`LocalFilesNode`)** with paths, offsets, lengths, partition values and metadata columns.
- The scan's Substrait `ReadRel` carries schema + pushed filters. In Velox it becomes a `TableScanNode` on the **Hive connector** (or Delta/Iceberg-aware splits); Velox's Hive connector converts eligible filters into **subfield filters** evaluated inside the reader.
- Scan validation checks: file format support (Parquet ✓, ORC ✓ with type caveats, DWRF ✓, text via CSV paths), **filesystem scheme** registered natively (HDFS/S3/GCS/ABFS compiled in), Parquet metadata quirks, types.
- Optional: Velox **AsyncDataCache** (memory + SSD) with **soft affinity** scheduling for remote storage; file handle cache; IO tuning knobs (`loadQuantum`, coalescing, prefetch).

---

## 1. Why native scans matter

| Vanilla Spark Parquet path | Gluten/Velox path |
|---|---|
| Vectorized reader into `ColumnarBatch` (on-heap/off-heap vectors), then usually `ColumnarToRow` for whole-stage codegen | Velox DWIO reader decodes directly into Velox vectors consumed by native operators |
| Filters as Parquet row-group stats + post-scan Filter | Subfield filters pushed into the decoder (skip rows/pages), lazy vectors, dynamic filters from joins |
| Java IO stack | Native IO with coalesced reads, prefetch, async split preload, optional SSD cache |

And since every following operator is native, the scan's output never leaves native memory.

---

## 2. Key concepts

| Concept | Description | Code |
|---|---|---|
| `BasicScanExecTransformer` | Shared scan logic: output schema, pushed filters, `doTransform` building `ReadRel`, validation, `getSplitInfos` | [BasicScanExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/BasicScanExecTransformer.scala) |
| `FileSourceScanExecTransformer` | DSv1 file scans (Parquet/ORC/...) | [FileSourceScanExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/FileSourceScanExecTransformer.scala) |
| `BatchScanExecTransformer` | DSv2 scans | [BatchScanExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/BatchScanExecTransformer.scala) |
| `HiveTableScanExecTransformer` | Hive tables (Parquet/ORC/text SerDes) | [HiveTableScanExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/hive/HiveTableScanExecTransformer.scala) |
| `ScanTransformerFactory` | Creates the right transformer (also data-lake-specific ones registered by components) | [ScanTransformerFactory.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/ScanTransformerFactory.scala) |
| `PushDownFilterToScan` | Post-transform rule moving filter conjuncts into the scan | [PushDownFilterToScan.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/PushDownFilterToScan.scala) |
| `SplitInfo` / `LocalFilesNode` | Per-task list of files & metadata | [LocalFilesNode.java](../../gluten-substrait/src/main/java/org/apache/gluten/substrait/rel/LocalFilesNode.java) |
| `VeloxIteratorApi.genSplitInfo` | Converts Spark `FilePartition`s into `LocalFilesNode` | [VeloxIteratorApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxIteratorApi.scala#L68-L118) |
| `VeloxBackendSettings.validateScanExec` | Format / scheme / metadata / type validation | [VeloxBackend.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBackend.scala#L109-L262) |
| `SoftAffinity` | Stable executor preference per file for cache locality | `gluten-substrait/.../softaffinity` |
| Native readers | Velox DWIO Parquet/ORC readers, Hive connector, filesystems | `cpp/velox/compute/VeloxBackend.cc`, `cpp/velox/filesystem` |

---

## 3. Architecture

How a file scan is planned and executed.

**Driver:**

1. Spark's `FileSourceScanExec` does partition pruning, dynamic partition pruning (DPP) and file listing, and builds `FilePartition`s.
2. `OffloadOthers` uses `ScanTransformerFactory` to create a `FileSourceScanExecTransformer`.
3. The transformer:
   - validates itself (`validateScanExec`: format, filesystem scheme, metadata, types, plus native validation);
   - takes filter conjuncts pushed down by `PushDownFilterToScan`;
   - emits a Substrait `ReadRel(schema, filter)` in `doTransform`;
   - produces split infos through `getSplitInfos` → `VeloxIteratorApi.genSplitInfo`, one per `FilePartition`.

**Executor:**

1. The `ReadRel` and its `SplitInfo` become a Velox `TableScanNode` on the Hive connector.
2. The DWIO reader reads Parquet or ORC with subfield filters and lazy vectors.
3. Bytes come from the filesystem layer: local, HDFS, S3, GCS or ABFS, optionally through `AsyncDataCache`.

---

## 4. Detailed walkthrough

### 4.1 Offload and validation

`OffloadOthers` maps `FileSourceScanExec`/`BatchScanExec`/Hive scans via `ScanTransformerFactory`. Validation (`doValidateInternal` in `BasicScanExecTransformer`) checks:

1. **Backend settings** — `VeloxBackendSettings.validateScanExec`:
   - **Scheme**: every distinct non-`file` root path scheme must be supported by a registered native filesystem (`VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems`). E.g. `s3a://` requires a build with `--enable_s3=ON`.
   - **Format**: Parquet (fails if `mergeSchema=true`), DWRF, ORC (if `spark.gluten.sql.columnar.backend.velox.orc.scan.enabled`, default `true`; some nested types and `TimestampType` in ORC fall back; char types can be forced to fall back), anything else → "Unsupported file format".
   - **Parquet metadata**: samples files (`parquetMetadataFallbackFileLimit`) for unsupported encodings/metadata.
   - **Data schema types** when not using column names for schema evolution.
2. **Native validation** of the generated `ReadRel` (types, filter expressions).

### 4.2 Filter pushdown

- Spark already pushes data filters into `FileSourceScanExec` (`PushedFilters` in EXPLAIN) — Gluten uses them.
- After offload, `PushDownFilterToScan` takes a `FilterExecTransformer` directly above a scan and calls `scan.withNewPushdownFilters(conjuncts)`; if the new scan validates, the filter's conditions are evaluated in the scan. The filter node remains but becomes a **no-op** if nothing remains (`FilterExecTransformerBase.isNoop`).
- In Substrait, pushed conditions become `ReadRel.filter`.
  - Native code creates `HiveTableHandle(..., subfieldFilters = {}, remainingFilter = filter, dataColumns)`.
  - Velox's Hive connector then extracts conjuncts it can evaluate per column as **subfield filters** (using Gluten's registered `SparkExprToSubfieldFilterParser` for Spark semantics) and keeps the rest as a remaining filter — both evaluated inside the reader, before materializing other columns.

### 4.3 Splits

Spark has already planned `FilePartition`s (respecting `spark.sql.files.maxPartitionBytes`, `openCostInBytes`). For each partition, `genSplitInfo` builds:

```scala
LocalFilesBuilder.makeLocalFiles(
  partitionIndex,
  paths, starts, lengths, fileSizes, modificationTimes,
  partitionColumns,          // Map[colName -> value] per file (from Hive-style partition dirs)
  metadataColumns,           // _metadata.file_path etc.
  fileFormat,                // ParquetReadFormat / OrcReadFormat / ...
  preferredLocations,        // soft affinity / HDFS locality
  properties,                // reader options
  otherMetadataColumns)
```

Then `setFileSchemaForLocalFiles` attaches the table/data schema. In native code, `WholeStageResultIterator` converts each file entry into a Velox `HiveConnectorSplit` (or Iceberg/Delta split with delete files / deletion vectors) with partition keys and metadata columns, and adds them to the scan node.

### 4.4 Native reading

- **Readers**: Velox DWIO Parquet and ORC readers (registered in `VeloxBackend::init`), selected by split format.
- **Schema evolution**: `parquetUseColumnNames` / `orcUseColumnNames` (default `true`) match columns by name; when false, by index (then the full table schema must be type-supported).
- **IO**: async split preload (`SplitPreloadPerDriver`, default 2), coalesced reads (`maxCoalescedDistance` 512KB, `maxCoalescedBytes` 64MB), `loadQuantum` (256MB), `prefetchRowGroups` (1), small files fully loaded below `filePreloadThreshold` (1MB), footer read size `footerEstimatedSize` (32KB).
- **Caching**: optional `AsyncDataCache` (memory `memCacheSize`, SSD `ssdCachePath/ssdCacheSize`) + `fileHandleCacheEnabled` — see [docs/get-started/VeloxLocalCache.md](../../docs/get-started/VeloxLocalCache.md). The cache requires the file handle cache, and `loadQuantum ≤ 8MB` (checked at driver start).
- **Dynamic filters**: Velox hash joins can push build-side value ranges/bloom filters into the probe-side scan (`hashProbe.dynamicFilterPushdown.enabled`, default `true`).

### 4.5 Filesystems

Registered at backend init depending on build flags (`dev/buildbundle-veloxbe.sh --enable_hdfs/--enable_s3/--enable_gcs/--enable_abfs`): local (always), HDFS, S3 (Gluten S3 FS wrapper), GCS, ABFS. Credentials/config are mapped from Spark/Hadoop configs (e.g. `spark.hadoop.fs.s3a.*`) in `GlutenConfig.getNativeBackendConf`. Docs: [VeloxS3.md](../../docs/get-started/VeloxS3.md), [VeloxGCS.md](../../docs/get-started/VeloxGCS.md), [VeloxABFS.md](../../docs/get-started/VeloxABFS.md).

### 4.6 Soft affinity

With `spark.gluten.soft-affinity.enabled=true`, `SoftAffinity` assigns each file a stable set of preferred executors (hash of path, `replications.num` default 2), so repeated scans of the same file land on executors that already cached it. Optional duplicate-reading detection tracks files read by multiple executors. Use together with the Velox SSD cache for remote storage.

### 4.7 Data lake formats

> Deep dive for Delta and Iceberg (deletion vectors, delete files, column mapping, write paths): [16 · Delta & Iceberg](16-delta-and-iceberg-integration.md).

Each is a **component** (built with its Maven profile) that injects offload rules for its scans:

| Format | Component / module | Read support (v1.7.0 docs) | Notes |
|---|---|---|---|
| Iceberg | `VeloxIcebergComponent`, `gluten-iceberg` | copy-on-write (COW) & merge-on-read (MOR); position deletes offloaded; **equality deletes fall back** (`supportIcebergEqualityDeleteRead=false`) | Writes fall back by default; partitioned tables may partially fall back due to unsupported `StaticInvoke` functions ([VeloxIceberg.md](../../docs/get-started/VeloxIceberg.md)) |
| Delta | `VeloxDeltaComponent` (+ `VeloxDelta33/40WriteComponent`), `gluten-delta` | Read incl. column mapping; deletion vectors partial | Native write experimental (`...velox.delta.enableNativeWrite=false` by default) ([VeloxDelta.md](../../docs/get-started/VeloxDelta.md)) |
| Hudi | `VeloxHudiComponent`, `gluten-hudi` | **Copy-on-write (COW) only** | |
| Paimon | `VeloxPaimonComponent`, `gluten-paimon` | **Non-PK tables**, Spark ≥ 3.3 | |
| Kafka | `gluten-kafka` (`MicroBatchScanExecTransformer`) | Streaming micro-batch source | |

---

## 5. End-to-end worked example

Table `sales` (Parquet), partitioned by `dt`:

```sql
SELECT store, sum(amount)
FROM sales
WHERE dt = '2026-10-01' AND amount > 100
GROUP BY store;
```

1. **Spark**: partition pruning keeps `dt=2026-10-01` dirs; `PushedFilters: [IsNotNull(amount), GreaterThan(amount,100)]`; files split into `FilePartition`s.
2. **Gluten planning**: `FileSourceScanExecTransformer parquet` validated (scheme `hdfs://` registered, Parquet OK). `FilterExecTransformer` pushed into the scan by `PushDownFilterToScan`.
3. **Substrait** `ReadRel`: `base_schema = [store, amount, dt(partition)]`, `filter = and(isnotnull(amount), gt(amount, 100))`.
4. **SplitInfo** for task 3 (illustrative):

```text
LocalFiles {
  items: [
    { uri_file: "hdfs://nn/warehouse/sales/dt=2026-10-01/part-00007.parquet",
      start: 0, length: 134217728, file_size: 268435456,
      partition_columns: [ {key: "dt", value: "2026-10-01"} ], parquet: {} },
    ...
  ]
}
```

5. **Velox**: `TableScan[hive_table, remaining filter: and(isnotnull(amount), greaterthan(amount, 100))]` → reader extracts `amount > 100` as a subfield filter, decodes `store` only for passing rows; `dt` is filled as a constant partition column.
6. **Metrics** on the scan node in the UI: `time of scan`, `number of output rows`, `io wait time`, `storage read bytes`, `number of skipped splits`, `number of skipped row groups`, `number of preloaded splits`, `remaining filter time`, `data source add split time`, ... (names from `VeloxMetricsApi`)

EXPLAIN (illustrative):

```text
(1) FileSourceScanExecTransformer parquet
Output [2]: [store#1, amount#2]
Batched: true
Location: InMemoryFileIndex [hdfs://nn/warehouse/sales]
PartitionFilters: [isnotnull(dt#3), (dt#3 = 2026-10-01)]
PushedFilters: [IsNotNull(amount), GreaterThan(amount,100)]
ReadSchema: struct<store:string,amount:decimal(10,2)>
```

**Try it / verify**

- `df.explain("formatted")`: the `FileSourceScanExecTransformer` entry should list `PushedFilters`/`DataFilters` containing `amount > 100`.
- SQL UI on the scan: `number of skipped row groups` > 0 and low `number of output rows` mean pushdown and pruning work. A high `io wait time` with low `time of scan` means the scan is IO-bound.
- To compare with vanilla Spark, set `spark.gluten.sql.columnar.filescan=false`: the scan becomes a plain `FileScan parquet`, and a transition into Velox batches is added above it.

---

## 6. Configuration reference

| Key | Default | Effect |
|---|---|---|
| `spark.gluten.sql.columnar.filescan` / `.batchscan` / `.hivetablescan` | `true` | Enable scan offload per type |
| `spark.gluten.sql.columnar.backend.velox.orc.scan.enabled` | `true` | Native ORC scan |
| `spark.gluten.sql.columnar.backend.velox.parquetUseColumnNames` / `orcUseColumnNames` | `true` / `true` | Match columns by name |
| `spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver` | `2` | Splits preloaded asynchronously |
| `spark.gluten.sql.columnar.backend.velox.IOThreads` | task slots | IO threads |
| `spark.gluten.sql.columnar.backend.velox.loadQuantum` | `256MB` | IO unit (≤ 8MB with cache) |
| `spark.gluten.sql.columnar.backend.velox.maxCoalescedDistance` / `maxCoalescedBytes` | `512KB` / `64MB` | Read coalescing |
| `spark.gluten.sql.columnar.backend.velox.prefetchRowGroups` | `1` | Row groups to prefetch |
| `spark.gluten.sql.columnar.backend.velox.filePreloadThreshold` | `1MB` | Load whole small files |
| `spark.gluten.sql.columnar.backend.velox.footerEstimatedSize` | `32KB` | Footer read size (`directorySizeGuess` deprecated) |
| `spark.gluten.sql.columnar.backend.velox.cacheEnabled` | `false` | AsyncDataCache |
| `spark.gluten.sql.columnar.backend.velox.memCacheSize` / `ssdCachePath` / `ssdCacheSize` | see VeloxLocalCache.md | Cache sizing |
| `spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled` | `false` | Required with cache |
| `spark.gluten.sql.columnar.backend.velox.hashProbe.dynamicFilterPushdown.enabled` | `true` | Join → scan dynamic filters |
| `spark.gluten.soft-affinity.enabled` | `false` | Soft affinity scheduling |
| `spark.gluten.soft-affinity.replications.num` / `min.target-hosts` | `2` / `1` | Affinity parameters |
| `spark.sql.files.maxPartitionBytes` (Spark) | `128MB` | Split size (still Spark's) |

---

## 7. Pitfalls & FAQ

| Symptom | Cause | Fix |
|---|---|---|
| `Scheme of [s3a://...] is not supported by registered file systems` | Native build without S3 | Rebuild with `--enable_s3=ON` (or use a bundle that includes it) |
| `not support when merge schema is true` | Parquet `mergeSchema` | Disable mergeSchema or accept fallback |
| ORC scan falls back | Timestamp / nested types in ORC, or char type forced fallback | Check reason; use Parquet |
| Wrong/NULL columns after schema evolution | Name vs index matching | Check `parquetUseColumnNames`/`orcUseColumnNames` |
| Slow remote reads | No cache / poor coalescing | Enable cache + soft affinity; tune `loadQuantum`/coalescing |
| Iceberg MOR with equality deletes falls back | Not supported | Expected in v1.7.0 |

---

## 8. Code map

| File | Role |
|---|---|
| [BasicScanExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/BasicScanExecTransformer.scala) | Scan base |
| [FileSourceScanExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/FileSourceScanExecTransformer.scala) | DSv1 |
| [BatchScanExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/BatchScanExecTransformer.scala) | DSv2 |
| [ScanTransformerFactory.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/ScanTransformerFactory.scala) | Factory |
| [PushDownFilterToScan.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/PushDownFilterToScan.scala) | Pushdown |
| [VeloxIteratorApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxIteratorApi.scala) | Split infos |
| [VeloxBackend.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBackend.scala) | Scan validation |
| [SubstraitToVeloxPlan.cc](../../cpp/velox/substrait/SubstraitToVeloxPlan.cc#L1499-L1620) | ReadRel → TableScan |
| [WholeStageResultIterator.cc](../../cpp/velox/compute/WholeStageResultIterator.cc#L163-L280) | Splits → connector splits |
| [VeloxBackend.cc](../../cpp/velox/compute/VeloxBackend.cc) | Readers, filesystems, cache |
| `backends-velox/src-iceberg`, `src-delta*`, `src-hudi`, `src-paimon` | Lake components |

## 9. Related docs

- [03 · Substrait](03-substrait-and-native-plan-conversion.md) (ReadRel & splits) · [10 · Native write](10-native-write.md) · [13 · Metrics](13-metrics-ui-config-troubleshooting.md)
- Repo: [VeloxLocalCache.md](../../docs/get-started/VeloxLocalCache.md), [VeloxIceberg.md](../../docs/get-started/VeloxIceberg.md), [VeloxDelta.md](../../docs/get-started/VeloxDelta.md), [VeloxS3.md](../../docs/get-started/VeloxS3.md)
