# 16 · Delta Lake & Iceberg Integration

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [15 · Other backends & frontends](15-other-backends-and-frontends.md) · Back to [00 · Overview](00-overview-and-query-lifecycle.md)

---

## TL;DR

- Delta and Iceberg tables are Parquet/ORC files plus metadata. The **Delta/Iceberg libraries still handle the metadata**; Gluten offloads only the file reading, compute and (optionally) writing.
- **Deletes are applied natively**: Delta deletion vectors and Iceberg position deletes. Iceberg equality deletes and v3 delete files fall back.
- Native **Delta write** is experimental and off by default. Native **Iceberg write** only exists in builds made with `--enable_enhanced_features=ON`.

**Technical summary**

- Delta and Iceberg support are **optional components**. Each one is built with a Maven profile (`-Pdelta`, `-Piceberg`) and registered through `META-INF/gluten-components`. A component turns itself on only when its Spark extension class is on the classpath (`isRuntimeCompatible`).
- Both formats come down to **"Parquet/ORC files plus table-format metadata"**.
  - Gluten lets the **JVM library** (Delta / Iceberg) do the metadata work: snapshot resolution, file pruning, planning delete files or deletion vectors, commits.
  - Gluten offloads only the **data-file I/O and compute** to Velox.
- **Delta read:** `OffloadDeltaScan` turns a Delta `FileSourceScanExec` into a `DeltaScanTransformer`.
  - Deletion vectors (DVs) are **loaded on the driver** while splits are built, serialized as Roaring bitmaps into `DeltaLocalFilesNode`, and applied natively by a dedicated **Velox Delta connector**.
  - `DeltaPostTransformRules` remove the synthetic `__delta_internal_is_row_deleted` column and filter that Delta injects, and rewrite column-mapping tables to physical column names.
- **Delta write:** off by default. `spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=true` (experimental) wraps Delta commands so that the transaction becomes a `GlutenOptimisticTransaction`. That transaction writes columnar batches natively and computes Delta file statistics with a Velox aggregation.
- **Iceberg read:** `OffloadIcebergScan` turns a `BatchScanExec(SparkBatchQueryScan)` into an `IcebergScanTransformer`.
  - Splits become `IcebergLocalFilesNode`s that carry each data file's **delete files**.
  - Velox's `HiveIcebergSplit` applies **position deletes** natively.
  - **Equality deletes**, v3 delete files (Puffin DVs), UUID/FIXED types, renamed columns and similar cases **fall back** to vanilla Spark.
- **Iceberg write:** the five DSv2 write execs (`AppendData`, `ReplaceData`, `OverwriteByExpression`, `OverwritePartitionsDynamic`, `WriteToDataSourceV2`) can be offloaded to `VeloxIceberg*Exec` → JNI `IcebergWriter` → Velox `IcebergDataSink`.
  - This path exists only when the native library is built with **`--enable_enhanced_features=ON`** (CMake default OFF).
  - It also needs `spark.gluten.sql.enable.enhancedFeatures=true`, which is the default.
  - Without enhanced features, Iceberg writes fall back.
  - This is why the upstream docs say "writes fall back".

---

## 1. Why this needs special handling

A Delta or Iceberg table is a directory of ordinary Parquet (or ORC) files plus a metadata layer:

| Concern | Delta | Iceberg |
|---|---|---|
| Table metadata / log | `_delta_log/*.json` + checkpoints | metadata.json → manifest lists → manifests |
| Which files make up a snapshot | `AddFile` / `RemoveFile` actions | manifest entries |
| Row-level deletes without rewriting | **Deletion vectors** (Roaring bitmap of row indexes per file) | **Position deletes** (file + pos), **equality deletes** (key values), v3 **DVs in Puffin** |
| Column renames / drops | **Column mapping** (physical names/ids differ from logical names) | Field ids (every column has an id) |
| Writes | `OptimisticTransaction.writeFiles` + stats + commit | `SparkWrite` → `DataFile`s + commit |

Vanilla Spark reads these tables through format-specific code: `DeltaParquetFileFormat`, or Iceberg's `SparkBatchQueryScan` with its own vectorized reader. A plain `FileSourceScanExecTransformer` would get several things wrong:
- it would **ignore deletes**, so deleted rows would come back;
- it would **use the wrong column names** on column-mapped tables, reading nulls;
- it would **miss data files** if it tried to resolve the snapshot itself.

So each format needs:
1. a **scan transformer** that keeps the library's file listing and pruning but encodes extra per-file information (deletes) into the Substrait split;
2. **native support** in Velox to apply that information;
3. **validation** that falls back whenever Gluten cannot guarantee correct results;
4. optionally, a **native write path** that still lets the library produce the metadata and commit.

---

## 2. Key concepts

| Concept | Meaning | Where |
|---|---|---|
| Component | Pluggable module that injects rules; loaded via ServiceLoader-style files | [VeloxDeltaComponent.scala](../../backends-velox/src-delta/main/scala/org/apache/gluten/component/VeloxDeltaComponent.scala), [VeloxIcebergComponent.scala](../../backends-velox/src-iceberg/main/scala/org/apache/gluten/component/VeloxIcebergComponent.scala) |
| `DeltaScanTransformer` | `FileSourceScanExecTransformerBase` for Delta; always Parquet; adds DV split info | [DeltaScanTransformer.scala](../../gluten-delta/src/main/scala/org/apache/gluten/execution/DeltaScanTransformer.scala) |
| `DeltaDeletionVectorScanInfo` | Driver-side extraction and loading of DV bitmaps per file | [DeltaDeletionVectorScanInfo.scala](../../gluten-delta/src-delta33/main/scala/org/apache/gluten/delta/DeltaDeletionVectorScanInfo.scala) |
| `RowIndexFilterType` | `KEEP_ALL` / `IF_CONTAINED` (drop rows in DV) / `IF_NOT_CONTAINED` (keep only rows in DV) | same file + native `DeltaRowIndexFilterType` |
| Delta connector | Velox connector (`connectorIds_.delta`) with `DeltaDataSource`/`DeltaSplitReader` that apply DVs | [cpp/velox/compute/delta/](../../cpp/velox/compute/delta/DeltaConnector.h) |
| Column mapping | Delta files use physical names (`col-<uuid>`); metadata uses logical names | [DeltaPostTransformRules.scala](../../gluten-delta/src/main/scala/org/apache/gluten/extension/DeltaPostTransformRules.scala) |
| `IcebergScanTransformer` | `BatchScanExecTransformerBase` for Iceberg `SparkBatchQueryScan` | [IcebergScanTransformer.scala](../../gluten-iceberg/src/main/scala/org/apache/gluten/execution/IcebergScanTransformer.scala) |
| `IcebergLocalFilesNode` | `LocalFilesNode` + `List<List<DeleteFile>>` (delete files per data file) | [IcebergLocalFilesNode.java](../../gluten-iceberg/src/main/java/org/apache/gluten/substrait/rel/IcebergLocalFilesNode.java) |
| `HiveIcebergSplit` | Velox's Iceberg split type; reads position/equality delete files | Velox `connectors/hive/iceberg` |
| Enhanced features | Build flag that enables Gluten-fork-only Velox features, including the Iceberg writer | [CMakeLists.txt](../../cpp/velox/CMakeLists.txt), [VeloxConfig.scala](../../backends-velox/src/main/scala/org/apache/gluten/config/VeloxConfig.scala) |

---

## 3. Architecture

How Delta and Iceberg scans flow from planning to native reading.

**Driver (JVM):**

1. Spark plans the query with the Delta/Iceberg extensions. The result is a `FileSourceScanExec` (Delta) or a `BatchScanExec` (Iceberg).
2. The component offload rules replace them:
   - Delta → `DeltaScanTransformer`, followed by `DeltaPostTransformRules` (strip the DV filter, handle column mapping);
   - Iceberg → `IcebergScanTransformer`.
3. Splits are built:
   - Delta: `DeltaLocalFilesNode` with the serialized deletion-vector bitmap;
   - Iceberg: `IcebergLocalFilesNode` with the delete files.

**Executor (Velox):**

1. Delta: `parseDeltaSplitInfo` → `DeltaSplitInfo` → `HiveDeltaSplit` on the Delta connector. `DeltaSplitReader` reads Parquet and applies the DV mask.
2. Iceberg: `parseIcebergSplitInfo` → `IcebergSplitInfo` → `HiveIcebergSplit` on the Hive connector. `IcebergSplitReader` reads Parquet/ORC and applies position deletes.

Component wiring (both are legacy-transform rules running in the heuristic planner, see [02 · Planning](02-planning-offload-validation-fallback.md)):

| Component | Name | Depends on | Active when class present | Rules injected |
|---|---|---|---|---|
| `VeloxDeltaComponent` | `velox-delta` | `VeloxBackend` | `io.delta.sql.DeltaSparkSessionExtension` | transform: `OffloadDeltaScan`, `OffloadDeltaProject`, `OffloadDeltaFilter`; post-transform: `DeltaPostTransformRules.rules` |
| `VeloxDelta33WriteComponent` / `VeloxDelta40WriteComponent` | `velox-delta33-write` / `velox-delta40-write` | `VeloxDeltaComponent` | (inherits) | transform: `OffloadDeltaCommand` |
| `VeloxIcebergComponent` | `velox-iceberg` | `VeloxBackend` | `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` | transform: `OffloadIcebergScan`; `OffloadIcebergWrite.inject` (5 write rules) |

Each transform rule is wrapped as `HeuristicTransform.Simple(Validators.newValidator(conf, offload), offload)`. The usual validators therefore still run: user options, backend settings, and the plan's own `doValidate`.

---

## 4. Delta Lake read path

### 4.1 Offload decision — `OffloadDeltaScan`

[OffloadDeltaScan.scala](../../gluten-delta/src/main/scala/org/apache/gluten/extension/OffloadDeltaScan.scala) matches `FileSourceScanExec` and checks these cases in order:

| Case | Result | Why |
|---|---|---|
| Root path contains `_delta_log` | **Fallback** (tag "fallback Delta _delta_log scan") | Log/checkpoint reads are small metadata scans done by Delta itself |
| Spark < 3.5 **and** table can contain DVs | **Fallback** ("Spark 3.4 Delta DV scan") | Native DV path requires the Spark 3.5+ metadata-row-index contract |
| Spark ≥ 3.5, DV present, `spark.databricks.delta.deletionVectors.useMetadataRowIndex=false` | **Fallback** | Delta DML tests force this mode and rely on Spark's injected row-index filter column |
| Location is `TahoeFileIndex` / `PreparedDeltaFileIndex`, or file format is `DeltaParquetFileFormat` / `GlutenDeltaParquetFileFormat` | **`DeltaScanTransformer(scan)`** | Normal Delta scan |
| otherwise | unchanged | handled by the generic scan rule |

"Contains a DV" is answered as follows:
- for a `PreparedDeltaFileIndex`: whether any prepared file has `deletionVector != null`;
- for a plain `TahoeFileIndex`: whether the snapshot's protocol/metadata make DVs readable.

The other two rules handle Delta's `IncrementMetric` expression. Delta DML (UPDATE/DELETE/MERGE) wraps predicates and projections in `increment_metric(...)` to count rows:
- `OffloadDeltaFilter` replaces a `FilterExec` whose condition contains it with a `DeltaFilterExecTransformer`;
- `OffloadDeltaProject` does the same for a `ProjectExec`, producing a `DeltaProjectExecTransformer`.

These transformers drop the wrapper from the native expression and keep the metric updated, so DML plans don't fall back just because of a metrics wrapper.

### 4.2 `DeltaScanTransformer`

`DeltaScanTransformer` extends `FileSourceScanExecTransformerBase`, the same base as a normal Parquet scan (see [09 · Scan](09-scan-and-data-sources.md)). It changes three things:

1. `fileFormat = ParquetReadFormat`. Delta data files are always Parquet.
2. **`scanFilters` override (column mapping).**
   - On column-mapped tables, `dataFilters` stay in *logical* names, so Delta's file index can still prune partitions and skip files by stats.
   - The native side needs *physical* names, so the override rewrites attributes by `exprId` against the (physical) `output`.
   - Without it, `scanFilters` and `pushDownFilters` would never match by name, and the filter would be evaluated twice.
3. **`getSplitInfosFromPartitions` override (deletion vectors).** It builds the generic `LocalFilesNode` first. Then, for each `FilePartition`, `DeltaDeletionVectorScanInfo.normalize(...)` decides what to emit:
   - If **no file** in the split has a DV, it keeps the generic node, so there is zero overhead.
   - Otherwise it builds a `DeltaLocalFilesNode` with two per-file lists: `otherMetadataColumns`, with the DV bookkeeping keys stripped, and `DeltaFileReadOptions` (`rowIndexFilterType`, `hasDeletionVector`, `cardinality`, `serializedDeletionVector`).

### 4.3 How a deletion vector reaches Velox

How a deletion vector travels from Delta's metadata to Velox.

1. During planning, Delta's `PreprocessTableWithDVsStrategy` stores two values in each file's metadata: `FILE_ROW_INDEX_FILTER_ID_ENCODED` (a base64 DV descriptor) and `FILE_ROW_INDEX_FILTER_TYPE`.
2. On the driver, `DeltaDeletionVectorScanInfo` reads those values (`otherConstantMetadataColumnValues`).
3. It decodes the descriptor (`DeletionVectorDescriptor.deserializeFromBase64`).
4. It finds the table root (`resolveTablePath`), walking up the partition directories until `_delta_log` exists.
5. It loads the bitmap from storage (`StoredBitmap.create(desc, tablePath).load(HadoopFileSystemDVStore)`).
6. It serializes it as a portable Roaring bitmap (`RoaringBitmapArrayFormat.Portable`).
7. The bytes go into the Substrait `ReadRel` (`FileOrFiles.delta`: filter type, cardinality, bytes).
8. On the executor, `parseDeltaSplitInfo` builds a `DeltaDeletionVectorDescriptor`.
9. `DeltaSplitReader` reads Parquet rows and drops or keeps each row by its file-global row index.

Things to notice:
- **DV bitmaps are loaded on the driver** while splits are generated, and they travel **inline in the Substrait plan** for each task.
  - Inline DVs and on-disk DVs both end up as serialized bytes.
  - So a table with many large DVs means extra driver I/O and bigger task plans. Keep this in mind when you debug slow planning.
- Both metadata keys must be present together, or both absent. Otherwise you get `IllegalStateException("Both ... must either be present or absent")`.
- Native checks:
  - the payload must not be empty and must be ≤ `INT32_MAX` bytes;
  - `DeltaSplitReader` checks that the file's `numRecords` is consistent with the DV cardinality;
  - DVs use **file-global** row positions, not positions relative to the split, so a split that starts in the middle of a file still applies the right bits.
- On the native side, `SubstraitToVeloxPlan` routes the scan to `connectorIds_.delta` when the split info is a `DeltaSplitInfo`. `WholeStageResultIterator` then builds `gluten::delta::HiveDeltaSplit` with the descriptor and filter type ([WholeStageResultIterator.cc](../../cpp/velox/compute/WholeStageResultIterator.cc)).

### 4.4 `DeltaPostTransformRules`

[DeltaPostTransformRules.scala](../../gluten-delta/src/main/scala/org/apache/gluten/extension/DeltaPostTransformRules.scala) runs `RemoveTransitions → nativeDeletionVectorRule → pushDownInputFileExprRule → columnMappingRule`.

**`nativeDeletionVectorRule`.** For the JVM reader, Delta's `PreprocessTableWithDVsStrategy` adds the column `__delta_internal_is_row_deleted` and the filter `= 0`. Velox already applies the DV as a mask, so keeping them would apply the DV twice with incompatible semantics. The rule strips them:

```text
Before (as planned by Delta, scan offloaded):
  ProjectExecTransformer [key, value]
  +- FilterExecTransformer (__delta_internal_is_row_deleted = 0)
     +- DeltaScanTransformer [key, value, __delta_internal_is_row_deleted]

After:
  DeltaScanTransformer [key, value]
```

`__delta_internal_row_index` is kept when an upstream operator still references it (tagged via `PRESERVE_DELETION_VECTOR_ROW_INDEX_TAG`). Scans that were not offloaded are left untouched.

**`pushDownInputFileExprRule`.** Pushes `input_file_name()`-style expressions into the native scan. Delta DML uses these to find touched files.

**`columnMappingRule` (`transformColumnMappingPlan`).** Runs once per plan (tagged). For `nameMapping`/`idMapping` tables:

| Field on the scan | Name space | Consumer |
|---|---|---|
| `output`, `relation.dataSchema`, data fields of `requiredSchema` | **physical** | Substrait `NamedStruct`; Velox finds Parquet columns by these names |
| `partitionSchema`, `partitionFilters`, `dataFilters`, partition fields of `requiredSchema` | **logical** | Delta file index pruning and stats skipping (issue #10511 came from rewriting these) |
| `DeltaScanTransformer.scanFilters` | **physical** (from `dataFilters` by exprId) | native filter pushdown |

Above the scan, a `ProjectExecTransformer` aliases physical names back to logical ones. For structs, the nested field names are rebuilt with `GetStructField` + `CreateNamedStruct` rather than `Cast`, so correctness doesn't depend on Velox's struct-cast-by-name config.

### 4.5 Native function support

`registerDeltaBitmapAggregator()` registers a native `bitmapaggregator(bigint) → row(bigint, bigint, varbinary)` aggregate that builds a Roaring bitmap of row indexes. Delta uses this aggregate to produce DVs during DML. In v1.7.0 only the ClickHouse backend maps Delta's `BitmapAggregator` expression to it on the JVM side. I didn't find a Velox-side Scala mapping, so treat this function as groundwork.

---

## 5. Delta Lake write path (experimental)

By default **nothing** about the Delta write is offloaded beyond the query that feeds it. Delta's `TransactionalWrite` runs the vanilla `FileFormatWriter`, with a columnar-to-row transition placed under it.

With `spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=true`, `OffloadDeltaCommand` ([OffloadDeltaCommand.scala](../../backends-velox/src-delta33/main/scala/org/apache/spark/sql/execution/datasources/v2/OffloadDeltaCommand.scala)) wraps the following commands:

| Command | Wrapped as |
|---|---|
| `UpdateCommand`, `DeleteCommand` | `GlutenDeltaLeafRunnableCommand` |
| `SaveIntoDataSourceCommand(_, DeltaDataSource, ...)` (DataFrame `.write.format("delta")`) | `GlutenDeltaLeafRunnableCommand` |
| `OptimizeTableCommand` (**plain bin-packing only**: no ZORDER, REORG, FULL, or liquid clustering) | `GlutenDeltaRunnableCommand` |
| `AtomicCreateTableAsSelectExec` / `AtomicReplaceTableAsSelectExec` on `DeltaCatalog` | `GlutenDeltaLeafV2CommandExec` |

The wrapper runs the original command inside `TransactionExecutionObserver.withObserver(UseColumnarDeltaTransactionLog)`. That observer **swaps the `OptimisticTransaction` for a `GlutenOptimisticTransaction`** ([DeltaWriteOperators.scala](../../backends-velox/src-delta33/main/scala/org/apache/spark/sql/execution/datasources/v2/DeltaWriteOperators.scala)). Delta's own command logic (conflict detection, commit, log) is unchanged. Only `writeFiles` is replaced:

What `GlutenOptimisticTransaction.writeFiles` does.

1. Normalize the data and collect constraints and the stats schema.
2. If there are constraints, wrap the plan in `DeltaInvariantCheckerExec`. If there are none, skip it; this avoids extra transitions.
3. Turn the plan into a Velox plan: `Transitions.toBatchPlan(VeloxBatchType)`, or set `supportsColumnar` on the AQE plan.
4. If optimized writes are on and this isn't `OPTIMIZE`, add `GlutenDeltaOptimizedWriterExec` (columnar shuffle). If that can't be created, fall back to `DeltaOptimizedWriterExec`.
5. Write with `GlutenDeltaFileFormatWriter.write`, using `GlutenDeltaParquetFileFormat`.
6. Wrap the stats trackers: `GlutenDeltaJobStatsTracker`, the row-counting tracker, or the fallback tracker.
7. Return the `AddFile` actions, which Delta commits.

Details:
- Writer options are reduced to `maxRecordsPerFile`, `compression` and `writePartitionColumns`. The last one is true when IcebergCompat (UniForm) is enabled, because Iceberg requires partition columns inside the data files.
- **Statistics.** Delta needs per-file min/max/nullCount/numRecords. `GlutenDeltaJobStatsTracker.apply` picks one of three trackers:
  - `BasicWriteJobStatsTracker` → `GlutenDeltaJobStatsRowCountingTracker`, which only counts rows;
  - `DeltaJobStatisticsTracker` → `GlutenDeltaJobStatsTracker`, which runs the stats expressions as a **local Velox aggregation** on a single background thread per task;
  - anything else → `GlutenDeltaJobStatsFallbackTracker`, which logs a warning ("frequent columnar-to-row conversions").
- Batches reach the file writer as `BatchCarrierRow`s, so Spark's row-based `FileFormatWriter` API can carry columnar batches. See [05 · Transitions](05-columnar-batches-conventions-transitions.md) and [10 · Native write](10-native-write.md).
- Why it is still "experimental": the config doc says *"the feature now has performance issue because of the fallback processing of statistics."*

---

## 6. Iceberg read path

### 6.1 Offload — `OffloadIcebergScan`

It matches `BatchScanExec` whose `scan.getClass == SparkBatchQueryScan` (`IcebergScanTransformer.supportsBatchScan`) and creates `IcebergScanTransformer(batchScan)`. Output attribute names go through `AvroSchemaUtil.makeCompatibleName`, which matches how Iceberg stores names in Parquet. Runtime filters, key-grouped partitioning (storage-partitioned join) and common partition values are carried over through Spark shims.

### 6.2 Validation — `IcebergScanTransformer.doValidateInternal`

Any failure → **fallback to Spark's Iceberg reader**. For Velox, `supportIcebergEqualityDeleteRead()` is `false`, so the first group always applies:

| Check | Failure message |
|---|---|
| Table schema has UUID/FIXED types or metadata columns | `Contains not supported data type or metadata column` |
| Reading a metadata column other than `input_file_name` / `input_file_block_start` / `input_file_block_length` (e.g. `_pos`, `_spec_id`, `_partition`) | `Read unsupported metadata column` |
| Current snapshot summary `total-equality-deletes > 0` | `Contains equality delete files` |
| A column was renamed, or Iceberg/Spark types don't match | `The column is renamed or data type mismatch, cannot read it.` |
| Table format version ≥ 3 **and any** scan task has delete files | `Delete file format puffin is not supported` |
| Table property `write.spark.accept-any-schema=true` | `Not support read the file with accept any schema` (issue #11135) |

Notes:
- The equality-delete check uses the **snapshot-wide** counter. One equality delete anywhere in the table makes every scan of it fall back, even scans that don't touch the affected partitions.
- Partition columns are taken from the task's partition spec (`getReadPartitionSchema`):
  - void transforms are dropped;
  - hidden transform fields that aren't table columns (e.g. `x_bucket`) are kept as partition fields;
  - only BOOLEAN/INT/LONG/FLOAT/DOUBLE/DATE/TIME/TIMESTAMP/STRING/BINARY/DECIMAL values are accepted. Any other type throws `UnsupportedOperationException`, which validation turns into a fallback.
- Upstream docs list partitioned tables as "Fallback mostly". In practice the scan itself often offloads. Fallback usually comes from expressions around it, e.g. `StaticInvoke` transform functions or unsupported metadata columns. Check the fallback reason in the UI (see [13 · Troubleshooting](13-metrics-ui-config-troubleshooting.md)).

### 6.3 Splits with delete files

`getSplitInfosFromPartitions` → `partitionToSplitInfo` → `GlutenIcebergSourceUtil.genSplitInfo`. For each `SparkInputPartition`, it expands `CombinedScanTask`/`FileScanTask`s and builds `IcebergLocalFilesNode` with:
- paths, starts, lengths, file format (Parquet/ORC);
- partition values (stringified via `TypeUtil.getPartitionValueString`, null → `__HIVE_DEFAULT_PARTITION__`);
- metadata columns (`input_file_name` etc.);
- **`task.deletes()`** for each data file.

`IcebergLocalFilesNode.toProtobuf` writes `FileOrFiles.iceberg` with the file format and a `DeleteFile` entry per delete. Each entry has: content (POSITION_DELETES / EQUALITY_DELETES), path, format, record count, file size.

Native side ([IcebergPlanConverter.cc](../../cpp/velox/compute/iceberg/IcebergPlanConverter.cc)): `parseIcebergSplitInfo` converts these to `IcebergDeleteFile`s, and adds an empty vector for files without deletes. `WholeStageResultIterator` then builds `HiveIcebergSplit(connectorIds_.hive, ..., customSplitInfo{"table_format":"hive-iceberg"}, deleteFiles, ...)`. Velox's Iceberg split reader opens each **position delete** file, keeps the positions that match the current data file path, and masks those rows while it reads.

### 6.4 Iceberg functions in expressions

Iceberg's Spark functions (`bucket`, `truncate`, `years`, `months`, `days`, `hours`) show up as `StaticInvoke` on `org.apache.iceberg.spark.functions.*Function`. `ExpressionConverter.icebergStaticInvokeMap` maps them to Substrait functions. Velox implementations come from `velox::functions::iceberg::registerFunctions()`. This matters for partition-transform expressions in filters and for distribution/sort on writes.

---

## 7. Iceberg write path (enhanced-features builds only)

### 7.1 Gating

| Gate | Default | Where |
|---|---|---|
| Native lib built with `--enable_enhanced_features=ON` (CMake `ENABLE_ENHANCED_FEATURES`, which compiles `compute/iceberg/IcebergWriter.cc` and defines `GLUTEN_ENABLE_ENHANCED_FEATURES`) | **OFF** | [builddeps-veloxbe.sh](../../dev/builddeps-veloxbe.sh), [cpp/CMakeLists.txt](../../cpp/CMakeLists.txt) |
| `spark.gluten.sql.enable.enhancedFeatures` | `true` | `VeloxConfig.ENABLE_ENHANCED_FEATURES` |
| Effective `VeloxConfig.enableEnhancedFeatures()` | = `ConfigJniWrapper.isEnhancedFeaturesEnabled && conf` | VeloxConfig.scala |
| `supportAppendDataExec` / `supportReplaceDataExec` / `supportOverwriteByExpression` / `supportOverwritePartitionsDynamic` / `supportWriteToDataSourceV2` | = `enableEnhancedFeatures()` | [VeloxBackend.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBackend.scala) |
| `Validators.FallbackByBackendSettings` fails these execs if unsupported | — | [Validators.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/validator/Validators.scala) |

> **Important:** The official Velox bundle is built without enhanced features, so there `isEnhancedFeaturesEnabled` returns false and **all five Iceberg write execs fall back**. That matches "Writing: Fallback" in `VeloxIceberg.md`. The offload code described below runs only on enhanced builds (the IBM Velox fork features).

When `supportAppendDataExec` is true, `VeloxRuleApi` also injects the shim's `RewriteCreateTableAsSelect` planner strategy, so CTAS/RTAS turn into append-style writes that can be offloaded.

### 7.2 Offload and validation

`OffloadIcebergWrite.inject` registers five rules. Each one replaces the Spark exec with a Velox exec only when `IcebergWriteUtil.supportsWrite(write)`, i.e. the `Write` is Iceberg's `SparkWrite`:

| Spark exec | Gluten exec | Typical SQL |
|---|---|---|
| `AppendDataExec` | `VeloxIcebergAppendDataExec` | `INSERT INTO` |
| `ReplaceDataExec` | `VeloxIcebergReplaceDataExec` | COW `UPDATE`/`DELETE`/`MERGE` |
| `OverwriteByExpressionExec` | `VeloxIcebergOverwriteByExpressionExec` | `INSERT OVERWRITE` (static) |
| `OverwritePartitionsDynamicExec` | `VeloxIcebergOverwritePartitionsDynamicExec` | dynamic `INSERT OVERWRITE` |
| `WriteToDataSourceV2Exec` | `VeloxIcebergWriteToDataSourceV2Exec` | streaming / micro-batch writes |

`IcebergWriteExec.doValidateInternal` ([IcebergWriteExec.scala](../../gluten-iceberg/src/main/scala/org/apache/gluten/execution/IcebergWriteExec.scala)) rejects the write in these cases:
- the write isn't a `SparkWrite`;
- the schema has UUID/FIXED types;
- backend schema validation fails;
- a partition field is on a DOUBLE/FLOAT column, is nested, or uses a void transform;
- the table has a **sort order**;
- the file format is **not Parquet**;
- the column names aren't Avro-compatible;
- `write.spark.accept-any-schema=true`;
- **merge-schema** is set.

### 7.3 Execution

A native Iceberg write, step by step.

1. On the driver, `AbstractIcebergWriteExec` builds an `IcebergDataWriteFactory` with:
   - write schema, Parquet format, data directory, codec;
   - `PartitionSpec` and nested field ids (as protobuf), sort order;
   - page size, target file size and query id.
2. In each task, the factory creates an `IcebergColumnarBatchDataWriter`.
3. The writer calls `IcebergWriteJniWrapper.init(...)` with the schema, format, directory, codec, spec, fields and configs.
4. For each `ColumnarBatch`, it calls `write(handle, batch)`. Velox's `IcebergWriter` appends the batch to an `IcebergDataSink`.
5. At the end, it calls `commit(handle)` and gets back an array of DataFile JSON strings.
6. It parses them (`DataFileJson`, `PartitionDataJson`, `MetricsWrapper`) into Iceberg `DataFile`s and returns them as a `WriterCommitMessage`.
7. On the driver, Iceberg's own `SparkWrite` commits them (append, overwrite or row delta).

- Velox computes partition values for identity and transform partitions. Iceberg partition specs and nested field ids are passed as protobuf (`IcebergPartitionSpec.proto`, `IcebergNestedField.proto`) so that Velox can write correct field ids into Parquet.
- `write.parquet.page-size-bytes` and the target file size are forwarded unless the user overrides them in the session.
- The commit still uses **Iceberg's own `BatchWrite`**, so snapshot isolation and conflict checks are unchanged.

---

## 8. End-to-end worked examples

### 8.1 Delta: read with deletion vectors

```scala
// spark-shell with the Gluten bundle built with -Pdelta, Spark 3.5 + Delta 3.3
spark.sql("""CREATE TABLE t (id BIGINT, v STRING) USING delta
             TBLPROPERTIES ('delta.enableDeletionVectors'='true')""")
spark.range(0, 1000000).selectExpr("id", "cast(id as string) v").write.mode("append").saveAsTable("t")
spark.sql("DELETE FROM t WHERE id % 10 = 0")   // writes DVs instead of rewriting files
spark.sql("SELECT count(*) FROM t WHERE v LIKE '9%'").explain()
```

What happens, step by step:
1. Delta's planner produces `FileSourceScanExec` over a `PreparedDeltaFileIndex`. Its `PreprocessTableWithDVsStrategy` adds `__delta_internal_is_row_deleted` plus `Filter(... = 0)`.
2. `OffloadDeltaScan`: Spark is 3.5 and `useMetadataRowIndex` defaults to true, so the scan becomes a `DeltaScanTransformer`. `OffloadDeltaFilter` / `OffloadDeltaProject` only fire if `increment_metric` is present, which it isn't for a plain SELECT.
3. `nativeDeletionVectorRule` removes the synthetic column and filter. The plan now looks like this (illustrative):
   ```text
   VeloxColumnarToRow
   +- ^ RegularHashAggregateExecTransformer [count(1)]
      +- ^ ... ColumnarExchange ...
         +- ^ FlushableHashAggregateExecTransformer [partial_count(1)]
            +- ^ FilterExecTransformer (v LIKE 9%)
               +- ^ DeltaScanTransformer parquet t [v]
   ```
4. On the driver, `getSplitInfosFromPartitions` loads each file's DV bitmap and emits `DeltaLocalFilesNode` with `IF_CONTAINED` and the serialized bitmap.
5. On executors, the Substrait plan routes the scan to the Delta connector. `DeltaSplitReader` reads Parquet and drops rows whose global row index is in the bitmap.
6. Result: identical to vanilla Delta. The whole stage runs natively.

### 8.2 Delta: column mapping

```sql
CREATE TABLE cm (a INT, b STRUCT<x: INT>) USING delta
  TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5');
ALTER TABLE cm RENAME COLUMN a TO a2;
SELECT a2, b.x FROM cm WHERE a2 > 5;
```

- The Parquet files contain columns named like `col-1a2b...`.
- `columnMappingRule` makes the scan `output` physical. `scanFilters` turns `a2 > 5` into `col-1a2b... > 5` for Velox. `dataFilters` stay logical so that Delta can skip files using stats on `a2`.
- A `ProjectExecTransformer` aliases `col-1a2b... AS a2` and rebuilds `b` with logical field names.

### 8.3 Iceberg: MOR read with position deletes

```sql
CREATE TABLE local.db.t (id BIGINT, data STRING) USING iceberg
  TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read');
INSERT INTO local.db.t SELECT id, cast(id AS STRING) FROM range(100000);
DELETE FROM local.db.t WHERE id < 100;           -- produces a position delete file
SELECT count(*), max(id) FROM local.db.t;
```

1. Iceberg plans `SparkBatchQueryScan`. Each `FileScanTask` lists the data file and its position delete file.
2. `OffloadIcebergScan` → `IcebergScanTransformer`. Validation passes: no equality deletes, format v2, no unsupported types.
3. Each split becomes an `IcebergLocalFilesNode` with `deletes=[POSITION_DELETES path, recordCount=100]`.
4. Velox builds `HiveIcebergSplit` with those delete files. It reads the delete file, filters entries for this data file path, and skips positions 0..99.
5. If you later run a Flink job that writes **equality deletes**, the snapshot summary shows `total-equality-deletes > 0`. From then on, every scan of the table falls back with `Contains equality delete files`.

### 8.4 Iceberg: write (enhanced build)

```sql
INSERT INTO local.db.t SELECT id, data FROM src WHERE length(data) = 1;
```

| Build | Plan (illustrative) |
|---|---|
| Standard bundle | `AppendData` (Spark, row-based) ← `VeloxColumnarToRow` ← offloaded read of `src` — **PartialOffload** |
| `--enable_enhanced_features=ON` | `VeloxIcebergAppendDataExec` ← offloaded read; Velox writes Parquet; Iceberg commits the DataFiles |

### 8.5 Seeing (and fixing) a lake-format fallback

Continuing 8.3: an equality delete appears in the table, e.g. written by a Flink upsert job or a MOR `MERGE` from another engine.

```scala
import org.apache.spark.sql.execution.GlutenImplicits._
val df = spark.sql("SELECT count(*), max(id) FROM local.db.t")
df.fallbackSummary().fallbackNodeToReason.foreach(println)
```

Output (illustrative):

```text
Map(001 BatchScan local.db.t -> ... Contains equality delete files)
```

The scan now runs in Spark's Iceberg reader, and a `RowToVeloxColumnar` appears above it so that the rest of the stage can stay native. Fixes:
- Compact the deletes away: `CALL local.system.rewrite_data_files(table => 'db.t', options => map('delete-file-threshold','1'))`. The next snapshot has `total-equality-deletes = 0`, so scans offload again.
- Or accept the fallback. Results are still correct; only the scan is slower.

The same workflow works for Delta. For example, on Spark 3.4 a table with deletion vectors shows `fallback Spark 3.4 Delta DV scan`.

---

## 9. Support matrix (v1.7.0, Velox)

| Feature | Delta | Iceberg |
|---|---|---|
| Build profile | `-Pdelta` | `-Piceberg` (`-Piceberg-test` for tests) |
| Default library version (Spark 3.5 profile) | delta-spark 3.3.2 | iceberg 1.10.0 |
| Other versions in pom | Spark 3.3 → Delta 2.3.0, 3.4 → 2.4.0, 4.0 → 4.0.1, 4.1 → 4.1.0 | Spark 3.3 → 1.5.0, 3.4/4.0 → 1.10.0, 4.1 → 1.11.0 |
| Officially documented | Spark 3.5 + Delta 3.3, Spark 4.0 + Delta 4.0 (`VeloxDelta.md`) | "all versions; Spark 3.4 well tested" (`VeloxIceberg.md`) |
| Plain read (COW) | ✅ native | ✅ native (unpartitioned; partitioned subject to §6.2) |
| Row-level deletes on read | DVs ✅ on Spark ≥ 3.5 (inline + on-disk, driver-loaded); ❌ Spark 3.4 | Position deletes ✅; equality deletes ❌; v3 Puffin DVs ❌ |
| Column mapping / renames | ✅ (name & id mapping) | ❌ renamed columns fall back |
| Metadata tables / log scans | `_delta_log` scans fall back | metadata tables fall back |
| Write | Experimental, `enableNativeWrite=false` default | Only on enhanced builds; standard bundle falls back |
| DML | UPDATE/DELETE/MERGE read side native; write side via native write if enabled | COW DML via `ReplaceData` (enhanced); MOR delete-file writes not offloaded |
| `CALL` procedures | n/a | fall back |

Delta feature table from `VeloxDelta.md`:
- **Yes:** column mapping, CDF, identity columns, liquid clustering.
- **Partial:** deletion vectors, generated columns, row tracking, type widening.
- **No:** CHECK constraints, TimestampNTZ.

---

## 10. Configuration reference

| Key | Default | Effect |
|---|---|---|
| `spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite` | `false` (experimental) | Turns on `OffloadDeltaCommand` / `GlutenOptimisticTransaction` |
| `spark.databricks.delta.deletionVectors.useMetadataRowIndex` (Delta) | `true` | If `false` and DVs are present, the Delta scan falls back |
| `spark.gluten.sql.enable.enhancedFeatures` | `true` | Enables Iceberg write execs, but only if the native lib has enhanced features |
| `spark.gluten.sql.columnar.appendData` / `.replaceData` / `.overwriteByExpression` / `.overwritePartitionsDynamic` | `true` | Per-exec user kill switches (`FallbackByUserOptions`) |
| `spark.gluten.sql.columnar.batchscan` | `true` | Disabling falls back all DSv2 scans, Iceberg included |
| `spark.gluten.sql.columnar.filescan` | `true` | Disabling falls back all V1 file scans, Delta included |
| `spark.sql.extensions` | — | Must include `io.delta.sql.DeltaSparkSessionExtension` / `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`. Gluten's own extension comes from the plugin |

---

## 11. Pitfalls & FAQ

- **"My Delta scan isn't offloaded on Spark 3.4."** That's expected when the table can contain DVs. The native DV path is gated to Spark ≥ 3.5.
- **Driver slow or tasks large on Delta tables with many DVs.** DV bitmaps are loaded on the driver and embedded in every task's Substrait plan. Run `OPTIMIZE`/`PURGE` to materialize the deletes.
- **Column-mapping tables return nulls or skip pruning.** These were real bugs (e.g. #10511). Make sure you are on v1.7.0 rules. When you debug, check that `dataFilters` are logical and `output` is physical.
- **One equality delete disables native reads for the whole Iceberg table.** The check uses the snapshot-wide `total-equality-deletes`. Compact with `rewrite_data_files` / `rewrite_position_delete_files` to remove it.
- **Iceberg v3 tables with deletes fall back.** v3 uses Puffin deletion vectors, which this path doesn't support.
- **"Iceberg write offload exists in code but never happens."** Check `ConfigJniWrapper.isEnhancedFeaturesEnabled`. Standard builds return false.
- **Iceberg write codec check quirk.** `IcebergWriteExec` tests `Seq("brotli, lzo").contains(codec)`. That is a sequence with one string, `"brotli, lzo"`, so brotli and lzo are *not* rejected by this check. Treat that as a known code smell, not a guarantee.
- **Sorted Iceberg tables / ORC writes / merge-schema** always fall back on write.
- **Delta native write is slower than expected.** Look for the `GlutenDeltaJobStatsFallbackTracker` warning, which means row conversions for stats. Optimized writes fall back to `DeltaOptimizedWriterExec` if the internal columnar shuffle can't be created.
- **How do I see why it fell back?** Use the Gluten UI tab or `spark.gluten.sql.debug`. Fallback reasons such as `fallback Delta _delta_log scan` or `Contains equality delete files` show up there (see [13 · Troubleshooting](13-metrics-ui-config-troubleshooting.md)).

---

## 12. Code map

| Path | Role |
|---|---|
| [gluten-delta/src/main/.../DeltaScanTransformer.scala](../../gluten-delta/src/main/scala/org/apache/gluten/execution/DeltaScanTransformer.scala) | Delta scan transformer, column-mapping filters, DV split infos |
| [OffloadDeltaScan.scala](../../gluten-delta/src/main/scala/org/apache/gluten/extension/OffloadDeltaScan.scala), `OffloadDeltaFilter.scala`, `OffloadDeltaProject.scala` | Delta offload rules |
| [DeltaPostTransformRules.scala](../../gluten-delta/src/main/scala/org/apache/gluten/extension/DeltaPostTransformRules.scala) | DV column stripping, input-file pushdown, column mapping |
| `gluten-delta/src-delta{20,23,24,33,40}/` | Version-specific `DeltaDeletionVectorScanInfo`, `DeltaFilter/ProjectExecTransformer` |
| [VeloxDeltaComponent.scala](../../backends-velox/src-delta/main/scala/org/apache/gluten/component/VeloxDeltaComponent.scala), [VeloxDeltaConfig.scala](../../backends-velox/src-delta/main/scala/org/apache/gluten/config/VeloxDeltaConfig.scala) | Component + native write config |
| `backends-velox/src-delta33/`, `src-delta40/` | Native write: `OffloadDeltaCommand`, `DeltaWriteOperators`, [GlutenOptimisticTransaction.scala](../../backends-velox/src-delta33/main/scala/org/apache/spark/sql/delta/GlutenOptimisticTransaction.scala), `GlutenDeltaFileFormatWriter`, stats trackers, `GlutenDeltaOptimizedWriterExec` |
| [cpp/velox/compute/delta/](../../cpp/velox/compute/delta/DeltaSplitReader.h) | Delta connector, data source, split reader, DV reader, `RoaringBitmapArray` |
| [VeloxPlanConverter.cc](../../cpp/velox/compute/VeloxPlanConverter.cc) | `parseDeltaSplitInfo`, Iceberg split parsing dispatch |
| `cpp/velox/operators/functions/delta/DeltaBitmapAggregator.cc` | `bitmapaggregator` native aggregate |
| [IcebergScanTransformer.scala](../../gluten-iceberg/src/main/scala/org/apache/gluten/execution/IcebergScanTransformer.scala), `OffloadIcebergScan.scala` | Iceberg scan offload + validation |
| [GlutenIcebergSourceUtil.scala](../../gluten-iceberg/src/main/scala/org/apache/iceberg/spark/source/GlutenIcebergSourceUtil.scala) | Split generation, partition schema, delete detection |
| [IcebergLocalFilesNode.java](../../gluten-iceberg/src/main/java/org/apache/gluten/substrait/rel/IcebergLocalFilesNode.java) | Substrait encoding of delete files |
| [IcebergWriteExec.scala](../../gluten-iceberg/src/main/scala/org/apache/gluten/execution/IcebergWriteExec.scala), [IcebergWriteUtil.scala](../../gluten-iceberg/src/main/scala/org/apache/iceberg/spark/source/IcebergWriteUtil.scala) | Write validation and reflection helpers |
| `backends-velox/src-iceberg/` ([OffloadIcebergWrite.scala](../../backends-velox/src-iceberg/main/scala/org/apache/gluten/extension/OffloadIcebergWrite.scala), [AbstractIcebergWriteExec.scala](../../backends-velox/src-iceberg/main/scala/org/apache/gluten/execution/AbstractIcebergWriteExec.scala), `IcebergWriteJniWrapper.java`, `IcebergColumnarBatchDataWriter.scala`) | Velox Iceberg write |
| [cpp/velox/compute/iceberg/](../../cpp/velox/compute/iceberg/IcebergPlanConverter.cc) | `IcebergPlanConverter`, `IcebergWriter`, `IcebergFormat` |
| `cpp/velox/tests/iceberg/`, `cpp/velox/compute/delta/tests/` | Native tests |
| `backends-velox/src-delta33/test/`, `src-iceberg*/test` | JVM suites (e.g. `DeltaDeletionVectorHandoffSuite`, `DeltaNativeWriteSuite`) |

## 13. Related docs

- [09 · Scan & data sources](09-scan-and-data-sources.md) (generic scan, splits) · [10 · Native write](10-native-write.md) (write framework, carrier rows) · [01 · Bootstrap](01-plugin-bootstrap-components-backends.md) (components) · [02 · Planning](02-planning-offload-validation-fallback.md) (validators, fallback) · [11 · Expressions](11-expressions-functions-udf.md) (StaticInvoke mapping)
- Repo: [VeloxDelta.md](../../docs/get-started/VeloxDelta.md), [VeloxIceberg.md](../../docs/get-started/VeloxIceberg.md), [Velox.md](../../docs/get-started/Velox.md)
