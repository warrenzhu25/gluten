# 10 · Native Write Path

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [09 · Scan](09-scan-and-data-sources.md) · Next: [11 · Expressions](11-expressions-functions-udf.md)

---

## TL;DR

- On Spark 3.4+, Gluten writes **Parquet natively**: the data goes from the last native operator straight to files, with no row conversion.
- Spark's commit protocol (staging dirs, task/job commit, partition discovery) is unchanged, so output and atomicity are the same.
- Unsupported writes (other formats, some options/codecs) fall back to Spark's writer.

**Technical summary**

- Since Spark 3.4, Spark plans file writes with a physical **`WriteFilesExec`** node under `DataWritingCommandExec`.
  - Gluten offloads it to **`WriteFilesExecTransformer`** so the data being written never leaves native memory: the last native stage ends with a Velox **`TableWriteNode`** that writes **Parquet** files directly.
- Spark's **commit protocol** (`FileCommitProtocol`, job/task commit, staging dirs, partition discovery, stats) is still used.
  - `VeloxColumnarWriteFilesExec` / `VeloxColumnarWriteFilesRDD` bridge them: they give native code a **staging path + file name**, let Velox write, then read back per-file **write metadata** and build Spark's `WriteTaskResult`.
- Native write is enabled by default on Spark 3.4+ shims (`enableNativeWriteFilesByDefault`), overridable via `spark.gluten.sql.native.writer.enabled`.
- Supported: **Parquet** (`ParquetFileFormat`; `HiveFileFormat` with Parquet output when `spark.gluten.sql.native.hive.writer.enabled=true`, the default), static & dynamic partitions, Hive-compatible bucketed partitioned tables.
- Not supported → fallback: other formats (ORC/CSV/JSON writes), `maxRecordsPerFile > 0`, codecs `brotli/lzo/lz4raw`, year-month intervals, fields with metadata, non-Hive-compatible bucketing, CTAS via `CreateDataSourceTableAsSelectCommand` (`skipNativeCtas = true`), `InsertIntoHadoopFsRelationCommand` with bucket spec.
- Data lakes: Delta native write is experimental (`spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=false`); Iceberg writes have offload rules (`OffloadIcebergWrite`) but are documented as fallback in v1.7.0 docs; DSv2 append/overwrite execs are gated by `enableEnhancedFeatures()`.

---

## 1. Why native writes

Without native write, a query like `INSERT ... SELECT` would run natively up to the end and then convert every batch to rows (`VeloxColumnarToRow`) just to feed Spark's row-based Parquet writer, which converts back to columns. Native write:

- removes that C2R + Java encoding,
- uses Velox's Parquet writer (vectorized encoding, compression),
- keeps memory accounted & spillable in one place.

It deliberately **does not** replace Spark's commit protocol, so correctness properties (atomic job commit, speculation handling, dynamic partition overwrite) remain Spark's.

---

## 2. Key concepts

| Concept | Description | Code |
|---|---|---|
| `WriteFilesExec` (Spark 3.4+) | Spark physical node performing file writes under a write command | Spark |
| `WriteFilesExecTransformer` | Offloaded write; builds Substrait `WriteRel` (table schema, partition columns, bucket spec, options) | [WriteFilesExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/WriteFilesExecTransformer.scala) |
| `ColumnarWriteFilesExec` | Columnar replacement of the write node; implements `doExecuteWrite` | [ColumnarWriteFilesExec.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/execution/ColumnarWriteFilesExec.scala) |
| `VeloxColumnarWriteFilesExec` / `VeloxColumnarWriteFilesRDD` | Velox implementation; per task: setup commit protocol, inject temp path, run native plan, collect metadata, commit | [VeloxColumnarWriteFilesExec.scala](../../backends-velox/src/main/scala/org/apache/spark/sql/execution/VeloxColumnarWriteFilesExec.scala) |
| `supportWriteFilesExec` | Velox validation of format/codec/types/options/buckets | [VeloxBackend.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBackend.scala#L298-L400) |
| `RemoveNativeWriteFilesSortAndProject` | Removes Spark's partition-ordering sort/project added for row writers when native write handles partitions | [RemoveNativeWriteFilesSortAndProject.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/RemoveNativeWriteFilesSortAndProject.scala) |
| `GlutenWriterColumnarRules` | Write-related rules (incl. Spark 3.3 `NativeWritePostRule`) | [GlutenWriterColumnarRules.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/execution/datasources/GlutenWriterColumnarRules.scala) |
| `VeloxParquetWriterInjects` | Registers Velox Parquet writer with `GlutenFormatFactory` | [VeloxParquetWriterInjects.scala](../../backends-velox/src/main/scala/org/apache/spark/sql/execution/datasources/velox/VeloxParquetWriterInjects.scala) |
| `WriteRel` → `TableWriteNode` | Native conversion with Hive insert table handle, partition keys, bucket property | [SubstraitToVeloxPlan.cc](../../cpp/velox/substrait/SubstraitToVeloxPlan.cc#L792-L906) |

---

## 3. Architecture

How a native write fits into Spark's write path.

**Driver:**

1. `InsertIntoHadoopFsRelationCommand` (under `DataWritingCommandExec`) plans a `WriteFilesExec` (Spark 3.4+).
2. `OffloadOthers` replaces it with `WriteFilesExecTransformer` if `supportWriteFilesExec` passes.
3. It is executed by `VeloxColumnarWriteFilesExec`, a columnar write node.
4. Spark's `FileCommitProtocol` still runs `setupJob` … `commitJob`.

**Each executor task:**

1. `VeloxColumnarWriteFilesRDD.compute` asks `SparkWriteFilesCommitProtocol` for `setupTask`, the temp path and the file name.
2. It passes them to native code with `injectWriteFilesTempPath(path, fileName)` (JNI).
3. The native stage ends in a Velox `TableWriteNode`, which writes Parquet.
4. Native code returns one result batch with per-file metadata (JSON fragments).
5. The JVM builds Spark's `WriteTaskResult` (files, partitions, stats) and calls `commitTask`. The driver's `commitJob` then finishes the job.

---

## 4. Detailed walkthrough

### 4.1 Planning

1. Spark's planner produces `DataWritingCommandExec(InsertIntoHadoopFsRelationCommand, WriteFilesExec(child))` (Spark 3.4+; on 3.3 Gluten uses a post rule `NativeWritePostRule`).
2. `FallbackByBackendSettings` rejects `WriteFilesExec` if `enableNativeWriteFiles()` is false (default true on 3.4+ unless `spark.gluten.sql.native.writer.enabled=false`).
3. `OffloadOthers` creates `WriteFilesExecTransformer(child, fileFormat, partitionColumns, bucketSpec, options, staticPartitions)`.
4. Validation (`doValidateInternal`):
   - `supportWriteFilesExec` (format, codec, field metadata, data types, `maxRecordsPerFile`, bucket compatibility),
   - checks like constant complex-type expressions in the child,
   - native validation of the `WriteRel` (`SubstraitToVeloxPlanValidator::validate(WriteRel)`).
5. Post-transform `RemoveNativeWriteFilesSortAndProject`: Spark adds a local sort by partition columns so its row writer can write one partition at a time; Velox's writer handles multiple open partitions itself, so the sort is removed when safe.
6. The write node becomes `VeloxColumnarWriteFilesExec` with the transformer stage as its child (stage ends in the write).

### 4.2 Substrait `WriteRel` and Velox `TableWriteNode`

`WriteFilesExecTransformer.getRelNode` builds `RelBuilder.makeWriteRel(input, tableSchema with column types (regular / partition key), bucketSpec?, extension with write options (codec, ...))`. Native `toVeloxPlan(WriteRel)`:
- determines partition keys from column types,
- builds a `HiveBucketProperty` for bucketed writes,
- creates a Hive **insert table handle** with location = the injected temp path, file format Parquet, compression, and serde/writer options,
- returns a `TableWriteNode` above the child plan.

### 4.3 Per-task execution (`VeloxColumnarWriteFilesRDD.compute`)

```scala
val commitProtocol = new SparkWriteFilesCommitProtocol(jobTrackerID,
    description, committer)
commitProtocol.setupTask()
val writePath = commitProtocol.newTaskAttemptTempPath()
val fileName  = commitProtocol.getFilename(FileNameSpec("", suffix))
// -> native
BackendsApiManager.getIteratorApiInstance.injectWriteFilesTempPath(writePath,
    fileName)
// runs the native stage incl. TableWriteNode
val iter = firstParent[ColumnarBatch].iterator(split, context)
// one batch of write results
val resultColumnarBatch = iter.next()
val nativeWriteTaskResult = collectNativeWriteFilesMetrics(resultColumnarBatch)
// vanilla empty file for metadata
if (nativeWriteTaskResult.isEmpty) writeTaskResult =
    writeFilesForEmptyIterator(commitProtocol)
else { writeTaskResult = nativeWriteTaskResult.get;
    commitProtocol.commitTask() }
```

`collectNativeWriteFilesMetrics` loads the result batch into the JVM and parses JSON **fragments** (`VeloxWriteFilesMetrics`: per partition name, file infos `writeFileName/targetFileName/fileSize`, row counts). It builds `BasicWriteTaskStats(partitions, numFiles, numBytes, numRows)`, `ExecutedWriteSummary`, and a `TaskCommitMessage` with absolute-path files for partitions — exactly what Spark's job commit expects. Output metrics (`bytesWritten`, `recordsWritten`) are reported to the task.

On errors: `abortTask(writePath)`; `FileAlreadyExistsException` honors `spark.sql.execution.fastFailOnFileFormatOutput`.

### 4.4 Parquet writer settings

See [docs/velox-parquet-write-configuration.md](../../docs/velox-parquet-write-configuration.md). Highlights:
- Write batch size ← `spark.gluten.sql.columnar.maxBatchSize`.
- Row group length ← `parquet.block.rows` option / `spark.gluten.sql.native.parquet.write.blockRows`.
- Block size ← `spark.gluten.sql.columnar.parquet.write.blockSize`.
- Compression ← `spark.sql.parquet.compression.codec` / `compression` option (snappy, gzip, zstd, lz4, none; not brotli/lzo/lz4raw).
- Velox target file size: `spark.gluten.sql.columnar.backend.velox.parquetMaxTargetFileSize`; partitions per writer session: `...velox.maxPartitionsPerWritersSession`.

---

## 5. End-to-end worked example

```sql
CREATE TABLE sales_out (store STRING, amount DECIMAL(10,2), dt STRING)
USING parquet PARTITIONED BY (dt);

INSERT OVERWRITE TABLE sales_out PARTITION (dt)
SELECT store, amount, dt FROM sales WHERE amount > 0;
```

Illustrative plan:

```text
Execute InsertIntoHadoopFsRelationCommand ... sales_out, [dt], Overwrite
+- VeloxColumnarWriteFilesExec
   +- WholeStageCodegenTransformer (1)
      +- ^ WriteFilesExecTransformer (partition columns: [dt], format: parquet)
         +- ^ ProjectExecTransformer [store, amount, dt]
            +- ^ FilterExecTransformer (amount > 0)
               +- ^ FileSourceScanExecTransformer parquet sales
```

Per task:
1. Commit protocol gives `.../_temporary/0/_temporary/attempt_.../` and file name `part-00007-<uuid>.c000.snappy.parquet`.
2. Velox `TableWriteNode` writes one file per `dt` value it encounters under the staging path (e.g. `.../dt=2026-10-01/<writeFileName>`), and reports the mapping from its write file name to the Spark target file name.
3. Result fragment per written partition (shape follows Velox `HiveDataSink::close()`, as documented in `VeloxColumnarWriteFilesExec.scala`; values illustrative): `{"name":"dt=2026-10-01","inMemoryDataSizeInBytes":0,"onDiskDataSizeInBytes":1048576,"containsNumberedFileNames":true,"fileWriteInfos":[{"writeFileName":"Gluten_Stage_1_TID_7_....parquet","targetFileName":"part-00007-....parquet","fileSize":1048576}],"rowCount":12345,...}`.
4. `commitTask`; job commit (dynamic partition overwrite) moves files into place.

What you'll observe: no `VeloxColumnarToRow` at the top of the write; output metrics show bytes/records written; write metrics in the SQL UI (e.g. `physical written bytes`, `write IO time`, `number of written files` from Velox stats).

**Try it / verify**

- EXPLAIN shows `VeloxColumnarWriteFilesExec` under the insert command; there is no `VeloxColumnarToRow` between the last native operator and the write.
- SQL UI on the write node: `number of written files` and output bytes.
- Compare with `spark.gluten.sql.native.writer.enabled=false`: the plan goes back to Spark's `WriteFiles` with a `VeloxColumnarToRow` in front. Row counts and the partition directory layout should match.

---

## 6. Configuration reference

| Key | Default | Effect |
|---|---|---|
| `spark.gluten.sql.native.writer.enabled` | unset → shim default (`true` on Spark 3.4+) | Enable native `WriteFilesExec` offload |
| `spark.gluten.sql.native.hive.writer.enabled` | `true` | Native write for `HiveFileFormat` backed by Parquet |
| `spark.gluten.sql.native.parquet.write.blockRows` | see doc | Parquet row group rows |
| `spark.gluten.sql.columnar.parquet.write.blockSize` | see `GlutenConfig` | Parquet block size |
| `spark.gluten.sql.columnar.backend.velox.parquetMaxTargetFileSize` | see `VeloxConfig` | Target file size for Velox writer |
| `spark.gluten.sql.columnar.backend.velox.maxPartitionsPerWritersSession` | see `VeloxConfig` | Max open partitions per writer |
| `spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite` | `false` | Experimental native Delta write |
| `spark.sql.parquet.compression.codec` (Spark) | `snappy` | Codec |
| `spark.sql.files.maxRecordsPerFile` (Spark) | `0` | Must be 0 for native write |

---

## 7. Pitfalls & FAQ

| Symptom | Cause | Fix |
|---|---|---|
| Write falls back with `maxRecordsPerFile not supported` | `spark.sql.files.maxRecordsPerFile > 0` | Set to 0 or accept fallback |
| `... compression codec is unsupported in Velox backend` | brotli/lzo/lz4raw | Use snappy/zstd/gzip |
| `Only ParquetFileFormat and HiveFileFormat are supported` | ORC/CSV/JSON output | Expected; those writes are vanilla |
| `StructField contain the metadata information` | Column metadata (e.g. char/varchar) | Expected fallback |
| CTAS not native | `skipNativeCtas = true` for `CreateDataSourceTableAsSelectCommand` | Use `CREATE TABLE` + `INSERT` |
| Empty task writes a vanilla file | Intentional: produces metadata-only Parquet for empty partitions | — |

**FAQ**
- *Is the output byte-identical to Spark's writer?* No — it's a different Parquet writer (Velox/Arrow). It follows Spark/Parquet compatibility settings (e.g. timestamp type) documented in the Parquet write configuration page; validate downstream readers if you rely on writer-specific metadata.
- *Spark 3.3?* No `WriteFilesExec` in 3.3; Gluten registers `NativeWritePostRule` for 3.3 only.

---

## 8. Code map

| File | Role |
|---|---|
| [WriteFilesExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/WriteFilesExecTransformer.scala) | Transformer |
| [ColumnarWriteFilesExec.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/execution/ColumnarWriteFilesExec.scala) | Columnar write node |
| [VeloxColumnarWriteFilesExec.scala](../../backends-velox/src/main/scala/org/apache/spark/sql/execution/VeloxColumnarWriteFilesExec.scala) | Velox write RDD & commit glue |
| [VeloxBackend.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBackend.scala) | `supportWriteFilesExec`, `enableNativeWriteFiles` |
| [RemoveNativeWriteFilesSortAndProject.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/RemoveNativeWriteFilesSortAndProject.scala) | Sort removal |
| [SubstraitToVeloxPlan.cc](../../cpp/velox/substrait/SubstraitToVeloxPlan.cc) | WriteRel conversion |
| [VeloxParquetWriterInjects.scala](../../backends-velox/src/main/scala/org/apache/spark/sql/execution/datasources/velox/VeloxParquetWriterInjects.scala) | Writer injection |

## 9. Related docs

- [09 · Scan](09-scan-and-data-sources.md) · [16 · Delta & Iceberg](16-delta-and-iceberg-integration.md) (lake-format writes) · [05 · Transitions](05-columnar-batches-conventions-transitions.md) (carrier rows for Delta writer path) · [13 · Troubleshooting](13-metrics-ui-config-troubleshooting.md)
- Repo: [velox-parquet-write-configuration.md](../../docs/velox-parquet-write-configuration.md), [VeloxDelta.md](../../docs/get-started/VeloxDelta.md)
