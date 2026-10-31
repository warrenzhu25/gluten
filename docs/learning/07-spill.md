# 07 · Spill

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [06 · Memory](06-memory-management.md) · Next: [08 · Shuffle](08-shuffle.md)

---

## TL;DR

- Spilling writes operator data to disk so a task can finish with less memory than its data needs.
- In Gluten, **Spark decides when to spill**. Gluten first returns unused reserved memory (cheap), and only then asks Velox operators (aggregation, join, sort, window) and the shuffle writer to spill.
- Spill files go to Spark's local dirs. Per-operator switches, file size and compression are all configurable.

**Technical summary**

- Spilling = writing operator state to disk to free memory, so a task can finish with less memory than its data needs.
- In Gluten, spilling is **driven by Spark's memory manager**: when a reservation can't be satisfied, Spark calls `spill()` on memory consumers, including Gluten's `TreeMemoryConsumer`, which walks its **memory-target tree** and invokes registered **`Spiller`s**.
- Spillers run in two phases: **SHRINK** first (return reserved-but-unused native pool capacity — cheap, no I/O), then **SPILL** (ask native operators to write data to disk). Within a phase, the largest children are asked first.
- Registered spillers per task:
  - `NativeMemoryManager` → SHRINK: `VeloxMemoryManager::shrink`.
  - `NativePlanEvaluator` → SPILL: `WholeStageResultIterator::spillFixedSize` → `ListenableArbitrator::shrinkCapacity` → **Velox `MemoryPool::reclaim`** → operator reclaimers (hash aggregation, hash join, order by, window, row number, ...).
  - `ColumnarShuffleWriter` → SPILL: native shuffle writer `reclaim` (evict partition buffers / sort and spill).
- Spills can be triggered **re-entrantly** while a Velox operator is allocating: allocation → Spark → `spill()` → Velox reclaims other operators (or the same task) safely inside an arbitration context.
- Velox spill behavior is configured via `spark.gluten.sql.columnar.backend.velox.*` (`spillStrategy=auto`, per-operator enable flags, partition bits, file size, compression...). Spill files go to Spark local dirs under a `gluten-spill` namespace.

---

## 1. Why spill exists and how it differs from vanilla Spark

| | Vanilla Spark | Gluten (Velox) |
|---|---|---|
| Who holds state | Java operators (UnsafeExternalSorter, BytesToBytesMap, ...) | Velox operators in native memory |
| Who decides to spill | Spark `TaskMemoryManager` calls `MemoryConsumer.spill()` | Same entry point → Gluten maps it to native reclaim |
| Spill format | Spark's own files | Velox spill files (serialized with Presto vector serde, optional compression) |
| Partitioned/recursive spill | Limited | Velox hash-partitioned spill with multiple levels (`maxSpillLevel`) for joins/aggregations |
| Partial aggregation | Spills like a normal agg | Usually **flushes** early (`FlushableHashAggregate`) or abandons partial agg instead of spilling |

The key design point: Gluten does **not** run its own memory pressure detection; it lets Spark arbitrate across *all* consumers (Gluten's native operators, Spark's own operators, shuffle writers) and translates spill requests to Velox.

---

## 2. Key concepts

| Concept | Description | Code |
|---|---|---|
| `Spiller` | `long spill(MemoryTarget self, Phase phase, long size)`; phases `SHRINK`, `SPILL` (order matters) | [Spiller.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/Spiller.java) |
| `Spillers` | Helpers: `NOOP`, `appendable()` list, `withMinSpillSize(spiller, reservationBlockSize)` | [Spillers.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/Spillers.java) |
| `TreeMemoryTargets.spillTree` | Phase loop + largest-child-first recursion | [TreeMemoryTargets.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/TreeMemoryTargets.java#L43-L82) |
| `TreeMemoryConsumer.spill` | Spark's entry point into Gluten | [TreeMemoryConsumer.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/spark/TreeMemoryConsumer.java) |
| `NativeMemoryManager` spiller | SHRINK only → JNI `shrink` | [NativeMemoryManager.scala](../../gluten-arrow/src/main/scala/org/apache/gluten/memory/NativeMemoryManager.scala) |
| Plan spiller | SPILL only → `ColumnarBatchOutIterator.spill` | [NativePlanEvaluator.java](../../gluten-arrow/src/main/java/org/apache/gluten/vectorized/NativePlanEvaluator.java) |
| Shuffle spiller | SPILL only → `ShuffleWriterJniWrapper.reclaim` | [ColumnarShuffleWriter.scala](../../backends-velox/src/main/scala/org/apache/spark/shuffle/ColumnarShuffleWriter.scala) |
| `spillFixedSize` | Native: shrink, then arbitrator reclaim | [WholeStageResultIterator.cc](../../cpp/velox/compute/WholeStageResultIterator.cc#L330-L352) |
| `ListenableArbitrator::shrinkCapacity` | Calls `pool->reclaim(targetBytes, maxWaitMs, stats)` then releases freed capacity to the listener | [VeloxMemoryManager.cc](../../cpp/velox/memory/VeloxMemoryManager.cc) |
| Velox query spill config | Mapped from Gluten configs in `WholeStageResultIterator::getQueryContextConf` | [WholeStageResultIterator.cc](../../cpp/velox/compute/WholeStageResultIterator.cc#L672-L705) |

---

## 3. Architecture

What happens when Spark needs memory back.

1. Spark's `TaskMemoryManager` needs memory and calls `TreeMemoryConsumer.spill(size)`.
2. That calls `TreeMemoryTargets.spillTree(root, size)`, which works in two phases. In each phase, the largest child goes first.
3. **Phase SHRINK.** The `NativeMemoryManager` spiller calls JNI shrink → `VeloxMemoryManager::shrink` → `arbitrator.shrinkCapacity(pool, 0)`. This only returns reserved-but-unused memory; there is no disk I/O.
4. **Phase SPILL** (only if still short):
   - Plan spiller: `ColumnarBatchOutIterator.spill` → `spillFixedSize` → `ListenableArbitrator.shrinkCapacity(bytes)` → root pool reclaim → Velox operator reclaimers (HashAggregation, HashBuild, OrderBy, Window, RowNumber, TopNRowNumber, …). These write spill files under Spark local dirs (`gluten-spill/...`).
   - Shuffle spiller: `shuffleWriter.reclaim(size)` writes shuffle spill files.

---

## 4. Detailed walkthrough

### 4.1 Spark → Gluten

Spark's `TaskMemoryManager.acquireExecutionMemory(required, consumer)` first tries to grant from the pool; if short, it calls `spill()` on other consumers of the task (largest first in recent Spark versions), and as a last resort on the requesting consumer itself. Gluten's consumer:

```java
@Override
public long spill(long size, MemoryConsumer trigger) throws IOException {
  return TreeMemoryTargets.spillTree(this, size);
}
```

### 4.2 Walking the tree

```java
public static long spillTree(TreeMemoryTarget node, final long bytes) {
  long remainingBytes = bytes;
  for (Spiller.Phase phase : Spiller.Phase.values()) {        // SHRINK, then SPILL
    if (remainingBytes <= 0) break;
    remainingBytes -= spillTree(node, phase, remainingBytes);
  }
  return bytes - remainingBytes;
}
private static long spillTree(TreeMemoryTarget node, Spiller.Phase phase, final long bytes) {
  // children sorted by usedBytes descending; recurse; then the node's own spiller if still short
}
```

Why this order?
- **SHRINK before SPILL** — returning idle pool capacity costs nothing; spilling costs disk I/O and later re-reads.
- **Largest first** — frees the most memory with the fewest disruptions.

`Spillers.withMinSpillSize(spiller, reservationBlockSize)` wraps each node's spiller so a request is at least one reservation block (8MB): native releases smaller than a block might not be reported back through the block-granular listener.

### 4.3 SHRINK phase (native memory manager)

```scala
spillers.append(new Spiller() {
  override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long = {
    if (!Spillers.PHASE_SET_SHRINK_ONLY.contains(phase)) return 0L   // only respond for shrinking
    NativeMemoryManagerJniWrapper.shrink(handle, size)
  }
})
```

Native `VeloxMemoryManager::shrink` → `arbitrator()->shrinkCapacity(rootPool, 0)` → frees unused capacity of pools and calls `listener->allocationChanged(-freed)`, which repays Spark.

### 4.4 SPILL phase (native plan)

```java
// NativePlanEvaluator: registered right after creating the kernel
public long spill(MemoryTarget self, Spiller.Phase phase, long size) {
  if (!Spiller.Phase.SPILL.equals(phase)) return 0L;
  return out.spill(size);                    // ColumnarBatchOutIterator.nativeSpill -> ResultIterator::spillFixedSize
}
```

```cpp
int64_t WholeStageResultIterator::spillFixedSize(int64_t size) {
  int64_t shrunken = memoryManager_->shrink(size);
  if (spillStrategy_ == "auto") {
    int64_t remaining = size - shrunken;
    uint64_t spilledOut = memoryManager_->getMemoryManager()->arbitrator()->shrinkCapacity(remaining); // conducts spill
    return shrunken + spilledOut;
  }
  LOG(WARNING) << "Spill-to-disk was disabled since " << kSpillStrategy << " was not configured.";
  return shrunken;
}
```

`ListenableArbitrator::shrinkCapacity(targetBytes, allowSpill, allowAbort)`:

```cpp
velox::memory::ScopedMemoryArbitrationContext ctx{};
pool->reclaim(targetBytes, memoryReclaimMaxWaitMs_, status);   // Velox walks child pools & operators
return shrinkCapacityInternal(pool, 0);                          // release freed capacity -> repay Spark
```

`MemoryPool::reclaim` asks each reclaimable operator's `MemoryReclaimer` to free memory. Spillable Velox operators (when enabled by config):

| Operator (Velox) | Gluten transformer | Spill behavior |
|---|---|---|
| `HashAggregation` (final/single) | `RegularHashAggregateExecTransformer` | Spill sorted/partitioned rows; merge on output |
| `HashAggregation` (partial) | `FlushableHashAggregateExecTransformer` | Prefer **flush** (emit partial results early) / abandon partial agg; spill if configured |
| `HashBuild` / `HashProbe` | `ShuffledHashJoinExecTransformer`, broadcast hash join (BHJ) | Hash-partitioned spill of build (and probe) sides; recursive up to `maxSpillLevel` |
| `OrderBy` | `SortExecTransformer` | Sorted runs spilled, merged on output |
| `Window` | `WindowExecTransformer` | Spill partitions (`windowSpillEnabled`) |
| `RowNumber`, `TopNRowNumber` | window group limit | Spill hash table / partitions |

Operators that can't spill at the moment (e.g. in a non-reclaimable section) are skipped; the arbitration wait is bounded by `reclaimMaxWaitMs` (default 60 min).

### 4.5 SPILL phase (shuffle writer)

```scala
runtime.memoryManager().addSpiller(new Spiller() {
  override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long = phase match {
    case Spiller.Phase.SPILL =>
      val spilled = shuffleWriterJniWrapper.reclaim(nativeShuffleWriter, size)
      spilled
    case _ => 0L
  }
})
```

The native writer frees memory by evicting cached partition payloads / partition buffers to a spill file (hash writer) or sorting and writing buffered rows (sort writer). Details in [08](08-shuffle.md).

### 4.6 Re-entrant spill during allocation

A re-entrant spill: an operator needs memory while it is running.

1. Velox `HashAggregation` (on the task thread) needs another 16MB and asks `ListenableArbitrator.growCapacity`.
2. The arbitrator tells the JNI `ReservationListener`: allocation changed by +16MB.
3. The listener calls Spark `acquireExecutionMemory(16MB)`.
4. Spark's pool is exhausted, so it calls `TreeMemoryConsumer.spill(16MB)`.
5. Gluten runs SHRINK, then SPILL, through `spillFixedSize`.
6. `spillFixedSize` calls `shrinkCapacity` → `pool.reclaim`. A `ScopedMemoryArbitrationContext` allows reclaiming this task's own operators.
7. Freed bytes (spilled to disk) flow back up: arbitrator → `spillFixedSize` → Gluten → Spark.
8. Spark grants the 16MB to the listener, the arbitrator grows the pool, and the aggregation continues.

`ScopedMemoryArbitrationContext` (set in `growCapacity`/`shrinkCapacity`) lets Velox perform reclaim while the requesting operator is mid-allocation (allowing temporary over-use per Velox's rules). If reclaim can't free enough, `RetryOnOom` spills the whole task tree once more, then `ThrowOnOom` fails the task ([06](06-memory-management.md)).

### 4.7 Where spill files go

- Each native kernel gets `spillDirPath = SparkDirectoryUtil.get().namespace("gluten-spill").mkChildDirRoundRobin(UUID)` — i.e. inside Spark's local dirs (`spark.local.dir` / YARN local dirs), round-robin across disks.
- `spark.gluten.sql.columnar.backend.velox.spillFileSystem`:
  - `local` (default) — local filesystem.
  - `heap-over-local` — the path is rewritten to the `jol://` scheme (a JVM-heap-backed filesystem registered at backend init): spill to **JVM heap** when there's spare heap, otherwise local disk. Useful when heap is under-used and disks are slow.
- Spill compression: if `spark.shuffle.spill.compress=true` (Spark default), codec = `spark.gluten.sql.columnar.backend.velox.spillCompressionCodec` or `spark.io.compression.codec` (default `lz4`).

---

## 5. End-to-end worked examples

### 5.1 Scenario A — Spark needs memory for another consumer

A task runs a native stage (aggregation) and then, in the same task, a vanilla Spark `Sort` (fallback) needs 200MB:

1. Spark `UnsafeExternalSorter` asks `TaskMemoryManager` for 200MB; the off-heap pool is full because the native aggregation holds 1.5GB.
2. Spark calls `TreeMemoryConsumer.spill(200MB)`.
3. SHRINK: native pools return 40MB of idle capacity.
4. SPILL: plan spiller → `spillFixedSize(160MB)` → Velox aggregation spills ~160MB of hash table rows to `.../gluten-spill/<uuid>/...`.
5. Spark grants the sorter 200MB; the aggregation later merges its spilled runs when producing output.

You'll see in the executor log (illustrative):

```text
INFO WholeStageResultIterator: Spill[root/root]: trying to request spill for 160.00MB.
INFO WholeStageResultIterator: Spill[root/root]: successfully reclaimed total 200.12MB with shrunken 40.00MB and spilled 160.12MB.
```

and in the SQL UI on the aggregate transformer: `bytes written for spilling`, `total rows written for spilling`, `total spilled partitions`, `total spilled files` > 0 (names from `VeloxMetricsApi`).

### 5.2 Scenario B — the aggregation itself runs out of memory

1. During `addInput`, the hash table grows; Velox pool needs +8MB → `growCapacity` → listener → Spark.
2. Spark has no free memory; it spills other consumers first (e.g. a shuffle writer in the same task evicts buffers), then possibly Gluten's own consumer.
3. Gluten's spill path reaches `pool->reclaim` **for the same task**; Velox's aggregation reclaimer spills its own hash table (allowed within the arbitration context), then the allocation succeeds.
4. If nothing can be freed (e.g. a single huge row group), `RetryOnOom` → `ThrowOnOom` → task fails with the memory dump.

### 5.3 Example config for a spill-heavy job

```text
spark.memory.offHeap.size=16g
spark.gluten.sql.columnar.backend.velox.spillStrategy=auto
spark.gluten.sql.columnar.backend.velox.maxSpillFileSize=1GB
spark.gluten.sql.columnar.backend.velox.spillPartitionBits=3          # 8 spill partitions per level
spark.gluten.sql.columnar.backend.velox.maxSpillLevel=4
spark.gluten.sql.columnar.backend.velox.spillCompressionCodec=zstd    # smaller spill files, more CPU
spark.local.dir=/mnt/disk1,/mnt/disk2                                 # spread spill I/O
```

**Try it / verify**

- Force a spill on a laptop: `spark.memory.offHeap.size=512m`, then `spark.range(0, 50000000).selectExpr("id % 10000000 AS k", "id AS v").groupBy("k").count().collect()`.
- Executor log: `Spill[...]: trying to request spill for ...` lines from `WholeStageResultIterator`.
- SQL UI on the aggregate: `bytes written for spilling` > 0. Spill files appear under the Spark local dirs while the task runs.

---

## 6. Configuration reference

Velox spill settings are passed to the Velox `QueryConfig` per task (defaults below are the native fallbacks used in `getQueryContextConf`; JVM docs in [docs/get-started/Velox.md](../../docs/get-started/Velox.md#L460-L478)).

| Key (`spark.gluten.sql.columnar.backend.velox.` prefix unless noted) | Default | Effect |
|---|---|---|
| `spillStrategy` | `auto` | `auto`: Spark-driven spilling; `none`: disable Velox spill (only shrink) |
| `aggregationSpillEnabled` | `true` | Spill hash aggregations |
| `joinSpillEnabled` | `true` | Spill hash joins |
| `orderBySpillEnabled` | `true` | Spill sorts |
| `windowSpillEnabled` | `true` | Spill windows |
| `maxSpillLevel` | `4` | Max recursive spill levels (hash-partitioned) |
| `maxSpillFileSize` | `1GB` | Max size per spill file (`0` = unlimited) |
| `spillStartPartitionBit` | `48` | Start bit of hash used to pick spill partition |
| `spillPartitionBits` | `3` | Spill partitions per level = 2^bits |
| `spillableReservationGrowthPct` | `25` | Growth % of spillable reservations |
| `spillPrefixsortEnabled` | `false` | Use prefix sort when spilling |
| `MaxSpillRunRows` (native key; JVM config is `maxSpillRunRows`) | `3M rows` | Rows per spill run |
| `MaxSpillBytes` | `100GB` | Max total spill bytes per query (Velox) |
| `spillCompressionCodec` | unset → `spark.io.compression.codec` (lz4) | Codec for spill files (if `spark.shuffle.spill.compress=true`) |
| `spillFileSystem` | `local` | `local` or `heap-over-local` |
| `spillThreadNum` | `0` | Dedicated spill threads (experimental) |
| `reclaimMaxWaitMs` | `3600000` | Max wait in memory reclaim |
| `maxPartialAggregationMemoryRatio` | `0.1` | Partial agg memory cap = ratio × task off-heap (min 16MB) before flushing |
| `maxExtendedPartialAggregationMemoryRatio` | `0.15` | Extended cap (min 64MB) |
| `abandonPartialAggregationMinPct` / `abandonPartialAggregationMinRows` | `90` / `100000` | Abandon partial agg when it doesn't reduce rows enough |
| `flushablePartialAggregation` | `true` | Enable flushable partial aggregation |
| `spark.unsafe.sorter.spill.reader.buffer.size` | `1MB` (native fallback) | Spill read buffer |
| `spark.shuffle.spill.diskWriteBufferSize` | `1MB` (native fallback) | Spill write buffer |

> **Note:** The JVM config name for max spill run rows (`maxSpillRunRows`) differs in case from the native constant (`MaxSpillRunRows`). If tuning it, verify which key reaches native code in your build (e.g. with `spark.gluten.sql.debug=true`, which logs the native session config).

---

## 7. Pitfalls, debugging & FAQ

| Symptom | Cause | Fix |
|---|---|---|
| Task OOM although spill is enabled | Operator not spillable at that moment, or `spillStrategy=none` | Check strategy & per-operator flags; increase off-heap; reduce cores |
| `Spill-to-disk was disabled since ... spillStrategy was not configured` | `spillStrategy` not `auto` | Set `auto` |
| Very slow tasks with huge `bytes written for spilling` | Memory too small or skew | More off-heap / fewer cores / fix skew (AQE skew join) |
| Disk full in local dirs | Spills + shuffle | Add disks to `spark.local.dir`; enable compression |
| Hash join spills repeatedly to max level | Heavy key skew | AQE skew handling; consider broadcast; check `maxSpillLevel` |
| Shuffle writer spilling a lot | Partition buffers too large / many partitions | See shuffle tuning in [08](08-shuffle.md) |

**FAQ**
- *Does a spill free Spark execution memory immediately?* Yes — reclaimed capacity is released via `allocationChanged(-bytes)` → `repay` → Spark.
- *Can Spark spill native operators of other tasks?* No. Spark only spills consumers of the **same task**. Cross-task pressure is handled by Spark's per-task fairness and by each task spilling itself.
- *Are partial aggregations spilled?* Typically they flush/abandon instead; that's cheaper than spilling because the final aggregation will merge anyway.

---

## 8. Code map

| File | Role |
|---|---|
| [Spiller.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/Spiller.java), [Spillers.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/Spillers.java) | Spiller API |
| [TreeMemoryTargets.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/TreeMemoryTargets.java) | spillTree |
| [TreeMemoryConsumer.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/spark/TreeMemoryConsumer.java) | Spark entry |
| [NativeMemoryManager.scala](../../gluten-arrow/src/main/scala/org/apache/gluten/memory/NativeMemoryManager.scala) | Shrink spiller |
| [NativePlanEvaluator.java](../../gluten-arrow/src/main/java/org/apache/gluten/vectorized/NativePlanEvaluator.java) | Plan spiller |
| [ColumnarShuffleWriter.scala](../../backends-velox/src/main/scala/org/apache/spark/shuffle/ColumnarShuffleWriter.scala) | Shuffle spiller |
| [WholeStageResultIterator.cc](../../cpp/velox/compute/WholeStageResultIterator.cc) | spillFixedSize; Velox spill configs |
| [VeloxMemoryManager.cc](../../cpp/velox/memory/VeloxMemoryManager.cc) | Arbitrator reclaim |
| [VeloxConfig.h](../../cpp/velox/config/VeloxConfig.h), [VeloxConfig.scala](../../backends-velox/src/main/scala/org/apache/gluten/config/VeloxConfig.scala) | Config keys |
| [VeloxSparkPlanExecApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxSparkPlanExecApi.scala) | `rewriteSpillPath` (heap-over-local) |

## 9. Related docs

- [06 · Memory](06-memory-management.md) — the reservation path that triggers spills
- [08 · Shuffle](08-shuffle.md) — shuffle writer eviction
- [12 · Operators](12-operators-join-agg-window-sort.md) — flushable aggregation, joins
- Repo: [docs/get-started/Velox.md#spill](../../docs/get-started/Velox.md)
