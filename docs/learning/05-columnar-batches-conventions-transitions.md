# 05 · Columnar Batches, Conventions & Transitions

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [04 · Native runtime](04-native-execution-runtime-jni.md) · Next: [06 · Memory](06-memory-management.md)

---

## TL;DR

- A Gluten "columnar batch" usually just **points at data held in C++**; the JVM never copies it.
- Each plan node declares what it produces and needs (rows, native batches, …). Gluten inserts **conversion nodes** wherever they don't match.
- The usual conversions are `VeloxColumnarToRow` (native → Spark rows) and `RowToVeloxColumnar` (rows → native). They cost CPU, so lots of them in a plan is a sign of fallbacks to fix.

**Technical summary**

- Spark's `ColumnarBatch` is just a container of `ColumnVector`s. Gluten uses it in three flavors:
  - **Light (native) batch** — column 0 is an `IndicatorVector` holding a native handle, other columns are `PlaceholderVector`s. Data lives in C++ (a Velox `RowVector`). Produced by every native operator.
  - **Heavy (Java Arrow) batch** — columns are `ArrowWritableColumnVector`s readable from the JVM.
  - **Zero-column batch** — only a row count.
- The **Convention** system describes what each plan node produces and requires: a `RowType` (vanilla rows, Velox carrier rows, none) and a `BatchType` (vanilla, Velox, Arrow-native, Arrow-Java, none).
- Each type registers **transitions** (edges) to other types. `InsertTransitions` finds the cheapest path in the **transition graph** (Floyd–Warshall over a cost model) and inserts the needed nodes.
- The two transitions you'll see most: **`VeloxColumnarToRow`** (native batch → `UnsafeRow`, converted in C++ in memory-bounded chunks and read zero-copy by the JVM) and **`RowToVeloxColumnar`** (JVM rows → copied into a buffer → native batch).
- Transitions cost CPU and memory; many of them in one plan usually means an unsupported operator in the middle of a native pipeline. Avoid them by fixing fallbacks or letting whole stages fall back.
- **`VeloxResizeBatches`** is a columnar→columnar "transition-like" node that coalesces small batches before shuffle (enabled by default for shuffle input).

---

## 1. Why conventions exist

Early Gluten hard-coded "insert C2R here, R2C there". That broke as more batch formats appeared (Arrow Java batches for Python UDFs and caching, Arrow native batches for Arrow-based sources, GPU batches, carrier rows for writers). The convention system makes this declarative:

- Plan nodes declare their **output convention** and **required child conventions**.
- Batch/row types declare **how to convert** to/from each other.
- A generic rule computes and inserts the conversions.

Benefits: new formats or backends plug in without touching planner rules; transitions are minimal (cheapest path); Spark's own `ColumnarToRowExec`/`RowToColumnarExec` are handled uniformly.

---

## 2. Key concepts

| Concept | Description | Code |
|---|---|---|
| `Convention` | `(rowType, batchType)` pair for a plan node | [Convention.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/Convention.scala) |
| `ConventionReq` | What a parent requires from a child (`Any`, `Is(type)`) | [ConventionReq.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/ConventionReq.scala) |
| `ConventionFunc` | Computes conventions of arbitrary plans (Gluten plans, Spark plans, backend overrides via `convFuncOverride`) | [ConventionFunc.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/ConventionFunc.scala) |
| `Transition` | A function `SparkPlan => SparkPlan` that wraps a node | [Transition.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/Transition.scala) |
| `TransitionGraph` | Vertices = row/batch types, edges = transitions, shortest paths via `FloydWarshallGraph` with a cost model | [TransitionGraph.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/TransitionGraph.scala) |
| `InsertTransitions` / `RemoveTransitions` | Rules that strip and re-insert transitions | [Transitions.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/Transitions.scala) |
| `VeloxBatchType` | Velox native batch convention | [VeloxBatchType.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBatchType.scala) |
| `ArrowJavaBatchType`, `ArrowNativeBatchType` | Arrow conventions | [ArrowBatchTypes.scala](../../gluten-arrow/src/main/scala/org/apache/gluten/backendsapi/arrow/ArrowBatchTypes.scala) |
| `VeloxCarrierRowType` | Rows that carry a whole batch (for zero-copy into row-based APIs like writers) | [VeloxCarrierRowType.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxCarrierRowType.scala), [BatchCarrierRow.scala](../../shims/common/src/main/scala/org/apache/gluten/execution/BatchCarrierRow.scala) |
| `ColumnarBatches` | Light/heavy batch utilities: `create`, `load`, `offload`, `retain`, `release`, `getNativeHandle` | [ColumnarBatches.java](../../gluten-arrow/src/main/java/org/apache/gluten/columnarbatch/ColumnarBatches.java) |
| `VeloxColumnarBatch` (C++) | Wraps a Velox `RowVectorPtr` | `cpp/velox/memory/VeloxColumnarBatch.h` |

---

## 3. The batch flavors

The two batch layouts, and how to convert between them.

- **Light (native) batch.** Column 0 is an `IndicatorVector` holding a handle to a C++ `VeloxColumnarBatch` (a Velox `RowVectorPtr`). Columns 1..n are `PlaceholderVector`s with no data.
- **Heavy (Java Arrow) batch.** Every column is an `ArrowWritableColumnVector` that JVM code can read.
- **Light → heavy:** `ColumnarBatches.load` (Arrow C Data export).
- **Heavy → light:** `ColumnarBatches.offload` (Arrow C Data import).

```java
private static BatchType identifyBatchType(ColumnarBatch batch) {
  if (batch.numCols() == 0) return BatchType.ZERO_COLUMN;
  if (batch.column(0) instanceof IndicatorVector) { /* others must be PlaceholderVector */ return BatchType.LIGHT; }
  /* all columns must be ArrowWritableColumnVector */ return BatchType.HEAVY;
}
```

- Light batches can't be read row-by-row from Java (`PlaceholderVector` getters throw). That's intentional: data stays native.
- `ColumnarBatches.retain/release` manage reference counts so a batch can be shared (e.g. broadcast, cache) safely.
- `ColumnarBatchInIterator.next()` calls `checkOffloaded(batch)` before handing a handle to native code.

---

## 4. Convention types & registered transitions (Velox build)

| Type | Kind | Registered transitions |
|---|---|---|
| `VanillaRowType` | row | — |
| `VanillaBatchType` | batch | from `VanillaRowType` via Spark `RowToColumnarExec`; to `VanillaRowType` via Spark `ColumnarToRowExec` |
| `VeloxBatchType` | batch | from `VanillaRowType` via **`RowToVeloxColumnarExec`**; to `VanillaRowType` via **`VeloxColumnarToRowExec`**; from `ArrowNativeBatchType` via `ArrowColumnarToVeloxColumnarExec`; to `ArrowNativeBatchType` via *empty* (a Velox batch is already a native Arrow-compatible batch) |
| `ArrowNativeBatchType` | batch | from `ArrowJavaBatchType` via `OffloadArrowDataExec`; to `ArrowJavaBatchType` via `LoadArrowDataExec` |
| `ArrowJavaBatchType` | batch | to `VanillaBatchType` via *empty* (Arrow Java vectors implement Spark's `ColumnVector` getters) |
| `VeloxCarrierRowType` | row | from `VeloxBatchType` via `VeloxColumnarToCarrierRowExec` |

The graph composes edges, e.g. `ArrowJavaBatch → VeloxBatch` = `OffloadArrowDataExec` then `ArrowColumnarToVeloxColumnarExec`.

Registration happens at startup in `VeloxListenerApi.initialize` (`ensureRegistered()` for each type).

---

## 5. How transitions are inserted

### 5.1 Lifecycle in the planner

1. `GlutenColumnarRule.postColumnarTransitions` first calls `Transitions.insert(originalPlan, outputsColumnar)` with **vanilla** batch type to normalize the incoming plan.
2. Pre-transform `RemoveTransitions` strips all `ColumnarToRowLike`, `RowToColumnarLike`, `ColumnarToColumnarLike` nodes.
3. Offload happens (some nodes become transformers with `VeloxBatchType` output).
4. Post-transform `InsertTransitions.create(outputsColumnar, VeloxBatchType)`:

```scala
override def apply(plan: SparkPlan): SparkPlan = {
  val removed = RemoveTransitions.apply(plan)
  val filled = fillWithTransitions(removed)             // per node: child convention vs. required convention
  Transitions.enforceReq(filled, convReq)               // root requirement (rows, or Velox batch if caching)
}
private def applyForNode(node: SparkPlan): SparkPlan = {
  val convReqs = convFunc.conventionReqOf(node)
  val newChildren = node.children.zip(convReqs).map { case (child, req) =>
    val from = convFunc.conventionOf(child)
    if (from.isNone) child
    else Transition.factory.findTransitionOrThrow(from, req)(Transition.notFound(node)).apply(child)
  }
  node.withNewChildren(newChildren)
}
```

5. Later, `RemoveTopmostColumnarToRow` may remove a final C2R if the consumer can take columnar output (e.g. writes).

### 5.2 Choosing a path

`TransitionGraph` uses `FloydWarshallGraph` to precompute cheapest paths between all vertices, with costs from the Gluten cost model (`costers()` from components; Velox registers `LegacyCoster` and `RoughCoster`). If no path exists, planning fails with a "no viable transition" error — usually a bug in a custom node's convention declaration.

---

## 6. The main transitions in detail

### 6.1 `VeloxColumnarToRowExec` (native → rows)

JVM side ([VeloxColumnarToRowExec.scala](../../backends-velox/src/main/scala/org/apache/gluten/execution/VeloxColumnarToRowExec.scala#L160-L215)):

```scala
private val jniWrapper = NativeColumnarToRowJniWrapper.create(runtime)
private val c2rId = jniWrapper.nativeColumnarToRowInit()
...
override def next(): InternalRow = {
  if (rowId == 0 || rowId == baseLength + info.lengths.length) {
    info = jniWrapper.nativeColumnarToRowConvert(c2rId, batchHandle, rowId)   // convert next chunk natively
  }
  val (offset, length) = (info.offsets(rowId - baseLength), info.lengths(rowId - baseLength))
  row.pointTo(null, info.memoryAddress + offset, length)                     // zero-copy UnsafeRow view
  rowId += 1
  row
}
```

Native side ([VeloxColumnarToRowConverter.cc](../../cpp/velox/operators/serializer/VeloxColumnarToRowConverter.cc)): uses Velox's `UnsafeRowFast` to serialize rows in Spark's `UnsafeRow` layout into a native buffer. The number of rows per chunk is bounded by `spark.gluten.sql.columnarToRowMemoryThreshold` (default `64MB`), so a huge batch doesn't allocate a huge row buffer.

> **Warning:** The returned `UnsafeRow` points into native memory that's reused for the next chunk. Consumers that buffer rows must `copy()` them — standard Spark semantics for `UnsafeRow`, but easy to forget in custom code.

### 6.2 `RowToVeloxColumnarExec` (rows → native)

([RowToVeloxColumnarExec.scala](../../backends-velox/src/main/scala/org/apache/gluten/execution/RowToVeloxColumnarExec.scala))
- Converts each input row to `UnsafeRow` (via `UnsafeProjection` if needed).
- Copies row bytes into an Arrow off-heap buffer (growing it as needed) until a batch is full (`spark.gluten.sql.columnar.maxBatchSize`, default 4096 rows).
- Calls native conversion (`VeloxRowToColumnarConverter`) to build a Velox `RowVector` from the row buffer and returns a light batch.

This involves a full copy of row data plus a row→column transpose — usually the most expensive transition.

### 6.3 Arrow transitions

- `LoadArrowDataExec` (native → Java Arrow) and `OffloadArrowDataExec` (Java Arrow → native) use `ColumnarBatches.load/offload` (Arrow C Data Interface). Used e.g. by Python UDFs (`ColumnarArrowEvalPythonExec`) and Arrow-based scans.
- `ArrowColumnarToVeloxColumnarExec` converts Arrow native batches into Velox vectors (Velox's Arrow bridge).

### 6.4 Carrier rows

`VeloxColumnarToCarrierRowExec` turns each batch into N−1 `PlaceholderRow`s + one `TerminalRow` wrapping the batch. A row-based API (e.g. Spark's file-format writer path used by Delta writes) can then "iterate rows" while the receiver actually gets the whole columnar batch with zero copy.

### 6.5 `VeloxResizeBatches`

Not a format transition, but a columnar-to-columnar node inserted by `AppendBatchResizeForShuffleInputAndOutput`:
- Before a `ColumnarExchange` (when the shuffle writer type requires it) to **coalesce small batches** (e.g. after a selective filter) so the shuffle writer processes fewer, larger batches.
- Optionally after shuffle read (AQE) for the same reason.

| Config | Default |
|---|---|
| `spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInput` | `true` |
| `spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleOutput` | `false` |
| `spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInputOutput.minSize` | unset → `0.25 × maxBatchSize` |
| `spark.gluten.sql.columnar.backend.velox.resizeBatches.copyRanges.enabled` | `true` |

---

## 7. End-to-end worked example: fallback in the middle

Suppose a project contains an expression Velox can't run and partial project is disabled:

```scala
spark.conf.set("spark.gluten.sql.columnar.partial.project", "false")
val f = udf((s: String) => s.reverse)
spark.read.parquet("/tmp/s")
  .where($"id" > 10)
  .select(f($"name").as("r"), $"id")
  .groupBy("r").count()
  .explain("formatted")
```

Illustrative plan:

```text
VeloxColumnarToRow
+- ^ RegularHashAggregateExecTransformer
   +- ^ InputIteratorTransformer
      +- ColumnarExchange
         +- ^ ProjectExecTransformer
            +- ^ FlushableHashAggregateExecTransformer
               +- ^ InputIteratorTransformer
                  +- RowToVeloxColumnar                 <- transition #2 (rows -> native)
                     +- Project [UDF(name) AS r, id]     <- vanilla Spark (fell back)
                        +- VeloxColumnarToRow            <- transition #1 (native -> rows)
                           +- ^ FilterExecTransformer
                              +- ^ FileSourceScanExecTransformer parquet
```

Data path per batch:

The pipeline when a UDF in the middle falls back:

1. Native scan + filter produce Velox batches.
2. `VeloxColumnarToRow` serializes them into `UnsafeRow` chunks.
3. A Spark `Project` evaluates the UDF row by row (whole-stage codegen).
4. `RowToVeloxColumnar` copies the rows back and transposes them into columns.
5. Native partial aggregation continues on Velox batches.

Steps 2 and 4 are the extra cost of the fallback.

Cost: two full data conversions and an extra stage boundary (`InputIteratorTransformer`). Fixes, in order of preference:
1. Enable partial project (default) so only the UDF column is evaluated in the JVM.
2. Replace the UDF with a built-in function Velox supports.
3. If many such islands exist, set `spark.gluten.sql.columnar.wholeStage.fallback.threshold` so the stage runs fully in vanilla Spark.

**Try it / verify**

```scala
val plan = df.queryExecution.executedPlan
plan.collect { case p if p.nodeName.contains("ColumnarToRow") || p.nodeName.contains("RowToVeloxColumnar") => p.nodeName }
```

- In the healthy case, the only transition is the final `VeloxColumnarToRow`. Each extra pair marks a fallback in the middle of a pipeline.
- In the SQL UI, `VeloxColumnarToRow` / `RowToVeloxColumnar` show `time to convert`. Compare it with the stage time to see what the fallback costs.

---

## 8. Configuration reference

| Key | Default | Effect |
|---|---|---|
| `spark.gluten.sql.columnarToRowMemoryThreshold` | `64MB` | Max native buffer per C2R chunk |
| `spark.gluten.sql.columnar.maxBatchSize` | `4096` | Rows per batch (R2C and native operators) |
| `spark.gluten.sql.columnar.backend.velox.resizeBatches.*` | see §6.5 | Batch coalescing around shuffle |
| `spark.gluten.sql.columnar.fallback.ignoreRowToColumnar` | `true` | Ignore R2C when counting transition cost for fallback policy |
| `spark.gluten.sql.columnar.wholeStage.fallback.threshold` | `-1` | Stage fallback by transitions |
| `spark.gluten.sql.columnar.partial.project` | `true` | Avoid full C2R/R2C for unsupported expressions in projects |

---

## 9. Pitfalls & FAQ

- **"Heavy batch should consist of arrow vectors"** or **"Light batch should consist of one indicator vector..."** — a custom operator mixed vector types in one batch.
- **`PlaceholderVector` getter exceptions** — some JVM code tried to read a native batch directly; it needs a C2R (or `load`) first. Usually means a plan node declared the wrong convention.
- **Rows mysteriously change** — buffering `UnsafeRow` from `VeloxColumnarToRow` without `copy()`.
- **High `VeloxColumnarToRow` time in the UI** — check the metric "time to convert" on C2R nodes; consider `RemoveTopmostColumnarToRow`-eligible sinks (e.g. writes) or reducing collected data.
- **How to spot transitions quickly** — count `VeloxColumnarToRow`/`RowToVeloxColumnar` in `df.explain()`; more than the final one at the top deserves a look.

---

## 10. Code map

| File | Role |
|---|---|
| [Convention.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/Convention.scala) | Types & registration |
| [Transitions.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/Transitions.scala) | Insert/remove rules |
| [TransitionGraph.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/TransitionGraph.scala), [FloydWarshallGraph.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/FloydWarshallGraph.scala) | Path finding |
| [VeloxBatchType.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBatchType.scala) | Velox transitions |
| [ArrowBatchTypes.scala](../../gluten-arrow/src/main/scala/org/apache/gluten/backendsapi/arrow/ArrowBatchTypes.scala) | Arrow transitions |
| [ColumnarBatches.java](../../gluten-arrow/src/main/java/org/apache/gluten/columnarbatch/ColumnarBatches.java) | Batch utilities |
| [VeloxColumnarToRowExec.scala](../../backends-velox/src/main/scala/org/apache/gluten/execution/VeloxColumnarToRowExec.scala) | C2R |
| [RowToVeloxColumnarExec.scala](../../backends-velox/src/main/scala/org/apache/gluten/execution/RowToVeloxColumnarExec.scala) | R2C |
| [VeloxColumnarToRowConverter.cc](../../cpp/velox/operators/serializer/VeloxColumnarToRowConverter.cc), [VeloxRowToColumnarConverter.cc](../../cpp/velox/operators/serializer/VeloxRowToColumnarConverter.cc) | Native converters |
| [VeloxResizeBatchesExec.scala](../../backends-velox/src/main/scala/org/apache/gluten/execution/VeloxResizeBatchesExec.scala) | Batch resizing |
| [VeloxTransitionSuite.scala](../../backends-velox/src/test/scala/org/apache/gluten/extension/columnar/transition/VeloxTransitionSuite.scala) | Tests showing expected transitions |

## 11. Related docs

- [02 · Planning](02-planning-offload-validation-fallback.md) (where `InsertTransitions` runs) · [04 · Runtime](04-native-execution-runtime-jni.md) (handles) · [08 · Shuffle](08-shuffle.md) (resize batches) · [12 · Operators](12-operators-join-agg-window-sort.md) (Python UDF, cache)
