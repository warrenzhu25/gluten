# 00 · Gluten Overview & End-to-End Query Lifecycle

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten **v1.7.0** (tag `v1.7.0`)
>
> This is the entry point of the series. Read it first; every other doc goes deeper into one step of the flow described below.

---

## TL;DR

- Gluten makes Spark SQL faster by running most of the query in a **native C++ engine** (Velox) instead of the JVM. Your SQL and results stay the same.
- Spark still parses, optimizes and schedules the query. Gluten only swaps the **physical operators** it can run natively; anything else quietly stays on Spark ("fallback").
- Data stays in a **columnar, off-heap** format between native operators and through shuffle, which is where most of the speedup comes from.
- Native memory is **counted against Spark's off-heap memory**, so Spark can still ask native operators to spill.

**Technical summary**

- **Gluten is a Spark plugin** that keeps Spark's SQL front end (parser, analyzer, Catalyst optimizer, AQE, scheduler, shuffle service) and **replaces the physical execution** of supported operators with a **native, vectorized C++ engine** (Velox by default, ClickHouse as an alternative).
- It works by **rewriting Spark's physical plan**: supported `SparkPlan` nodes become `*Transformer` nodes; unsupported nodes stay vanilla Spark ("**fallback**").
- Contiguous transformers are grouped into a **`WholeStageTransformer`** (shown as `WholeStageCodegenTransformer (N)` in EXPLAIN). Each one is serialized into **one Substrait plan**.
- On executors, the Substrait plan is sent through **JNI** to C++, converted into a **Velox plan**, and executed by a single-threaded **Velox Task** that produces **columnar batches** (`VeloxColumnarBatch`).
- Data moves between stages through Gluten's **columnar shuffle** (`ColumnarExchange`) and enters/leaves the JVM row world through **transitions** (`VeloxColumnarToRow`, `RowToVeloxColumnar`).
- Native memory is **accounted to Spark's off-heap memory manager**, and native operators can **spill** when Spark asks for memory back.
- The design goal is **transparent acceleration**: same SQL, same results, no user code change, with automatic fallback for anything not supported.

---

## 1. Why Gluten exists

### The problem

Spark's execution engine is JVM-based and row-oriented at its core (whole-stage Java codegen over `UnsafeRow`). For analytical SQL this leaves performance on the table:

| Limitation of vanilla Spark execution | Effect |
|---|---|
| Row-at-a-time processing (even with codegen) | Poor CPU cache usage, little SIMD |
| JVM objects, GC, JIT warm-up | Overhead & unpredictable latency |
| Columnar file formats (Parquet/ORC) are decoded into rows | Wasted conversion work |
| Hard to reuse state-of-the-art C++ engines | Every optimization must be re-done in Java |

Meanwhile, native vectorized engines (Velox from Meta, ClickHouse) already implement fast columnar operators, readers, and functions in C++.

### The idea

> Keep everything Spark is good at (SQL semantics, optimizer, scheduling, fault tolerance, ecosystem) and **offload only the CPU-heavy physical execution** to a native engine — **at operator granularity, with safe fallback**.

That gives:

- **Drop-in adoption** — enabled with `spark.plugins=org.apache.gluten.GlutenPlugin`; no SQL or DataFrame change.
- **Correctness by default** — anything not proven supported (by Scala-side checks *and* native validation) stays on Spark.
- **Backend neutrality** — the plan is handed over as **Substrait** (an open IR), so the same front end can drive Velox or ClickHouse.

### What Gluten is *not*

- Not a new SQL engine or optimizer — logical optimization is still Catalyst (Gluten adds a few rules).
- Not a scheduler — Spark still creates stages/tasks; Gluten only changes what a task runs.
- Not a shuffle service — it plugs a columnar shuffle *writer/reader* into Spark's shuffle framework (or Celeborn/Uniffle).

---

## 2. Key concepts & terminology

| Term | What it is | Where |
|---|---|---|
| **Backend** | A native engine integration (Velox, ClickHouse). Implements a set of `*Api` traits. | `backends-velox`, `backends-clickhouse` |
| **Component** | A pluggable unit (backend or extension like Iceberg/Delta) discovered at startup; contributes rules and lifecycle hooks. | [Component.scala](../../gluten-core/src/main/scala/org/apache/gluten/component/Component.scala) |
| **Transformer** | Gluten's replacement for a Spark physical node, e.g. `FilterExecTransformer`. Knows how to validate itself and produce a Substrait `RelNode`. | `gluten-substrait/.../execution` |
| **Offload** | Replacing a Spark node with a transformer. | `OffloadSingleNode` rules |
| **Validation** | Deciding whether a node can be offloaded (config checks + native validation). | `Validators`, `ValidatablePlan.doValidate` |
| **Fallback** | Keeping a node (or a whole stage/query) on vanilla Spark. | `FallbackTags`, `ExpandFallbackPolicy` |
| **WholeStageTransformer** | A group of adjacent transformers compiled into one Substrait plan / one native task. Shown as `WholeStageCodegenTransformer (id)`. | [WholeStageTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/WholeStageTransformer.scala) |
| **Substrait** | Cross-language relational-algebra IR (protobuf). Gluten uses a modified copy. | `gluten-substrait/src/main/resources/substrait/proto` |
| **Split / SplitInfo** | Description of the input files (paths, offsets, partition values) given to a native scan. | `LocalFilesNode` |
| **Runtime** | Per-task native context (plan, memory manager, Velox config). | `Runtime`, `VeloxRuntime` |
| **Convention** | Gluten's type system for "what data format a plan node produces/needs" (rows, Velox batches, Arrow batches). | `extension/columnar/transition` |
| **Transition** | A conversion node between conventions, e.g. `VeloxColumnarToRow`. | `Transitions.scala` |
| **Columnar shuffle** | Native shuffle writer/reader for `ColumnarBatch` data. | `ColumnarShuffleManager`, `cpp/velox/shuffle` |
| **Memory target** | Gluten's tree of memory accounting nodes connected to Spark's `TaskMemoryManager`. | `gluten-core/.../memtarget` |
| **Light / heavy batch** | A *light* batch only holds a handle to data in C++; a *heavy* batch holds Arrow data readable from Java. See [05](05-columnar-batches-conventions-transitions.md). | `ColumnarBatches` |
| **C2R / R2C** | Columnar-to-row / row-to-columnar transitions (`VeloxColumnarToRow`, `RowToVeloxColumnar`). | `Transitions.scala` |
| **SHJ / BHJ / SMJ / BNLJ** | Join strategies: shuffled hash join, broadcast hash join, sort-merge join, broadcast nested-loop join. See [12](12-operators-join-agg-window-sort.md). | `*JoinExecTransformer` |
| **AQE** | Spark Adaptive Query Execution (re-plans stages at runtime). Gluten works with it. | Spark |
| **Remote shuffle (RSS)** | A remote shuffle service such as Apache Celeborn or Uniffle. Not to be confused with *process RSS* (resident memory), as in doc 06. | `gluten-celeborn`, `gluten-uniffle` |
| **COW / MOR** | Copy-on-write (rewrite files on update) vs. merge-on-read (write delete files, merge at read) table layouts in lake formats. | [16](16-delta-and-iceberg-integration.md) |
| **DV** | Delta deletion vector: a bitmap of deleted row positions per data file. | [16](16-delta-and-iceberg-integration.md) |

---

## 3. Architecture at a glance

How the pieces fit together, in the order a query passes through them.

**On the driver (JVM):**

1. You submit SQL or a DataFrame.
2. Catalyst analyzes and optimizes it, then Spark builds a physical plan.
3. `GlutenColumnarRule` (injected through `SparkSessionExtensions`) rewrites that plan: it offloads, validates and falls back node by node.
4. The result is a plan made of `WholeStageTransformer` stages, transitions and `ColumnarExchange` (shuffle) nodes.
5. Each native stage is serialized into Substrait plan bytes, which travel inside the RDD partitions sent to executors.

**On each executor:**

1. A Spark task runs `GlutenWholeStageColumnarRDD.compute`.
2. It calls into C++ through JNI (`PlanEvaluatorJniWrapper`).
3. `VeloxRuntime` turns the Substrait plan into a Velox plan.
4. A Velox task runs it serially (one driver) and produces `VeloxColumnarBatch`es.
5. Batches go either to the columnar shuffle writer, or through `VeloxColumnarToRow` if a Spark operator needs rows.

Throughout, Velox reserves memory from Spark's `TaskMemoryManager`, and Spark can ask it to spill.

### Layered view

| Layer | Language | Modules | Responsibility |
|---|---|---|---|
| Plugin & lifecycle | Scala | `gluten-core` | `GlutenPlugin`, component discovery, rule injection, task resources, memory targets |
| Planning & Substrait | Scala/Java | `gluten-substrait` | Transformers, validators, fallback, Substrait builders, whole-stage RDDs, shuffle exchange, configs |
| Arrow/JNI glue | Scala/Java | `gluten-arrow` | Runtime, native memory manager, JNI wrappers, Arrow batch helpers |
| Velox backend (JVM side) | Scala | `backends-velox` | `VeloxBackend` + Api impls, Velox-specific rules/transformers, shuffle writer, configs |
| Native core | C++ | `cpp/core` | JNI entry points, generic runtime/memory/shuffle infrastructure |
| Native Velox | C++ | `cpp/velox` | Substrait→Velox conversion, `WholeStageResultIterator`, Velox shuffle, memory manager, UDFs |
| Engine | C++ | Velox (built from `ep/build-velox`) | Operators, expressions, readers/writers, memory pools, spilling |

---

## 4. Repository map (v1.7.0)

| Path | What lives there |
|---|---|
| `gluten-core/` | Plugin entry (`GlutenPlugin.scala`), `component/` (discovery & DAG), `extension/` (session extensions, injectors, heuristic planner, transitions, validators), memory targets (`memory/memtarget`), `task/TaskResources`. |
| `gluten-substrait/` | `GlutenConfig`, `backendsapi/` (the `*Api` traits), `execution/` (all base transformers, `WholeStageTransformer`, scan transformers), `expression/` (expression converters), `substrait/` (Java builders for Substrait nodes), `extension/` (offload/rewrite rules), shuffle exchange & dependency, soft affinity. |
| `gluten-arrow/` | Arrow memory/batch utilities, `runtime/Runtime(s)`, `NativeMemoryManager`, `NativePlanEvaluator`, `ColumnarBatchOutIterator`, Arrow conventions. |
| `backends-velox/` | `backendsapi/velox/*` (VeloxBackend, VeloxRuleApi, VeloxSparkPlanExecApi, VeloxIteratorApi, VeloxListenerApi, ...), Velox-only transformers & rules, `ColumnarShuffleWriter`, table cache, UDF support; `src-iceberg/`, `src-delta*/`, `src-hudi/`, `src-paimon/` for data-lake components. |
| `cpp/core/` | `jni/JniWrapper.cc` (most JNI entry points), `compute/` (Runtime, ResultIterator), `memory/`, `shuffle/` (partition writers, payloads, spillers), `operators/`, `threads/`. |
| `cpp/velox/` | `compute/` (VeloxBackend, VeloxRuntime, WholeStageResultIterator, VeloxPlanConverter), `substrait/` (SubstraitToVeloxPlan, validator, expressions), `shuffle/` (hash/sort/rss-sort writers, reader), `memory/` (VeloxMemoryManager, ListenableArbitrator), `operators/` (C2R, R2C, serde, writers), `udf/`, `cudf/` (GPU), `filesystem/`, `tests/`, `benchmarks/`. |
| `shims/` | Spark-version adapters (`spark33` … `spark41`) behind `SparkShimLoader`. |
| `gluten-ut/` | Reuse of Spark's own unit tests against Gluten (`VeloxTestSettings` enable/exclude lists). |
| `gluten-iceberg/`, `gluten-delta/`, `gluten-hudi/`, `gluten-paimon/` | Shared (backend-neutral) data-lake integration code. |
| `gluten-celeborn/`, `gluten-uniffle/` | Remote shuffle service integrations. |
| `gluten-kafka/` | Kafka source support. |
| `gluten-flink/` | Flink frontend (experimental). |
| `gluten-ui/` | "Gluten SQL" tab in Spark UI (build info, fallback summary). |
| `backends-clickhouse/`, `cpp-ch/` | ClickHouse backend (JVM + native). |
| `ep/` | External projects build scripts (Velox fetch/patch/build). |
| `dev/` | Build scripts (`builddeps-veloxbe.sh`, `buildbundle-veloxbe.sh`), formatters, checks. |
| `tools/` | `gluten-it` (TPC-H/DS integration test harness), workload scripts. |
| `docs/` | Official docs: configuration, Velox limitations, function support, developer guides. |

---

## 5. End-to-end worked example: the life of a query

We follow **TPC-H Q1** (scan → filter → aggregate → shuffle → final aggregate → sort) on a Parquet table.

```sql
SELECT l_returnflag, l_linestatus,
       sum(l_quantity) AS sum_qty,
       sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       avg(l_discount) AS avg_disc, count(*) AS count_order
FROM lineitem
WHERE l_shipdate <= date '1998-12-01' - interval 90 day
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

### Step 0 — Configure Spark

Minimum configuration (from [docs/get-started/Velox.md](../../docs/get-started/Velox.md)):

```bash
spark-shell \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.driver.extraClassPath=${GLUTEN_JAR} \
  --conf spark.executor.extraClassPath=${GLUTEN_JAR} \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=20g \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager
```

> **Important:** Off-heap must be enabled (unless dynamic off-heap sizing is used): `GlutenDriverPlugin.checkOffHeapSettings` throws if `spark.memory.offHeap.enabled` is false or the size is < 1MB. Native memory is accounted against this budget. See [Memory management](06-memory-management.md).

### Step 1 — Startup (driver & executors)

1. Spark instantiates `GlutenPlugin` → `GlutenDriverPlugin.init` ([GlutenPlugin.scala](../../gluten-core/src/main/scala/org/apache/gluten/GlutenPlugin.scala)):
   - appends `GlutenSessionExtensions` to `spark.sql.extensions`,
   - computes per-task off-heap budgets (`spark.gluten.memory.offHeap.size.in.bytes`, `...task.offHeap.size.in.bytes`),
   - discovers components (`META-INF/gluten-components/*`), sorts them by dependency, calls `onDriverStart`.
2. `VeloxBackend` → `VeloxListenerApi.onDriverStart` sets memory overhead defaults, loads `libgluten.so` + `libvelox.so`, and calls `NativeBackendInitializer.initialize(...)`, which runs `VeloxBackend::init` in C++ (glog, memory manager, filesystems, Parquet/ORC readers, function registry, caches).
3. Each executor does the same through `GlutenExecutorPlugin.init` → `onExecutorStart`.

Details: [01 · Plugin bootstrap](01-plugin-bootstrap-components-backends.md).

### Step 2 — Catalyst produces a vanilla physical plan

Unchanged Spark: parse → analyze → optimize (plus a few Gluten optimizer rules injected by `VeloxRuleApi.injectSpark`) → physical planning. The "Initial Plan" for Q1:

```text
Sort
+- Exchange
   +- HashAggregate (final)
      +- Exchange
         +- HashAggregate (partial)
            +- Project
               +- Filter
                  +- Scan parquet
```

### Step 3 — Gluten rewrites the physical plan

Spark's `ApplyColumnarRulesAndInsertTransitions` calls Gluten's `GlutenColumnarRule.postColumnarTransitions`, which runs the **heuristic applier** in four phases:

| Phase | What happens (selected rules) |
|---|---|
| **transform** | pre-transform rules (`RemoveTransitions`, `FallbackOnANSIMode`, ...) → **HeuristicTransform**: *rewrite* (`PullOutPreProject`, `RewriteJoin`, ...), *validate* (`AddFallbackTags` with config checks + **native validation**), *offload* (`OffloadOthers`, `OffloadExchange`, `OffloadJoin`) → post-transform rules (`PushDownFilterToScan`, `CollapseProjectExecTransformer`, `FlushableHashAggregateRule`, `InsertTransitions`) |
| **fallback** | `ExpandFallbackPolicy` may decide to fall back an entire stage/query if too many transitions would be needed |
| **post** | `RemoveTopmostColumnarToRow`, **`ColumnarCollapseTransformStages`** (creates `WholeStageTransformer`s), `GenerateTransformStageId` |
| **final** | `GlutenFallbackReporter` (logs + UI), `RemoveFallbackTagRule` |

Result for Q1 (real approved plan from the repo, [tpch-approved-plan/v1/spark35/1.txt](../../backends-velox/src/test/resources/tpch-approved-plan/v1/spark35/1.txt#L1-L19)):

```text
AdaptiveSparkPlan
+- == Final Plan ==
   VeloxColumnarToRow (21)
   +- ^ SortExecTransformer (19)
      +- ^ InputIteratorTransformer (18)
         +- ShuffleQueryStage (16)
            +- ColumnarExchange (15)
               +- VeloxResizeBatches (14)
                  +- ^ RegularHashAggregateExecTransformer (12)
                     +- ^ InputIteratorTransformer (11)
                        +- ShuffleQueryStage (9)
                           +- ColumnarExchange (8)
                              +- VeloxResizeBatches (7)
                                 +- ^ ProjectExecTransformer (5)
                                    +- ^ FlushableHashAggregateExecTransformer (4)
                                       +- ^ ProjectExecTransformer (3)
                                          +- ^ FilterExecTransformer (2)
                                             +- ^ FileSourceScanExecTransformer parquet (1)
```

How to read it:

- **`^`** marks nodes that run *inside* a native whole stage (a `WholeStageCodegenTransformer`). Here there are 3 native stages: (1)–(5), (11)–(12), (18)–(19).
- **`InputIteratorTransformer`** is where a native stage reads batches produced outside it (here, from a shuffle).
- **`ColumnarExchange`** is Gluten's columnar shuffle; **`VeloxResizeBatches`** coalesces/splits batches around shuffle.
- **`FlushableHashAggregateExecTransformer`** is the partial aggregation that may flush early under memory pressure.
- **`VeloxColumnarToRow`** at the top converts to rows only because the driver collects rows.

Details: [02 · Planning](02-planning-offload-validation-fallback.md).

### Step 4 — Substrait plan generation (driver)

When Spark calls `executeColumnar()` on a `WholeStageTransformer`, `doWholeStageTransform()` walks its children recursively (`transform(context)` → each transformer's `doTransform`) and builds one Substrait `Plan`:

```text
ReadRel(LocalFiles: lineitem)  ← FileSourceScanExecTransformer
  → FilterRel(l_shipdate <= ...) ← FilterExecTransformer
    → ProjectRel(...)            ← ProjectExecTransformer
      → AggregateRel(partial)    ← FlushableHashAggregateExecTransformer
        → ProjectRel(...)
```

For scan stages it also builds one **SplitInfo** per partition (file paths, start/length, partition values) via `VeloxIteratorApi.genSplitInfo`. The plan is serialized **once** and placed, with the split infos, into `GlutenPartition`s of a `GlutenWholeStageColumnarRDD`.

Details: [03 · Substrait](03-substrait-and-native-plan-conversion.md), [09 · Scan](09-scan-and-data-sources.md).

### Step 5 — Native execution (executor)

For each task:

What happens inside one task, step by step.

1. `GlutenWholeStageColumnarRDD.compute` receives its partition: plan bytes plus split infos.
2. `VeloxIteratorApi.genFirstStageIterator` creates a `NativePlanEvaluator` and calls `createKernelWithBatchIterator(plan, splits, inputIters, spillDir)`.
3. The evaluator calls JNI `nativeCreateKernelWithIterator`.
4. In C++, `VeloxRuntime` parses the plan and splits, and `VeloxPlanConverter.toVeloxPlan` builds Velox plan nodes.
5. It creates a `WholeStageResultIterator`, which opens a Velox `TaskCursor` (serial execution, one driver).
6. Then, for each batch:
   - the JVM calls `nativeHasNext` / `nativeNext`;
   - the iterator calls `cursor.moveNext()` and Velox returns a `RowVector`;
   - the JVM receives a handle to a `VeloxColumnarBatch`. The data itself stays in C++.
7. At the end, the JVM closes the iterator and fetches native metrics.

Key facts:
- **One Velox Task per Spark task**, executed **serially on the Spark task thread** (`params.serialExecution = true`, `maxDrivers = 1`). Parallelism still comes from Spark tasks.
- Batches never get copied into the JVM: the JVM holds an opaque **handle** (`long`) to a native `ColumnarBatch` registered in an `ObjectStore`.
- Exceptions in C++ become `GlutenException` in Java via `JNI_METHOD_START/END`.

Details: [04 · Native runtime](04-native-execution-runtime-jni.md).

### Step 6 — Shuffle

The map stage's last native operator output feeds `ColumnarShuffleWriter` (JVM) → native `VeloxHashShuffleWriter` / `VeloxSortShuffleWriter` that partitions batches (hash on grouping keys) and writes compressed data + index files through Spark's shuffle framework. The reduce side reads blocks and deserializes them back into Velox batches, which enter the next native stage via `InputIteratorTransformer`.

Details: [08 · Shuffle](08-shuffle.md).

### Step 7 — Memory & spill while running

Every Velox memory pool growth is reported through a `ReservationListener` into Gluten's memory-target tree, which borrows from Spark's `TaskMemoryManager`. If Spark can't grant memory, it calls back into Gluten's spillers: first **shrink** free native capacity, then **spill** Velox operators (hash agg, join, sort, window) or the shuffle writer to disk.

Details: [06 · Memory](06-memory-management.md), [07 · Spill](07-spill.md).

### Step 8 — Results

The final stage's batches go through `VeloxColumnarToRow` (native conversion to `UnsafeRow`) because `collect()` needs rows. Metrics from each Velox operator are mapped back onto the corresponding transformer's `SQLMetric`s, so the Spark SQL UI shows native timings, rows, memory, and spill bytes.

Details: [05 · Transitions](05-columnar-batches-conventions-transitions.md), [13 · Metrics & troubleshooting](13-metrics-ui-config-troubleshooting.md).

---

## 6. What runs where

| Activity | Driver | Executor (JVM) | Executor (native) |
|---|---|---|---|
| Rule injection / plan rewrite | ✅ | | |
| Native validation (Substrait → Velox validator) | ✅ (via JNI on driver) | | |
| Substrait plan + split generation | ✅ | | |
| Task iteration, Spark metrics | | ✅ | |
| Plan conversion & execution | | | ✅ |
| Memory accounting | | ✅ (memory targets) | ✅ (pools, arbitrator) |
| Shuffle write/read | | ✅ (Spark framework) | ✅ (partitioning, serialization, compression) |

> **Note:** Native libraries are loaded on the **driver too**, because native validation runs there during planning.

---

## 7. Fallback in one paragraph

If any check fails for a node — a config disables it, the backend doesn't support it, an expression has no Velox mapping, a type isn't supported, or the native validator rejects the Substrait — the node is tagged and stays a vanilla Spark operator. Transitions (`VeloxColumnarToRow` / `RowToVeloxColumnar`) are inserted around it. If too many transitions would be needed, `ExpandFallbackPolicy` can fall back the whole stage or query (thresholds disabled by default, `-1`). Fallback reasons are logged (`Validation failed for plan: ...`) and shown in the **Gluten SQL** UI tab. Use `df.fallbackSummary()` (`import org.apache.spark.sql.execution.GlutenImplicits._`) to inspect them programmatically.

---

## 8. Quick hands-on

```scala
// spark-shell with Gluten enabled
spark.range(0, 1000000).selectExpr("id % 10 as k", "id as v").write.mode("overwrite").parquet("/tmp/t")
val df = spark.read.parquet("/tmp/t").groupBy("k").sum("v")
df.explain("formatted")        // look for ^ nodes and WholeStageCodegenTransformer
df.collect()

import org.apache.spark.sql.execution.GlutenImplicits._
df.fallbackSummary()          // numGlutenNodes, numFallbackNodes, reasons

spark.conf.set("spark.gluten.sql.injectNativePlanStringToExplain", "true")
df.explain("formatted")        // also prints the Velox plan per stage
```

---

## 9. Glossary of EXPLAIN node names

| Node | Meaning |
|---|---|
| `WholeStageCodegenTransformer (N)` | Native stage N (one Substrait plan / Velox task) |
| `^ XxxExecTransformer` | Offloaded operator inside a native stage |
| `InputIteratorTransformer` + `InputAdapter` | Stage input coming from outside (shuffle, broadcast, fallback operator) |
| `ColumnarExchange` | Native columnar shuffle |
| `ColumnarBroadcastExchange` | Broadcast of columnar data |
| `VeloxResizeBatches` | Batch coalescing/splitting around shuffle |
| `VeloxColumnarToRow` / `RowToVeloxColumnar` | Native transitions |
| `FlushableHashAggregateExecTransformer` | Partial agg that can flush under memory pressure |
| `RegularHashAggregateExecTransformer` | Normal (final/complete) hash aggregation |

---

## 10. Reading guide for this series

| # | Doc | Read when you want to understand… |
|---|---|---|
| 00 | **This doc** | the big picture |
| 01 | [Plugin bootstrap, components & backends](01-plugin-bootstrap-components-backends.md) | how Gluten starts and plugs into Spark |
| 02 | [Planning: offload, validation & fallback](02-planning-offload-validation-fallback.md) | how plans are rewritten and why nodes fall back |
| 03 | [Substrait & native plan conversion](03-substrait-and-native-plan-conversion.md) | the JVM→C++ plan contract |
| 04 | [Native execution runtime & JNI](04-native-execution-runtime-jni.md) | how a task actually runs |
| 05 | [Columnar batches, conventions & transitions](05-columnar-batches-conventions-transitions.md) | data formats & conversions |
| 06 | [Memory management](06-memory-management.md) | native memory accounting |
| 07 | [Spill](07-spill.md) | what happens under memory pressure |
| 08 | [Columnar shuffle](08-shuffle.md) | shuffle writers, readers, trade-offs |
| 09 | [Scan & data sources](09-scan-and-data-sources.md) | reading files and lake formats |
| 10 | [Native write path](10-native-write.md) | INSERT/CTAS offload |
| 11 | [Expressions, functions & UDFs](11-expressions-functions-udf.md) | expression mapping, adding functions |
| 12 | [Operators](12-operators-join-agg-window-sort.md) | joins, aggregation, window, sort |
| 13 | [Metrics, UI, config & troubleshooting](13-metrics-ui-config-troubleshooting.md) | debugging in practice |
| 14 | [Build, test & dev workflow](14-build-test-dev-workflow.md) | building and contributing |
| 15 | [Other backends & frontends](15-other-backends-and-frontends.md) | ClickHouse, GPU, Flink |
| 16 | [Delta Lake & Iceberg integration](16-delta-and-iceberg-integration.md) | lakehouse table formats |

### Suggested first week

| Day | Goal |
|---|---|
| 1 | Build the bundle jar (doc 14), run Q1 in `spark-shell`, read the EXPLAIN with this doc. |
| 2 | Read doc 01–02; set breakpoints in `GlutenColumnarRule` and `HeuristicTransform`; force a fallback and read the reason. |
| 3 | Read doc 03–04; dump a Substrait plan (`substraitPlanJson`) and the Velox plan (`injectNativePlanStringToExplain`). |
| 4 | Read doc 06–08; run a shuffle-heavy query, look at shuffle/spill metrics in the UI. |
| 5 | Read doc 11; add a trivial function mapping in a sandbox branch and write a test (doc 14). |

---

## 11. Code map (start here)

| File | Why it matters |
|---|---|
| [GlutenPlugin.scala](../../gluten-core/src/main/scala/org/apache/gluten/GlutenPlugin.scala) | Entry point |
| [GlutenSessionExtensions.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/GlutenSessionExtensions.scala) | Rule injection |
| [GlutenColumnarRule.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/GlutenColumnarRule.scala) | Hook into Spark's columnar rules |
| [VeloxRuleApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxRuleApi.scala) | The full ordered rule list for Velox |
| [WholeStageTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/WholeStageTransformer.scala) | Native stage, Substrait generation, RDD creation |
| [VeloxIteratorApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxIteratorApi.scala) | Split info and native iterator creation |
| [JniWrapper.cc](../../cpp/core/jni/JniWrapper.cc) | JNI entry points |
| [VeloxRuntime.cc](../../cpp/velox/compute/VeloxRuntime.cc) | Per-task native runtime |
| [WholeStageResultIterator.cc](../../cpp/velox/compute/WholeStageResultIterator.cc) | Velox task execution |
| [SubstraitToVeloxPlan.cc](../../cpp/velox/substrait/SubstraitToVeloxPlan.cc) | Plan conversion |

## 12. Further reading in the repo

- [docs/developers/NewToGluten.md](../../docs/developers/NewToGluten.md) — environment setup & debugging tips
- [docs/get-started/Velox.md](../../docs/get-started/Velox.md) — build & run with Velox
- [docs/Configuration.md](../../docs/Configuration.md), [docs/velox-configuration.md](../../docs/velox-configuration.md)
- [docs/velox-backend-limitations.md](../../docs/velox-backend-limitations.md) — known semantic differences
