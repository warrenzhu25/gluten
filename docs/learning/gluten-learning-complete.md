# Learning Apache Gluten (Velox backend) — Complete Guide

> All docs of the learning series merged into one file. Based on public Apache Gluten **v1.7.0**.

## Table of contents

- [00 · Gluten Overview & End-to-End Query Lifecycle](#doc-00)
- [01 · Plugin Bootstrap, Components & Backends](#doc-01)
- [02 · Query Planning: Offload, Validation & Fallback](#doc-02)
- [03 · Substrait Plan Generation & Conversion to Velox Plans](#doc-03)
- [04 · Native Execution Runtime & JNI](#doc-04)
- [05 · Columnar Batches, Conventions & Transitions](#doc-05)
- [06 · Memory Management](#doc-06)
- [07 · Spill](#doc-07)
- [08 · Columnar Shuffle](#doc-08)
- [09 · Scan & Data Sources](#doc-09)
- [10 · Native Write Path](#doc-10)
- [11 · Expressions, Functions & UDFs](#doc-11)
- [12 · Operators: Joins, Aggregation, Broadcast, Window, Sort & Others](#doc-12)
- [13 · Metrics, UI, Configuration & Troubleshooting](#doc-13)
- [14 · Build, Test & Development Workflow](#doc-14)
- [15 · Other Backends & Frontends: ClickHouse, GPU (cuDF), Flink](#doc-15)
- [16 · Delta Lake & Iceberg Integration](#doc-16)

---

<a id="doc-00"></a>

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
| **Light / heavy batch** | A *light* batch only holds a handle to data in C++; a *heavy* batch holds Arrow data readable from Java. See [05](#doc-05). | `ColumnarBatches` |
| **C2R / R2C** | Columnar-to-row / row-to-columnar transitions (`VeloxColumnarToRow`, `RowToVeloxColumnar`). | `Transitions.scala` |
| **SHJ / BHJ / SMJ / BNLJ** | Join strategies: shuffled hash join, broadcast hash join, sort-merge join, broadcast nested-loop join. See [12](#doc-12). | `*JoinExecTransformer` |
| **AQE** | Spark Adaptive Query Execution (re-plans stages at runtime). Gluten works with it. | Spark |
| **Remote shuffle (RSS)** | A remote shuffle service such as Apache Celeborn or Uniffle. Not to be confused with *process RSS* (resident memory), as in doc 06. | `gluten-celeborn`, `gluten-uniffle` |
| **COW / MOR** | Copy-on-write (rewrite files on update) vs. merge-on-read (write delete files, merge at read) table layouts in lake formats. | [16](#doc-16) |
| **DV** | Delta deletion vector: a bitmap of deleted row positions per data file. | [16](#doc-16) |

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

- **Plugin & lifecycle** (Scala, `gluten-core`): `GlutenPlugin`, component discovery, rule injection, task resources, memory targets.
- **Planning & Substrait** (Scala/Java, `gluten-substrait`): Transformers, validators, fallback, Substrait builders, whole-stage RDDs, shuffle exchange, configs.
- **Arrow/JNI glue** (Scala/Java, `gluten-arrow`): Runtime, native memory manager, JNI wrappers, Arrow batch helpers.
- **Velox backend (JVM side)** (Scala, `backends-velox`): `VeloxBackend` + Api impls, Velox-specific rules/transformers, shuffle writer, configs.
- **Native core** (C++, `cpp/core`): JNI entry points, generic runtime/memory/shuffle infrastructure.
- **Native Velox** (C++, `cpp/velox`): Substrait→Velox conversion, `WholeStageResultIterator`, Velox shuffle, memory manager, UDFs.
- **Engine** (C++, Velox, built from `ep/build-velox`): Operators, expressions, readers/writers, memory pools, spilling.

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

> **Important:** Off-heap must be enabled (unless dynamic off-heap sizing is used): `GlutenDriverPlugin.checkOffHeapSettings` throws if `spark.memory.offHeap.enabled` is false or the size is < 1MB. Native memory is accounted against this budget. See [Memory management](#doc-06).

### Step 1 — Startup (driver & executors)

1. Spark instantiates `GlutenPlugin` → `GlutenDriverPlugin.init` ([GlutenPlugin.scala](../../gluten-core/src/main/scala/org/apache/gluten/GlutenPlugin.scala)):
   - appends `GlutenSessionExtensions` to `spark.sql.extensions`,
   - computes per-task off-heap budgets (`spark.gluten.memory.offHeap.size.in.bytes`, `...task.offHeap.size.in.bytes`),
   - discovers components (`META-INF/gluten-components/*`), sorts them by dependency, calls `onDriverStart`.
2. `VeloxBackend` → `VeloxListenerApi.onDriverStart` sets memory overhead defaults, loads `libgluten.so` + `libvelox.so`, and calls `NativeBackendInitializer.initialize(...)`, which runs `VeloxBackend::init` in C++ (glog, memory manager, filesystems, Parquet/ORC readers, function registry, caches).
3. Each executor does the same through `GlutenExecutorPlugin.init` → `onExecutorStart`.

Details: [01 · Plugin bootstrap](#doc-01).

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
                                    +- ^ FlushableHashAggregateExecTransformer
                                             (4)
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

Details: [02 · Planning](#doc-02).

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

Details: [03 · Substrait](#doc-03), [09 · Scan](#doc-09).

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

Details: [04 · Native runtime](#doc-04).

### Step 6 — Shuffle

The map stage's last native operator output feeds `ColumnarShuffleWriter` (JVM) → native `VeloxHashShuffleWriter` / `VeloxSortShuffleWriter` that partitions batches (hash on grouping keys) and writes compressed data + index files through Spark's shuffle framework. The reduce side reads blocks and deserializes them back into Velox batches, which enter the next native stage via `InputIteratorTransformer`.

Details: [08 · Shuffle](#doc-08).

### Step 7 — Memory & spill while running

Every Velox memory pool growth is reported through a `ReservationListener` into Gluten's memory-target tree, which borrows from Spark's `TaskMemoryManager`. If Spark can't grant memory, it calls back into Gluten's spillers: first **shrink** free native capacity, then **spill** Velox operators (hash agg, join, sort, window) or the shuffle writer to disk.

Details: [06 · Memory](#doc-06), [07 · Spill](#doc-07).

### Step 8 — Results

The final stage's batches go through `VeloxColumnarToRow` (native conversion to `UnsafeRow`) because `collect()` needs rows. Metrics from each Velox operator are mapped back onto the corresponding transformer's `SQLMetric`s, so the Spark SQL UI shows native timings, rows, memory, and spill bytes.

Details: [05 · Transitions](#doc-05), [13 · Metrics & troubleshooting](#doc-13).

---

## 6. What runs where

- **Rule injection / plan rewrite**: Driver.
- **Native validation (Substrait → Velox validator)**: Driver (via JNI on driver).
- **Substrait plan + split generation**: Driver.
- **Task iteration, Spark metrics**: Executor (JVM).
- **Plan conversion & execution**: Executor (native).
- **Memory accounting**: Executor (JVM) (memory targets), Executor (native) (pools, arbitrator).
- **Shuffle write/read**: Executor (JVM) (Spark framework), Executor (native) (partitioning, serialization, compression).

> **Note:** Native libraries are loaded on the **driver too**, because native validation runs there during planning.

---

## 7. Fallback in one paragraph

If any check fails for a node — a config disables it, the backend doesn't support it, an expression has no Velox mapping, a type isn't supported, or the native validator rejects the Substrait — the node is tagged and stays a vanilla Spark operator. Transitions (`VeloxColumnarToRow` / `RowToVeloxColumnar`) are inserted around it. If too many transitions would be needed, `ExpandFallbackPolicy` can fall back the whole stage or query (thresholds disabled by default, `-1`). Fallback reasons are logged (`Validation failed for plan: ...`) and shown in the **Gluten SQL** UI tab. Use `df.fallbackSummary()` (`import org.apache.spark.sql.execution.GlutenImplicits._`) to inspect them programmatically.

---

## 8. Quick hands-on

```scala
// spark-shell with Gluten enabled
spark.range(0, 1000000)
  .selectExpr("id % 10 as k", "id as v")
  .write.mode("overwrite").parquet("/tmp/t")
val df = spark.read.parquet("/tmp/t").groupBy("k").sum("v")
// look for ^ nodes and WholeStageCodegenTransformer
df.explain("formatted")
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
| 01 | [Plugin bootstrap, components & backends](#doc-01) | how Gluten starts and plugs into Spark |
| 02 | [Planning: offload, validation & fallback](#doc-02) | how plans are rewritten and why nodes fall back |
| 03 | [Substrait & native plan conversion](#doc-03) | the JVM→C++ plan contract |
| 04 | [Native execution runtime & JNI](#doc-04) | how a task actually runs |
| 05 | [Columnar batches, conventions & transitions](#doc-05) | data formats & conversions |
| 06 | [Memory management](#doc-06) | native memory accounting |
| 07 | [Spill](#doc-07) | what happens under memory pressure |
| 08 | [Columnar shuffle](#doc-08) | shuffle writers, readers, trade-offs |
| 09 | [Scan & data sources](#doc-09) | reading files and lake formats |
| 10 | [Native write path](#doc-10) | INSERT/CTAS offload |
| 11 | [Expressions, functions & UDFs](#doc-11) | expression mapping, adding functions |
| 12 | [Operators](#doc-12) | joins, aggregation, window, sort |
| 13 | [Metrics, UI, config & troubleshooting](#doc-13) | debugging in practice |
| 14 | [Build, test & dev workflow](#doc-14) | building and contributing |
| 15 | [Other backends & frontends](#doc-15) | ClickHouse, GPU, Flink |
| 16 | [Delta Lake & Iceberg integration](#doc-16) | lakehouse table formats |

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

---

<a id="doc-01"></a>

# 01 · Plugin Bootstrap, Components & Backends

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [00 · Overview](#doc-00) · Next: [02 · Planning](#doc-02)

---

## TL;DR

- You turn Gluten on with one setting: `spark.plugins=org.apache.gluten.GlutenPlugin`.
- At startup the plugin finds every **component** on the classpath (the Velox backend, plus optional extras such as Iceberg or Delta), orders them by dependency, and lets each one register its planner rules.
- The backend loads the native libraries **once per JVM**; each Spark task then gets its own lightweight native **runtime**, cleaned up automatically when the task ends.

**Technical summary**

- Gluten enters Spark through **one class**: `org.apache.gluten.GlutenPlugin` (a Spark `SparkPlugin`) configured with `spark.plugins`.
- The **driver plugin** appends `GlutenSessionExtensions` to `spark.sql.extensions`, validates/derives memory configs, and starts all **components**. The **executor plugin** starts components and wires per-task resource cleanup (`TaskResources`).
- Everything pluggable is a **`Component`**, discovered from `META-INF/gluten-components/<fully.qualified.ClassName>` files on the classpath and **topologically sorted** by declared dependencies.
- A **backend** (`VeloxBackend`, `CHBackend`) is a root component that implements `SubstraitBackend` and exposes **8 API facets** (`IteratorApi`, `SparkPlanExecApi`, `TransformerApi`, `ValidatorApi`, `MetricsApi`, `ListenerApi`, `RuleApi`, `BackendSettingsApi`). Common code reaches them through `BackendsApiManager`.
- Extension components (Iceberg, Delta, Hudi, Paimon) depend on the backend and contribute extra rules, injected **after** the backend's so they can override them.
- `VeloxListenerApi` loads the native libraries (`libgluten`, `libvelox`) and calls **`NativeBackendInitializer`**, which runs C++ `VeloxBackend::init` once per JVM (driver and executor).
- Per Spark task, a **`Runtime`** (native context + `NativeMemoryManager` + thread manager) is created lazily and registered in `TaskResources`, which releases it when the task finishes.
- Multiple Spark versions are supported via **shims** loaded with `ServiceLoader` (`SparkShimLoader`).

---

## 1. Why this design

| Requirement | Design choice |
|---|---|
| Must be drop-in for existing Spark deployments | Use Spark's public `SparkPlugin` + `SparkSessionExtensions` APIs; no Spark fork |
| Support more than one native engine | Backend abstraction (`SubstraitBackend` + `*Api` traits) |
| Optional integrations (Iceberg, Delta, ...) that only load if their jars exist | `Component` with `isRuntimeCompatible` + classpath discovery |
| Integrations must be able to override backend behavior | Component dependency DAG; rules injected in reverse topological order |
| Native engine needs one-time global init per JVM | `NativeBackendInitializer` guarded by an `AtomicBoolean` |
| Native state must be cleaned up even if a task fails | `TaskResources` registry tied to Spark task listeners |
| Spark 3.3–4.1 API differences | Shim layer (`shims/sparkXX`) |

---

## 2. Key concepts

| Concept | Description | Code |
|---|---|---|
| `GlutenPlugin` | `SparkPlugin` returning `GlutenDriverPlugin` and `GlutenExecutorPlugin` | [GlutenPlugin.scala](../../gluten-core/src/main/scala/org/apache/gluten/GlutenPlugin.scala) |
| `Component` | Unit of functionality with lifecycle hooks and rule injection | [Component.scala](../../gluten-core/src/main/scala/org/apache/gluten/component/Component.scala) |
| `Discovery` | Finds components via `META-INF/gluten-components` resources | [Discovery.scala](../../gluten-core/src/main/scala/org/apache/gluten/component/Discovery.scala) |
| `Backend` | A component with no dependencies (root of the DAG) | [Backend.scala](../../gluten-core/src/main/scala/org/apache/gluten/backend/Backend.scala) |
| `SubstraitBackend` | Backend that offloads via Substrait; owns the 8 API facets | [SubstraitBackend.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/backendsapi/SubstraitBackend.scala) |
| `BackendsApiManager` | Static accessor for the single loaded `SubstraitBackend` | [BackendsApiManager.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/backendsapi/BackendsApiManager.scala) |
| `GlutenSessionExtensions` | `SparkSessionExtensions => Unit` that asks every component to inject rules | [GlutenSessionExtensions.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/GlutenSessionExtensions.scala) |
| `Injector` | Facade with `spark` (plain Spark rules) and `gluten` (columnar rules) injectors | [Injector.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/injector/Injector.scala) |
| `NativeBackendInitializer` | One-time JNI init of the native backend | [NativeBackendInitializer.java](../../gluten-arrow/src/main/java/org/apache/gluten/init/NativeBackendInitializer.java) |
| `Runtime` / `Runtimes` | Per-task native runtime handle | [Runtime.scala](../../gluten-arrow/src/main/scala/org/apache/gluten/runtime/Runtime.scala), [Runtimes.scala](../../gluten-arrow/src/main/scala/org/apache/gluten/runtime/Runtimes.scala) |
| `TaskResources` | Per-task registry of closeable resources | [TaskResources.scala](../../gluten-core/src/main/scala/org/apache/spark/task/TaskResources.scala) |
| `SparkShimLoader` | Loads the shim matching the running Spark version | [SparkShimLoader.scala](../../shims/common/src/main/scala/org/apache/gluten/sql/shims/SparkShimLoader.scala) |

---

## 3. Architecture

How Gluten boots, from one config key down to the native library.

1. `spark.plugins=org.apache.gluten.GlutenPlugin` loads `GlutenPlugin`, which has a driver part and an executor part.
2. The driver plugin (`GlutenDriverPlugin`):
   - appends `GlutenSessionExtensions` to `spark.sql.extensions`;
   - runs `setPredefinedConfigs` (off-heap checks, per-task sizes);
   - calls `Component.sorted()`.
3. The executor plugin (`GlutenExecutorPlugin`) also calls `Component.sorted()`.
4. `Component.sorted()` discovers components from `META-INF/gluten-components/*` files, builds a dependency graph, and sorts it. `VeloxBackend` is the root; `VeloxIcebergComponent`, `VeloxDeltaComponent` and others depend on it.
5. `VeloxBackend`'s listener (`VeloxListenerApi.onDriverStart` / `onExecutorStart`) loads `libgluten.so` and `libvelox.so`, then calls `NativeBackendInitializer.initialize`, which runs C++ `VeloxBackend::init`.
6. Later, for each `SparkSession`, `GlutenSessionExtensions.apply` asks every component (in reverse order) to inject its rules.

---

## 4. Detailed walkthrough

### 4.1 `GlutenPlugin` — the entry point

```scala
class GlutenPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new GlutenDriverPlugin()
  override def executorPlugin(): ExecutorPlugin = new GlutenExecutorPlugin()
}
```

#### Driver: `GlutenDriverPlugin.init`

From [GlutenPlugin.scala](../../gluten-core/src/main/scala/org/apache/gluten/GlutenPlugin.scala):

1. **Register the SQL extension.** If `spark.sql.extensions` doesn't contain `org.apache.gluten.extension.GlutenSessionExtensions`, append it. This is how Gluten's planner rules reach every `SparkSession`.
2. **`setPredefinedConfigs(conf)`**
   - `checkOffHeapSettings`: requires `spark.memory.offHeap.enabled=true` and `spark.memory.offHeap.size >= 1MB`, unless `spark.gluten.memory.dynamic.offHeap.sizing.enabled` or `spark.gluten.memory.untracked` is set.
   - If dynamic off-heap sizing is on: forces Spark off-heap to `0/false` and derives Gluten's off-heap budget as `(executorMemory - 300MB) * spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction` (default `0.6`).
   - Writes derived internal configs that native code and memory targets read later:

     | Derived config | Value |
     |---|---|
     | `spark.gluten.memory.offHeap.size.in.bytes` | total off-heap budget |
     | `spark.gluten.numTaskSlotsPerExecutor` | `SparkResourceUtil.getTaskSlots(conf)` (cores / task.cpus) |
     | `spark.gluten.memory.task.offHeap.size.in.bytes` | off-heap / task slots |
     | `spark.gluten.memory.conservative.task.offHeap.size.in.bytes` | off-heap × (1 − `spark.memory.storageFraction`) / slots |
3. **Start components**: `Component.sorted()` → print info → copy each component's `info()` into `spark.gluten.component.<k>` configs → `onDriverStart(sc, pluginContext)` on each, in dependency order.
4. `registerMetrics` → each component (Velox attaches the **Gluten SQL UI tab** here when `spark.gluten.ui.enabled=true`).
5. `shutdown` → `onDriverShutdown` in **reverse** order.

#### Executor: `GlutenExecutorPlugin`

```scala
override def init(ctx: PluginContext, extraConf: util.Map[String,
    String]): Unit = {
  // deep Substrait plans
  CodedInputStreamClassInitializer.modifyDefaultRecursionLimitUnsafe
  Component.sorted().foreach(_.onExecutorStart(ctx))
}
// TaskResources
override def onTaskStart(): Unit = taskListeners.foreach(_.onTaskStart())
override def onTaskSucceeded(): Unit =
    taskListeners.reverse.foreach(_.onTaskSucceeded())
override def onTaskFailed(r: TaskFailedReason): Unit =
    taskListeners.reverse.foreach(_.onTaskFailed(r))
```

- The protobuf recursion limit is raised because a complex query can produce a deeply nested Substrait plan.
- `TaskResources` is hooked into every task start/finish so native resources (runtimes, memory managers, iterators) are released even on failure.

### 4.2 Components: discovery, DAG & ordering

#### Discovery

A component is declared by an **empty resource file whose name is the class name**:

```text
backends-velox/src/main/resources/META-INF/gluten-components/
  org.apache.gluten.backendsapi.velox.VeloxBackend
backends-velox/src-iceberg/main/resources/META-INF/gluten-components/
  org.apache.gluten.component.VeloxIcebergComponent
backends-velox/src-delta/main/resources/META-INF/gluten-components/
  org.apache.gluten.component.VeloxDeltaComponent
backends-velox/src-hudi/.../
  org.apache.gluten.component.VeloxHudiComponent
backends-velox/src-paimon/.../
  org.apache.gluten.component.VeloxPaimonComponent
backends-velox/src-delta33/.../
  org.apache.gluten.component.VeloxDelta33WriteComponent
backends-velox/src-delta40/.../
  org.apache.gluten.component.VeloxDelta40WriteComponent
```

`Discovery.discoverAll()` lists all `META-INF/gluten-components/*` resources, de-duplicates by class name, and instantiates each with its no-arg constructor. Which components exist therefore depends on **which Maven profiles were used to build the bundle** (e.g. `-Piceberg`).

#### Registration and ordering

`ensureAllComponentsRegistered()` ([package.scala](../../gluten-core/src/main/scala/org/apache/gluten/component/package.scala)) registers all discovered components into a graph and logs:

```text
Components registered within order: velox, velox-iceberg, velox-delta, ...
```

`Component.sorted()` returns a **topological order**: dependencies first. `Backend` overrides `dependencies()` to `Nil`, so backends are always roots.

```scala
class VeloxIcebergComponent extends Component {
  override def name(): String = "velox-iceberg"
  override def dependencies(): Seq[Class[_ <: Component]] =
      classOf[VeloxBackend] :: Nil
  override def isRuntimeCompatible: Boolean =
    SparkReflectionUtil.isClassPresent(
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  override def injectRules(injector: Injector): Unit = {
    OffloadIcebergScan.inject(injector)
    OffloadIcebergWrite.inject(injector)
  }
}
```

#### What a component can contribute

| Hook | Called | Typical use |
|---|---|---|
| `onDriverStart / onDriverShutdown` | driver plugin | listeners, config defaults, load native libs |
| `onExecutorStart / onExecutorShutdown` | executor plugin | load native libs, init native backend |
| `registerMetrics` | driver | attach UI |
| `injectRules(injector)` | per SparkSession | optimizer rules, strategies, Gluten columnar rules |
| `convFuncOverride()` | planning | tell the transition planner the batch/row type of custom plan nodes |
| `costers()` | planning | cost functions for transition/plan cost |
| `info()` | driver | build info shown in logs and `spark.gluten.component.*` |
| `isRuntimeCompatible` | registration | skip if required third-party classes are missing |

### 4.3 Backends and the API facets

`SubstraitBackend` ([SubstraitBackend.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/backendsapi/SubstraitBackend.scala)) makes its lifecycle hooks `final` and delegates to facets:

- `onDriverStart`: registers `GlutenSQLAppStatusListener`, `GlutenQueryExecutionListener`, optionally `SoftAffinityListener`; posts a build-info event; then `listenerApi().onDriverStart`.
- `injectRules`: wraps every legacy rule in `LoggedRule` (timing/logging) then calls `ruleApi().injectRules(injector)`.
- `registerMetrics`: attaches the Gluten UI tab.

The Velox implementation ([VeloxBackend.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBackend.scala#L55-L74)):

| Facet | Velox class | Responsibility |
|---|---|---|
| `IteratorApi` | `VeloxIteratorApi` | Build split infos & partitions; create native iterators for a stage (`genFirstStageIterator`, `genFinalStageIterator`) |
| `SparkPlanExecApi` | `VeloxSparkPlanExecApi` | Factory for backend transformers (joins, aggregates, shuffle exchange, broadcast, C2R/R2C, expression transformers, shuffle dependency/serializer) |
| `TransformerApi` | `VeloxTransformerApi` | Misc transform helpers (partition columns, native plan string, native config post-processing) |
| `ValidatorApi` | `VeloxValidatorApi` | Schema validation & **native validation** of Substrait plans via JNI |
| `MetricsApi` | `VeloxMetricsApi` | Define `SQLMetric`s for each transformer & map native metrics back |
| `ListenerApi` | `VeloxListenerApi` | Driver/executor startup: configs, native library loading, native init |
| `RuleApi` | `VeloxRuleApi` | The ordered list of Spark and Gluten rules (see [02](#doc-02)) |
| `BackendSettingsApi` | `VeloxBackendSettings` | Capability flags (e.g. `supportSortMergeJoinExec`, `supportColumnarShuffleExec`, `shuffleSupportedCodec`, scan format validation) |

Common code **never** checks "if Velox then ..."; it asks a facet via `BackendsApiManager`:

```scala
object BackendsApiManager {
  private lazy val backend: SubstraitBackend = {
    val loaded = Component.sorted().filter(_.isInstanceOf[SubstraitBackend])
    assert(loaded.size == 1,
        s"Zero or more than one Substrait backends are loaded: ...")
    loaded.head.asInstanceOf[SubstraitBackend]
  }
  def getSparkPlanExecApiInstance: SparkPlanExecApi = backend.sparkPlanExecApi()
  def getSettings: BackendSettingsApi = backend.settings()
  // ...
}
```

> **Important:** Exactly **one** Substrait backend must be on the classpath. Don't mix the Velox and ClickHouse bundles.

### 4.4 `VeloxListenerApi` — native startup

[VeloxListenerApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxListenerApi.scala)

**`onDriverStart`** (driver only):
- Validates cache settings (`COLUMNAR_VELOX_CACHE_ENABLED` requires the file handle cache; load quantum ≤ 8MB with SSD cache).
- Generates HDFS client config into the Spark work dir.
- **Memory overhead default**: if `spark.executor.memoryOverhead` isn't set, sets it to `max(0.3 × off-heap, 384MB)`; warns if a user value is smaller. Rationale (from the log message): native engines need overhead proportional to off-heap use, not on-heap.
- If `spark.gluten.sql.columnar.tableCache` is on, sets `spark.sql.cache.serializer` to `ColumnarCachedBatchSerializer`.
- Then `initialize(conf, isDriver = true)`.

**`onExecutorStart`**: skips static init in local mode (already done by the driver in the same JVM) and calls `initialize(conf, isDriver = false)`.

**`initialize(conf, isDriver)`** — the order matters:

1. Create the **JNI workspace** dir (where native libs are extracted), or a debug dir if `DEBUG_KEEP_JNI_WORKSPACE`.
2. Resolve UDF libraries (`UDFResolver.resolveUdfConf`).
3. Register conventions: Spark row/batch, `ArrowJavaBatchType`, `ArrowNativeBatchType`, `VeloxBatchType`, `VeloxCarrierRowType` (see [05](#doc-05)).
4. Register `ColumnarShuffleManager` in `ShuffleManagerRegistry` (used when `GlutenShuffleManager` is the configured shuffle manager).
5. `System.setProperty("arrow.struct.conflict.policy", "CONFLICT_APPEND")`.
6. Load shared libraries the backend depends on (`SharedLibraryLoaderUtils.load`), then `libgluten.so` and `libvelox.so` from the jar (or from `GLUTEN_LIB_PATH` if set).
7. **`NativeBackendInitializer.forBackend("velox").initialize(globalListener, parseConf(conf, isDriver))`** — the global off-heap listener charges native *global* allocations to `GlobalOffHeapMemory.target`.
8. Register the Velox Parquet writer injects and `VeloxRowSplitter`.

> **Note:** On the driver, `parseConf` forces `COLUMNAR_VELOX_CACHE_ENABLED=false` (issue #7837) — the driver doesn't need the data cache.

### 4.5 Native initialization (`VeloxBackend::init`)

`NativeBackendInitializer.initialize0` filters the Spark conf with `GlutenConfig.getNativeBackendConf` (only keys native cares about + defaults), lets the backend post-process them, serializes them to protobuf, and calls the native `initialize`. In C++ ([VeloxBackend.cc](../../cpp/velox/compute/VeloxBackend.cc#L123-L295)):

| Step | Detail |
|---|---|
| Logging | glog verbosity/severity from config; full verbosity in debug mode |
| Global memory manager | `VeloxMemoryManager` for process-wide allocations, wired to the global listener |
| Factories | Register `MemoryManager`, `ThreadManager`, `Runtime` factories for kind `velox` |
| Velox flags | capacity transfer across tasks, no shared leaf pools, exception stack traces, huge pages, cache prefetch |
| Filesystems | local always; HDFS/S3/GCS/ABFS when compiled in (`ENABLE_HDFS` etc.) |
| GPU | register cuDF operators/functions if `GLUTEN_ENABLE_GPU` and cuDF enabled |
| Executors | optional spill executor (`spillThreadNum`), IO executor (`kVeloxIOThreads`, default = task slots) |
| Readers/writers | Parquet reader & writer factories, ORC reader, file sinks |
| Functions | `registerAllFunctions()` (Spark SQL functions + Gluten extras), Presto vector serde (for spill) |
| UDFs | `initUdf()` loads user UDF libraries |
| Cache | `initCache()` builds the `AsyncDataCache` (memory, optionally SSD) when enabled |

This happens **once per JVM**; a second call throws `IllegalStateException("Already initialized")`. A shutdown hook releases native resources.

### 4.6 Per-SparkSession: rule injection

`GlutenSessionExtensions.apply(exts)`:

```scala
val injector = new Injector(exts)
injector.control.disableOn(session =>
    !session.conf.get("spark.gluten.enabled", "true").toBoolean)
injector.control.disableOn(session =>
  !Option(session.sparkContext.getLocalProperty(
      "gluten.enabledForCurrentThread")).forall(_.toBoolean))
// Components should override Backend's rules. Hence, reversed injection order
// is applied.
Component.sorted().reverse.foreach(_.injectRules(injector))
injector.inject()
```

- **Two kill switches**: session conf `spark.gluten.enabled` and the thread-local property `gluten.enabledForCurrentThread`. When either disables Gluten, the injected rules become no-ops for that session/thread (`InjectorControl`).
- `injector.spark` registers normal Spark extensions (optimizer rules, planner strategies, parser, functions, query-stage prep rules). They are applied immediately.
- `injector.gluten.legacy` collects **builders** for Gluten's columnar rules by phase (pre-transform, transform, post-transform, fallback policy, post, final). `injector.inject()` registers a single Spark `ColumnarRule` (`GlutenColumnarRule`) that runs them all. See [02](#doc-02).

### 4.7 Per task: `Runtime` and `TaskResources`

When native code is first needed in a task (e.g. `NativePlanEvaluator.create`), Gluten calls:

```scala
Runtimes.contextInstance(backendName, name, extraConf)
// = TaskResources.addResourceIfNotRegistered(s"$backendName:$name:$extraConf",
// () => Runtime(...))
```

`RuntimeImpl` creates:
- a `NativeMemoryManager` (native memory pools wired to Spark's `TaskMemoryManager`, see [06](#doc-06)),
- a `NativeThreadManager` (initializes native child threads with the task context),
- the native runtime via `RuntimeJniWrapper.createRuntime(backend, mmHandle, tmHandle, sessionConf)` where `sessionConf = GlutenConfig.getNativeSessionConf(...)` (session-level `spark.gluten.*`, timezone, ANSI, case sensitivity, spill buffers, …) plus `extraConf`.

`TaskResources` releases registered resources by **priority** when the task completes or fails (runtime priority is `30`). This is the main mechanism that prevents native leaks across tasks.

### 4.8 Shims

`SparkShimLoader.getSparkShims` uses `ServiceLoader` to find the `SparkShimProvider` whose `matches(SPARK_VERSION_SHORT)` is true. Providers live in `shims/spark33`, `spark34`, `spark35`, `spark40`, `spark41`. Common code calls the `SparkShims` trait for anything version-specific (e.g. `getExtendedColumnarPostRules`, `isWindowGroupLimitExec`, metadata columns).

---

## 5. End-to-end worked example: starting `spark-shell` with Gluten

The startup sequence for `spark-shell`.

1. `spark-shell` creates a `SparkContext` whose conf contains `spark.plugins`.
2. The `SparkContext` calls `GlutenDriverPlugin.init`.
3. The driver plugin adds `GlutenSessionExtensions` and sets predefined configs.
4. It discovers and sorts components, for example `[velox, velox-iceberg, ...]`.
5. It calls `onDriverStart` on the Velox backend, which:
   - registers listeners and sets a default memory overhead;
   - loads `libgluten` and `libvelox`;
   - calls `NativeBackendInitializer.initialize(conf)` to run C++ `VeloxBackend::init`.
6. The `SparkContext` registers metrics and attaches the Gluten UI tab.
7. Executors launch. In each one, `GlutenExecutorPlugin.init` calls `onExecutorStart`, which loads the libraries and initializes native code.
8. `SparkSession.builder.getOrCreate()` runs `GlutenSessionExtensions.apply`, which injects the planner rules.

What you'll see in the driver log (illustrative):

```text
INFO GlutenDriverPlugin: Gluten components:
==============================================================
Component velox
  velox_branch = ...
  velox_revision = ...
Component velox-iceberg
==============================================================
INFO component: Components registered within order: velox, velox-iceberg
INFO VeloxListenerApi: Memory overhead is not set. Setting it to 6442450944
    automatically. ...
```

**Try it / verify**

```bash
spark-shell --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=4g \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --jars package/target/gluten-velox-bundle-*.jar
```

- Driver log: `Components registered within order: velox, ...` (extension components such as `velox-iceberg` appear only if their jars and Spark extensions are present), followed by the `Gluten build info:` block.
- Spark UI → Environment: `spark.sql.extensions` contains `org.apache.gluten.extension.GlutenSessionExtensions`.
- If you didn't set `spark.executor.memoryOverhead`, the driver log shows `Memory overhead is not set. Setting it to ... automatically`.
- Smoke test: `spark.range(10).selectExpr("id + 1").explain()` should show `^ ProjectExecTransformer`.

---

## 6. Configuration reference

| Key | Default | Purpose |
|---|---|---|
| `spark.plugins` | — | Must include `org.apache.gluten.GlutenPlugin` |
| `spark.memory.offHeap.enabled` / `.size` | Spark defaults | Required (≥ 1MB) unless dynamic sizing/untracked |
| `spark.gluten.enabled` | `true` | Session-level switch for Gluten rules (experimental; prefer `spark.plugins`) |
| `spark.gluten.memory.dynamic.offHeap.sizing.enabled` | `false` (static) | Derive off-heap from executor memory instead of `spark.memory.offHeap.size` |
| `spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction` | `0.6` (static) | Fraction used by dynamic sizing |
| `spark.gluten.memory.untracked` | `false` (static) | Don't account native memory to Spark (skips off-heap check) |
| `spark.gluten.ui.enabled` | `true` (static) | Attach the Gluten SQL UI tab |
| `spark.executor.memoryOverhead` | auto: `max(0.3 × off-heap, 384MB)` if unset | Headroom for untracked native memory |
| `spark.gluten.sql.columnar.tableCache` | see [12](#doc-12) | Switches the cache serializer to the columnar one |
| `spark.gluten.soft-affinity.enabled` | see [09](#doc-09) | Registers the soft-affinity listener |

Derived/internal (written by the plugin, don't set manually): `spark.gluten.memory.offHeap.size.in.bytes`, `spark.gluten.memory.task.offHeap.size.in.bytes`, `spark.gluten.memory.conservative.task.offHeap.size.in.bytes`, `spark.gluten.numTaskSlotsPerExecutor`, `spark.gluten.memoryOverhead.size.in.bytes`, `spark.gluten.component.*`.

---

## 7. Pitfalls, debugging & FAQ

| Symptom | Likely cause | Fix |
|---|---|---|
| `GlutenException: Must set 'spark.memory.offHeap.enabled' to true ...` | Off-heap not configured | Set off-heap enabled + size, or enable dynamic sizing |
| `No component files found in container directories named with 'META-INF/gluten-components'` | Gluten jar not on driver/executor classpath | Add the bundle jar to `spark.driver/executor.extraClassPath` (or `--jars`) |
| `Zero or more than one Substrait backends are loaded` | Two backend bundles on classpath | Keep exactly one bundle |
| `UnsatisfiedLinkError` loading `libvelox.so` | Missing system libs or wrong platform | Check `ldd` on the extracted lib (keep the JNI workspace with the debug config), rebuild for your OS |
| Iceberg queries not offloaded at all | Bundle built without `-Piceberg`, or Iceberg extensions class missing (`isRuntimeCompatible` false) | Rebuild with the profile; check "Components registered" log |
| `Already initialized` | Calling `NativeBackendInitializer.initialize` twice in one JVM | Expected in custom launchers only; use the plugin |
| Gluten silently not used for one job | `spark.gluten.enabled=false` in session or thread property `gluten.enabledForCurrentThread=false` | Check session conf/local properties |

**FAQ**

- *Does the driver need native libs?* Yes — native validation runs during planning on the driver.
- *Can I disable Gluten for one query?* `spark.conf.set("spark.gluten.enabled", "false")`, or set the thread-local property `gluten.enabledForCurrentThread=false` via `sc.setLocalProperty`.
- *Where do native libraries get extracted?* Into the JNI workspace under Spark local dirs (`namespace("jni")`), random subdir per JVM.

---

## 8. Code map

| File | Role |
|---|---|
| [GlutenPlugin.scala](../../gluten-core/src/main/scala/org/apache/gluten/GlutenPlugin.scala) | Driver/executor plugins, off-heap derivation |
| [Component.scala](../../gluten-core/src/main/scala/org/apache/gluten/component/Component.scala) | Component trait, registry, dependency graph |
| [Discovery.scala](../../gluten-core/src/main/scala/org/apache/gluten/component/Discovery.scala) | Classpath discovery |
| [GlutenSessionExtensions.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/GlutenSessionExtensions.scala) | Session extension entry |
| [GlutenInjector.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/injector/GlutenInjector.scala), [SparkInjector.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/injector/SparkInjector.scala) | Rule injectors |
| [SubstraitBackend.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/backendsapi/SubstraitBackend.scala) | Backend base & facets |
| [BackendsApiManager.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/backendsapi/BackendsApiManager.scala) | Facet access |
| [VeloxBackend.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBackend.scala) | Velox backend & settings |
| [VeloxListenerApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxListenerApi.scala) | Startup logic |
| [NativeBackendInitializer.java](../../gluten-arrow/src/main/java/org/apache/gluten/init/NativeBackendInitializer.java) | JNI init |
| [VeloxBackend.cc](../../cpp/velox/compute/VeloxBackend.cc) | Native global init |
| [Runtime.scala](../../gluten-arrow/src/main/scala/org/apache/gluten/runtime/Runtime.scala) | Per-task runtime |
| [TaskResources.scala](../../gluten-core/src/main/scala/org/apache/spark/task/TaskResources.scala) | Task-scoped cleanup |
| [VeloxIcebergComponent.scala](../../backends-velox/src-iceberg/main/scala/org/apache/gluten/component/VeloxIcebergComponent.scala) | Example extension component |

## 9. Related docs

- Next: [02 · Planning, offload, validation & fallback](#doc-02)
- [04 · Native runtime & JNI](#doc-04) (what `Runtime` does at execution time)
- [06 · Memory management](#doc-06) (memory managers created by `Runtime`)
- Repo: [docs/developers/NewToGluten.md](../../docs/developers/NewToGluten.md), [docs/get-started/GlutenUI.md](../../docs/get-started/GlutenUI.md)

---

<a id="doc-02"></a>

# 02 · Query Planning: Offload, Validation & Fallback

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [01 · Bootstrap](#doc-01) · Next: [03 · Substrait](#doc-03)

---

## TL;DR

- Gluten rewrites Spark's **physical plan**: every operator it can run natively is swapped for a native "transformer"; the rest stay as normal Spark.
- Before swapping, each operator is **validated**, both by quick JVM checks and by asking Velox itself. If either says no, the node falls back and the reason is recorded.
- Neighbouring native operators are glued into one **native stage**, and conversion nodes are added wherever native and Spark operators meet.
- To see why something fell back, check the **Gluten UI tab** or call `df.fallbackSummary()`.

**Technical summary**

- Gluten does **not** change Catalyst's logical planning much. It hooks into Spark's **physical planning** through a single `ColumnarRule` (`GlutenColumnarRule`) and rewrites the physical plan.
- The rewrite runs in **4 phases** (`HeuristicApplier`): **transform → fallback → post → final**.
- The core of "transform" is `HeuristicTransform.WithRewrites`: **rewrite** (normalize nodes so native can run them, e.g. pull expressions out into projects) → **validate** (tag nodes that can't be offloaded) → **offload** (replace untagged nodes with `*Transformer`s).
- Validation is layered: cheap Scala-side checks (configs, backend capabilities, expression depth, types) followed by **native validation**: the would-be transformer builds its Substrait `RelNode` and the **Velox validator** (in C++, via JNI) checks it.
- A failed validation leaves a **`FallbackTag`** with a human-readable reason on the node; the node stays vanilla Spark and **transitions** are inserted around it.
- `ExpandFallbackPolicy` can fall back a **whole stage/query** if too many transitions would result (thresholds default `-1` = disabled).
- `ColumnarCollapseTransformStages` groups adjacent transformers into **`WholeStageTransformer`**s, which become native tasks.
- Fallback reasons are logged (`Validation failed for plan: ...`), shown in the **Gluten SQL UI tab**, and available via `df.fallbackSummary()`.

---

## 1. Why planning works this way

| Goal | Consequence in design |
|---|---|
| **Correctness first** | Never offload unless proven supported → validate every node, including in native code |
| **Per-operator granularity** | Mixed plans (native + vanilla) must work → explicit transitions & conventions |
| **Avoid "transition storms"** | Many tiny offloaded islands can be slower than vanilla → fallback policy based on transition count |
| **Backend neutrality** | Common rules live in `gluten-core`/`gluten-substrait`; backend supplies the rule list (`VeloxRuleApi`) and factories (`SparkPlanExecApi`) |
| **Work with AQE** | Rules run on each AQE query stage re-plan; tags are propagated to logical plans so reasons aren't lost across stages |

### Why native validation?

Scala can't know every detail of what Velox supports (function signatures, type combinations, operator options). Asking Velox directly — by converting the candidate Substrait plan with the real converter and compiling expressions — is the most reliable way to avoid runtime failures. The cost is a JNI call per candidate node during planning (on the driver).

---

## 2. Key concepts

| Concept | What it is | Code |
|---|---|---|
| `GlutenColumnarRule` | Spark `ColumnarRule`; all Gluten logic runs in `postColumnarTransitions` | [GlutenColumnarRule.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/GlutenColumnarRule.scala) |
| `HeuristicApplier` | Runs the 4 rule phases | [HeuristicApplier.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/heuristic/HeuristicApplier.scala) |
| `HeuristicTransform` | rewrite → validate → offload | [HeuristicTransform.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/heuristic/HeuristicTransform.scala) |
| `RewriteSingleNode` | Rule that rewrites one node into an equivalent native-friendly shape | `extension/columnar/rewrite` |
| `Validator` | Returns `Passed` or `Failed(reason)` for a node | [Validator.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/validator/Validator.scala), [Validators.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/validator/Validators.scala) |
| `FallbackTag` / `FallbackTags` | TreeNodeTag storing why a node falls back (`Appendable` or `Exclusive`) | [FallbackTag.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/FallbackTag.scala) |
| `OffloadSingleNode` | Rule replacing a Spark node with a transformer | [OffloadSingleNodeRules.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/offload/OffloadSingleNodeRules.scala) |
| `ValidatablePlan` | Transformers implement `doValidateInternal`; `doValidate` adds schema checks & exception handling | [ValidatablePlan.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/ValidatablePlan.scala) |
| `TransformSupport` | Base trait of transformers: `doTransform(SubstraitContext)`, `columnarInputRDDs`, metrics | [WholeStageTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/WholeStageTransformer.scala#L42-L135) |
| `ExpandFallbackPolicy` | Whole-stage/query fallback decision | [ExpandFallbackPolicy.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/heuristic/ExpandFallbackPolicy.scala) |
| `ColumnarCollapseTransformStages` | Builds `WholeStageTransformer` + `InputIteratorTransformer` boundaries | [ColumnarCollapseTransformStages.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/execution/ColumnarCollapseTransformStages.scala) |
| `GlutenFallbackReporter` | Logs fallback reasons and posts them to the UI | [GlutenFallbackReporter.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/execution/GlutenFallbackReporter.scala) |

> **Note:** Older Gluten versions also had an experimental cost-based planner ("RAS"). In v1.7.0 `GlutenInjector` only builds the **heuristic (legacy) applier**; the remaining `*-ras` approved-plan test resources are historical.

---

## 3. Architecture

### 3.1 Where Gluten hooks into Spark

Spark's `ApplyColumnarRulesAndInsertTransitions` (part of `QueryExecution.preparations` and AQE's re-planning) calls `preColumnarTransitions`, inserts Spark's own transitions, then calls `postColumnarTransitions`. Gluten:

1. `preColumnarTransitions` only **wraps** the plan in a dummy node to remember whether the caller wants columnar or row output.
2. `postColumnarTransitions` unwraps, **removes Spark's transitions** (`Transitions.insert` on the original plan), and runs Gluten's applier with `outputsColumnar`.

How Gluten's rule sits inside Spark's own columnar planning.

1. Start from Spark's physical plan.
2. `preColumnarTransitions` wraps the plan (in `DummyRow`/`ColumnarOutputExec`) so Spark doesn't interfere.
3. Spark inserts its own C2R/R2C transitions.
4. `postColumnarTransitions` unwraps the plan and records whether the output must be columnar.
5. `HeuristicApplier.apply(plan, outputsColumnar)` runs Gluten's rules.
6. The result is a Gluten plan with transformers, transitions and `WholeStageTransformer`s.

### 3.2 The four phases

The four phases of `HeuristicApplier`, in order.

1. **Transform.** Start from the vanilla physical plan.
   - pre-transform rules;
   - `HeuristicTransform`: rewrite, validate (`AddFallbackTags`), offload;
   - post-transform rules, including `InsertTransitions`.
2. **Fallback.** `ExpandFallbackPolicy` decides whether to keep the Gluten plan, or to replace a stage or the whole query with the vanilla plan (`FallbackNode`).
3. **Post.** Only if the Gluten plan was kept: `RemoveTopmostColumnarToRow`, `ColumnarCollapseTransformStages`, `GenerateTransformStageId`, and others.
4. **Final.** Always: `GlutenFallbackReporter`, `RemoveFallbackTagRule`, and others.

The output is the executed plan.

Code (simplified from `HeuristicApplier.makeRule`):

```scala
val suggestedPlan = transformPlan("transform", transformRules(call),
    originalPlan)
val finalPlan = transformPlan("fallback",
    fallbackPolicies(call).map(_(originalPlan)), suggestedPlan) match {
  // skip post rules: no GlutenPlan left
  case FallbackNode(fallbackPlan) => fallbackPlan
  case plan                       => transformPlan("post", postRules(call),
      plan)
}
transformPlan("final", finalRules(call), finalPlan)
```

Each phase runs its rules through `ColumnarRuleExecutor`; every rule is wrapped by `LoggedRule` (added by `SubstraitBackend.injectRules`) for timing/debug logging.

---

## 4. The Velox rule list (v1.7.0)

From [VeloxRuleApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxRuleApi.scala). Order matters.

### 4.1 Plain Spark extensions (`injectSpark`)

| Kind | Rule | Purpose |
|---|---|---|
| Optimizer | `CollectRewriteRule`, `HLLRewriteRule` | Rewrite `collect_list/set`, HyperLogLog aggregates into Velox-friendly forms |
| Optimizer | `CollapseGetJsonObjectExpressionRule` | Merge nested `get_json_object` calls |
| Optimizer | `RewriteCastFromArray`, `RewriteUnboundedWindow` | Normalize casts/windows |
| Optimizer | `PushAggregateThroughJoinBatch` | Aggregate push-down through joins |
| Strategy | `ImplementJoinAggregate`, `GlutenJoinKeysCapture` | Join planning helpers |
| Strategy | CTAS rewrite (if `supportAppendDataExec`) | Enable native V2 writes |

### 4.2 Gluten columnar rules (`injectLegacy`)

**Pre-transform**

| Rule | Purpose |
|---|---|
| `RemoveTransitions` | Strip Spark-inserted transitions so Gluten can re-plan them |
| `PushDownInputFileExpression.PreOffload` | Handle `input_file_name()` & friends |
| `FallbackOnANSIMode` | If `spark.sql.ansi.enabled` **and** `spark.gluten.sql.ansiFallback.enabled` (default `true`) → tag **every** node "does not support ansi mode" |
| `FallbackMultiCodegens` | Optional (`physicalJoinOptimizeEnable`, default `false`): fall back deep chains of joins with wide output |
| `MergeTwoPhasesHashBaseAggregate` | Merge partial+final aggregates when there is no shuffle between them |
| `RewriteSubqueryBroadcast` | Make subquery broadcasts compatible with columnar broadcast |
| `BloomFilterMightContainJointRewriteRule` | Runtime bloom filter rewrite |
| `EliminateRedundantGetTimestamp` | Expression cleanup |

**Transform** — `HeuristicTransform.WithRewrites(validator, rewrites, offloads)`:

- *rewrites*: `RewriteIn`, `RewriteMultiChildrenCount`, `RewriteJoin`, `PullOutPreProject`, `PullOutPostProject`, `ProjectColumnPruning`
- *validator*: `Validators.newValidator(conf, offloads)` (see §5)
- *offloads*: `OffloadOthers`, `OffloadExchange`, `OffloadJoin` (each as a **strict** rule: children are hidden while offloading so a rule can't peek into them)

**Post-transform**

| Rule | Purpose |
|---|---|
| `AppendBatchResizeForShuffleInputAndOutput` | Insert `VeloxResizeBatches` around shuffles |
| `GpuBufferBatchResizeForShuffleInputOutput` | GPU variant |
| `UnionTransformerRule` | Native union handling |
| `PartialFallbackRules` | **Partial project/generate**: keep the supported part native and evaluate only the unsupported expressions (e.g. a Scala UDF) in the JVM (`spark.gluten.sql.columnar.partial.project`, default `true`) |
| `RemoveNativeWriteFilesSortAndProject` | Clean up write plans |
| `PushDownFilterToScan` | Push filter conditions into the native scan |
| `EnsureLocalSortRequirements`, `EliminateLocalSort` | Sort normalization |
| `CollapseProjectExecTransformer` | Merge adjacent projects |
| `FlushableHashAggregateRule` | Turn eligible partial aggregates into **flushable** ones |
| `CollectLimitTransformerRule`, `CollectTailTransformerRule` | Limit/tail offload |
| `V2WritePostRule` | DSv2 writes |
| `InsertTransitions.create(outputsColumnar, VeloxBatchType)` | Insert C2R/R2C (and other) transitions where conventions mismatch ([05](#doc-05)) |

**Fallback policy**: `ExpandFallbackPolicy(isAqe, originalPlan)`.

**Post**: `RemoveTopmostColumnarToRow`, shim post rules, **`ColumnarCollapseTransformStages`**, `GenerateTransformStageId`, `CudfNodeValidationRule`, `GlutenNoopWriterRule`.

**Final**: `RemoveGlutenTableCacheColumnarToRow`, `PreventBatchTypeMismatchInTableCache`, `GlutenAutoAdjustStageResourceProfile`, **`GlutenFallbackReporter`**, `RemoveFallbackTagRule`.

---

## 5. Validation in depth

### 5.1 The validator chain

`Validators.newValidator(conf, offloads)` builds (in order):

| # | Validator | Fails when |
|---|---|---|
| 1 | `fallbackByHint` | Node already has a `FallbackTag` |
| 2 | `fallbackIfScanOnlyWithFilterPushed` | `spark.gluten.sql.columnar.scanOnly=true` and node is not a scan (or filter directly on a scan) |
| 3 | `fallbackComplexExpressions` | Any expression deeper than `spark.gluten.sql.columnar.fallback.expressions.threshold` (default `50`) |
| 4 | `fallbackByBackendSettings` | Backend says "unsupported" (e.g. sort-merge join (SMJ) when `supportSortMergeJoinExec` is false, native write disabled) |
| 5 | `fallbackByUserOptions` | Operator disabled by config, e.g. `spark.gluten.sql.columnar.filter=false`, `.project`, `.sort`, `.window`, `.hashagg`, `.shuffle`, `.broadcastJoin`, ... |
| 6 | `fallbackByTimestampNTZ` | Input/output contains `TimestampNTZType` (with narrow exceptions) |
| 7 | `fallbackByTestInjects` | Test-only injection |
| 8 | **`fallbackByNativeValidation(offloads)`** | Offload attempt produces a `ValidatablePlan` whose `doValidate()` fails |

Native validation happens last because it's the most expensive.

### 5.2 What `doValidate()` does

```scala
final def doValidate(): ValidationResult = {
  // 1) schema check via ValidatorApi.doSchemaValidate(schema) -> unsupported
  // types fail here
  // 2) failValidationWithException { TransformerState.enterValidation;
  // doValidateInternal() }
  // - GlutenNotSupportException / UnsupportedOperationException become
  // ValidationResult.failed(
  //      "Validation failed with exception from: <node>, reason: <msg>")
}
```

Each transformer implements `doValidateInternal()`. The typical pattern (from `FilterExecTransformerBase`):

```scala
override protected def doValidateInternal(): ValidationResult = {
  val remainingCondition = getRemainingCondition
  // fully pushed into scan
  if (remainingCondition == null) return ValidationResult.succeeded
  val substraitContext = new SubstraitContext
  val operatorId = substraitContext.nextOperatorId(this.nodeName)
  // 1) can we even build Substrait for this node? (expression conversion may
  // throw)
  val relNode = getRelNode(substraitContext, remainingCondition,
      child.output, operatorId, null, validation = true)
  // 2) ask the native engine
  doNativeValidation(substraitContext, relNode)
}
```

`doNativeValidation` wraps the `RelNode` into a plan (`PlanBuilder.makePlan`) and calls `ValidatorApi.doNativeValidateWithFailureReason`. For Velox this goes through JNI into `SubstraitToVeloxPlanValidator::validate(plan)`, which:
- builds the function map,
- runs `validate(rel)` per Rel type (checks types, join types, window frames, aggregate functions, ...),
- for expressions, converts them with `SubstraitToVeloxExpr` and **compiles** them in an `exec::ExprSet` — any unregistered function or signature mismatch throws and fails validation,
- returns the collected failure messages back to Java.

`validation = true` tells builders they are only validating (e.g. no subquery execution). `TransformerState.underValidationState` prevents `transform()` from materializing subqueries during validation.

> **Tip:** `spark.gluten.sql.validation.failFast` (default `true`) stops after the schema check fails; set `spark.gluten.sql.validation.printStackOnFailure=true` to log the stack trace of validation exceptions, and lower `spark.gluten.sql.validation.logLevel` (default `WARN`) as needed.

### 5.3 Rewrites and validation interplay

Some nodes are only offloadable after a rewrite. `RewriteSparkPlanRulesManager`:
1. wraps the node's children in `RewrittenNodeWall` (so rewrite rules don't touch them),
2. applies rewrite rules (e.g. `PullOutPreProject` moves `a+b` out of `HashAggregate(sum(a+b))` into a new `Project` below),
3. validates the rewritten fragment,
4. if the main node still fails, **keeps the original** node and tags it; otherwise keeps the rewritten fragment.

Why `PullOutPreProject`? From its scaladoc: *"The native engine only supports executing Expressions within the project operator. When there are Expressions within physical operators such as SortExec and HashAggregateExec ... we need to pull out the Expressions to be executed earlier in the pre-project operator."*

### 5.4 Fallback tags

```scala
// Appendable: reasons accumulate ("r1; r2")
FallbackTags.add(plan, "reason")
// overrides others
FallbackTags.add(plan, FallbackTag.Exclusive("whole stage fallback"))
FallbackTags.nonEmpty(plan)                    // => node will stay vanilla
```

Offload rules check `FallbackTags.nonEmpty(p)` first and return the node unchanged if tagged.

---

## 6. Offload: Spark node → transformer

`OffloadOthers`, `OffloadExchange`, `OffloadJoin` map nodes through `SparkPlanExecApi` factories so backends can return their own subclasses:

| Spark node | Transformer (Velox) |
|---|---|
| `FileSourceScanExec`, `BatchScanExec`, Hive scan | `FileSourceScanExecTransformer`, `BatchScanExecTransformer`, `HiveTableScanExecTransformer` (via `ScanTransformerFactory`) |
| `FilterExec` | `FilterExecTransformer` |
| `ProjectExec` | `ProjectExecTransformer` |
| `HashAggregateExec`, `ObjectHashAggregateExec`, `SortAggregateExec` | `RegularHashAggregateExecTransformer` / `FlushableHashAggregateExecTransformer` (later rule) |
| `SortExec` | `SortExecTransformer` |
| `WindowExec`, `WindowGroupLimitExec` | `WindowExecTransformer`, `WindowGroupLimitExecTransformer` |
| `ShuffledHashJoinExec`, `SortMergeJoinExec`, `BroadcastHashJoinExec` | `ShuffledHashJoinExecTransformer`, `SortMergeJoinExecTransformer`, `BroadcastHashJoinExecTransformer` |
| `BroadcastNestedLoopJoinExec`, `CartesianProductExec` | `BroadcastNestedLoopJoinExecTransformer`, `CartesianProductExecTransformer` |
| `ShuffleExchangeExec` | `ColumnarShuffleExchangeExec` (`ColumnarExchange` in EXPLAIN) |
| `BroadcastExchangeExec` | `ColumnarBroadcastExchangeExec` |
| `UnionExec`, `ExpandExec`, `GenerateExec`, `Global/LocalLimitExec`, `TakeOrderedAndProjectExec`, `CoalesceExec`, `RangeExec`, `SampleExec` | corresponding transformers / columnar execs |
| `WriteFilesExec` | `WriteFilesExecTransformer` |
| `ArrowEvalPythonExec` | `ColumnarArrowEvalPythonExec` |

Details of each operator: [12 · Operators](#doc-12).

> **Note:** With `spark.gluten.sql.columnar.forceShuffledHashJoin=true` (default), the Velox backend prefers shuffled hash join over sort-merge join (via `RewriteJoin`), because Velox's hash join is typically faster and avoids sorts.

---

## 7. Whole-stage fallback policy

`ExpandFallbackPolicy` counts **transition cost** (number of row↔columnar transitions; R2C ignored by default via `spark.gluten.sql.columnar.fallback.ignoreRowToColumnar=true`):

| Context | Threshold used | Default |
|---|---|---|
| Inside AQE (per query stage) | `spark.gluten.sql.columnar.wholeStage.fallback.threshold` | `-1` (off) |
| Without AQE (whole query) | `spark.gluten.sql.columnar.query.fallback.threshold` | `-1` (off) |

If enabled and `netTransitionCost >= threshold`, the policy builds the vanilla plan and compares: with `spark.gluten.sql.columnar.fallback.preferColumnar=true` (default) it keeps the Gluten plan when the vanilla plan wouldn't have fewer transitions (e.g. a Gluten Parquet scan + one C2R is still better than a vanilla scan). Otherwise it returns `FallbackNode(vanillaPlan)` with an `Exclusive` reason like:

```text
Fallback policy is taking effect, net transition cost: 3, cost: 3,
    fallback cost: 0, threshold: 2
```

The scaladoc lists important safety cases (e.g. a broadcast hash join (BHJ) whose broadcast side is already columnar must stay columnar).

---

## 8. Building native stages: `ColumnarCollapseTransformStages`

After transformation, adjacent `TransformSupport` nodes are grouped:

```scala
private def insertWholeStageTransformer(plan: SparkPlan): SparkPlan =
    plan match {
  case wst: WholeStageTransformer => wst
  case t if supportTransform(t) =>
    WholeStageTransformer(t.withNewChildren(t.children.map(
        insertInputIteratorTransformer)))(-1)
  case other => other.withNewChildren(other.children.map(
      insertWholeStageTransformer))
}
```

- The **top-most** transformer of a contiguous region becomes the child of a new `WholeStageTransformer`.
- Wherever a child is **not** a transformer (a shuffle stage, broadcast, a fallen-back operator, another whole stage), Gluten inserts `InputIteratorTransformer(ColumnarInputAdapter(child))`.
  - In Substrait this becomes a `ReadRel` over an **input iterator** — at runtime the child's batches are streamed into the native task.
- `GenerateTransformStageId` assigns the `(N)` ids shown in EXPLAIN as `WholeStageCodegenTransformer (N)`.

A `WholeStageTransformer` with leaf scans creates a `GlutenWholeStageColumnarRDD` (first-stage iterator with splits); one without leaf scans creates a `WholeStageZippedPartitionsRDD` that zips its input RDDs (final-stage iterator). See [04](#doc-04).

---

## 9. End-to-end worked examples

### 9.1 Fully offloaded query (TPC-H Q3)

Real approved plan (Spark 3.5) from [tpch-approved-plan/v1/spark35/3.txt](../../backends-velox/src/test/resources/tpch-approved-plan/v1/spark35/3.txt):

```text
VeloxColumnarToRow (42)
+- TakeOrderedAndProjectExecTransformer (41)
   +- ^ ProjectExecTransformer (39)
      +- ^ RegularHashAggregateExecTransformer (38)
         +- ^ ProjectExecTransformer (37)
            +- ^ ShuffledHashJoinExecTransformer Inner BuildLeft (36)
               :- ^ InputIteratorTransformer (26)
               :  +- ShuffleQueryStage (24)
               :     +- ColumnarExchange (23)
               :        +- VeloxResizeBatches (22)
               :           +- ^ ProjectExecTransformer (20)
               :              +- ^ ShuffledHashJoinExecTransformer Inner
               :                       BuildLeft (19)
               :                 :- ^ InputIteratorTransformer (9) ...
               +- ^ InputIteratorTransformer (35) ...
```

Observe:
- The Initial Plan had `SortMergeJoin` + `Sort`; Gluten turned them into **`ShuffledHashJoinExecTransformer`** (forceShuffledHashJoin) — no sorts needed.
- Each `ShuffleQueryStage` boundary splits native stages; `InputIteratorTransformer` marks the stage inputs.

### 9.2 A forced fallback

```scala
// disable native sort
spark.conf.set("spark.gluten.sql.columnar.sort", "false")
val df = spark.read.parquet("/tmp/t").where("v > 10").orderBy("k")
df.explain("formatted")
```

Illustrative result:

```text
Sort [k ASC]                                   <- vanilla (validator #5 failed)
+- VeloxColumnarToRow                          <- transition inserted
   +- ^ InputIteratorTransformer
      +- ShuffleQueryStage
         +- ColumnarExchange
            +- ^ FilterExecTransformer
               +- ^ FileSourceScanExecTransformer parquet
```

Driver log (`GlutenFallbackReporter`, level from `spark.gluten.sql.validation.logLevel`):

```text
WARN GlutenFallbackReporter: Validation failed for plan: Sort[QueryId=3],
    due to: [FallbackByUserOptions] Validation failed on node Sort.
```

Programmatic summary:

```scala
import org.apache.spark.sql.execution.GlutenImplicits._
df.fallbackSummary()
// FallbackSummary(numGlutenNodes = 4, numFallbackNodes = 1, ...,
// fallbackNodeToReason = Seq(Map("Sort" -> "...")))
```

### 9.3 An unsupported expression

```scala
val f = udf((s: String) => s.reverse)
spark.read.parquet("/tmp/s").select(f($"name"), $"id" + 1)
```

- Expression conversion throws `GlutenNotSupportException("Not supported scala udf: ...")` while building the project's Substrait → validation fails with `Validation failed with exception from: ProjectExecTransformer, reason: Not supported scala udf ...`.
- Because `spark.gluten.sql.columnar.partial.project=true`, `PartialFallbackRules` can still keep `id + 1` native and evaluate only the UDF in the JVM (a partial project node). See [docs/developers/PartialProject.md](../../docs/developers/PartialProject.md) and [11](#doc-11).

**Try it / verify**

- Run either example, then `import org.apache.spark.sql.execution.GlutenImplicits._; df.fallbackSummary()`. `numFallbackNodes` and `fallbackNodeToReason` should match what the example predicts.
- Set `spark.gluten.sql.validation.logLevel=INFO` to see every `Validation failed for plan` line in the driver log.
- Set `spark.gluten.sql.columnar.failOnFallback=true` to turn any fallback into an exception; useful in tests.
- A worked fallback investigation is in [13 §6.1](#doc-13).

---

## 10. Configuration reference

- `spark.gluten.enabled`, default `true`: Session switch for all Gluten rules. *When to change:* Disable per session for A/B.
- `spark.gluten.sql.columnar.<op>` (e.g. `filter`, `project`, `sort`, `window`, `hashagg`, `shuffle`, `broadcastJoin`, `sortMergeJoin`, `shuffledHashJoin`, `expand`, `union`, `limit`, `generate`, `filescan`, `batchscan`, ...), default mostly `true`: Disable offload of a specific operator. *When to change:* Isolate bugs/perf issues.
- `spark.gluten.sql.columnar.forceShuffledHashJoin`, default `true`: Prefer shuffled hash join (SHJ) over sort-merge join (SMJ) *When to change:* Set `false` if hash build sides are huge and spill poorly.
- `spark.gluten.sql.columnar.scanOnly`, default `false`: Offload only scan (+ filter on scan) *When to change:* Conservative rollout.
- `spark.gluten.sql.columnar.fallback.expressions.threshold`, default `50`: Max expression tree depth for offload. *When to change:* Very deep generated expressions.
- `spark.gluten.sql.columnar.wholeStage.fallback.threshold`, default `-1`: Stage-level fallback by transition count (AQE) *When to change:* Avoid many tiny native islands.
- `spark.gluten.sql.columnar.query.fallback.threshold`, default `-1`: Query-level fallback (non-AQE) *When to change:* Same, without AQE.
- `spark.gluten.sql.columnar.fallback.ignoreRowToColumnar`, default `true`: Don't count R2C in transition cost.
- `spark.gluten.sql.columnar.fallback.preferColumnar`, default `true`: Keep Gluten plan when vanilla isn't cheaper.
- `spark.gluten.sql.ansiFallback.enabled`, default `true`: With ANSI mode on, fall back everything. *When to change:* Set `false` to try native ANSI support.
- `spark.gluten.sql.columnar.partial.project`, default `true`: Partial project for unsupported expressions. *When to change:* Disable if partial project misbehaves.
- `spark.gluten.sql.columnar.physicalJoinOptimizeEnable`, default `false`: Enable `FallbackMultiCodegens` *When to change:* Rarely.
- `spark.gluten.sql.enable.native.validation`, default `true` (internal): Turn off native validation. *When to change:* Debug only — risky.
- `spark.gluten.sql.validation.logLevel`, default `WARN`: Log level for fallback reasons. *When to change:* Lower to `INFO`/`DEBUG` to quiet.
- `spark.gluten.sql.validation.printStackOnFailure`, default `false`: Log stack traces on validation failure. *When to change:* Debug.
- `spark.gluten.sql.validation.failFast`, default `true`: Stop after schema validation failure. *When to change:* Debug.
- `spark.gluten.sql.columnar.fallbackReporter`, default `true` (internal): Enable reporter rule.
- `spark.gluten.sql.columnar.failOnFallback`, default `false` (internal): Throw if anything falls back. *When to change:* CI / tests to catch regressions.
- `spark.gluten.sql.injectNativePlanStringToExplain`, default `false`: Print Velox plan in EXPLAIN. *When to change:* Debugging plans.

---

## 11. Pitfalls, debugging & FAQ

- **"Why is my whole query vanilla?"** Check ANSI mode (`spark.sql.ansi.enabled` + `ansiFallback.enabled`), `spark.gluten.enabled`, and the whole-stage/query fallback thresholds. Look for `Fallback policy is taking effect` in logs.
- **Reasons disappear between AQE stages?** `GlutenFallbackReporter` copies tags to the node's `logicalLink`, so later stages can still report them.
- **Validation is slow for huge plans** — each candidate node triggers a native validation call. `spark.gluten.sql.cacheWholeStageTransformerContext` caches the generated Substrait context per stage (default `false`).
- **A transition storm** (many `VeloxColumnarToRow`/`RowToVeloxColumnar` pairs) usually means one unsupported operator in the middle of a pipeline. Fix the operator or enable a stage fallback threshold.
- **Tests**: set `spark.gluten.sql.columnar.failOnFallback=true` to make any fallback an error.
- **Breakpoints**: `HeuristicApplier.makeRule`, `Validators.FallbackByNativeValidation.validate`, `ColumnarCollapseTransformStages.apply`, `GlutenFallbackReporter.apply`.

---

## 12. Code map

| File | Role |
|---|---|
| [GlutenColumnarRule.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/GlutenColumnarRule.scala) | Hook into Spark |
| [GlutenInjector.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/injector/GlutenInjector.scala) | Rule phases builder |
| [HeuristicApplier.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/heuristic/HeuristicApplier.scala) | Phase driver |
| [HeuristicTransform.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/heuristic/HeuristicTransform.scala) | rewrite/validate/offload |
| [AddFallbackTags.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/heuristic/AddFallbackTags.scala) | Validation pass |
| [RewriteSparkPlanRulesManager.scala](../../gluten-core/src/main/scala/org/apache/gluten/extension/columnar/heuristic/RewriteSparkPlanRulesManager.scala) | Rewrite with walls |
| [Validators.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/validator/Validators.scala) | Validator chain |
| [OffloadSingleNodeRules.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/offload/OffloadSingleNodeRules.scala) | Offload mapping |
| [FallbackRules.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/FallbackRules.scala) | ANSI & multi-codegen fallback |
| [PullOutPreProject.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/rewrite/PullOutPreProject.scala) | Expression pull-out |
| [ValidatablePlan.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/ValidatablePlan.scala) | doValidate |
| [BasicPhysicalOperatorTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/BasicPhysicalOperatorTransformer.scala) | Filter/Project transformers (pattern) |
| [ColumnarCollapseTransformStages.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/execution/ColumnarCollapseTransformStages.scala) | Stage grouping |
| [GlutenFallbackReporter.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/execution/GlutenFallbackReporter.scala) | Reporting |
| [GlutenImplicits.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/execution/GlutenImplicits.scala) | `fallbackSummary()` |
| [SubstraitToVeloxPlanValidator.cc](../../cpp/velox/substrait/SubstraitToVeloxPlanValidator.cc) | Native validator |
| [VeloxRuleApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxRuleApi.scala) | Rule list |

## 13. Related docs

- [03 · Substrait & native plan conversion](#doc-03) — what `doTransform` produces
- [05 · Transitions](#doc-05) — `InsertTransitions` in detail
- [11 · Expressions](#doc-11) — why expressions fail validation
- [13 · Troubleshooting](#doc-13) — fallback debugging workflow
- Repo: [docs/velox-backend-limitations.md](../../docs/velox-backend-limitations.md), [docs/developers/PartialProject.md](../../docs/developers/PartialProject.md)

---

<a id="doc-03"></a>

# 03 · Substrait Plan Generation & Conversion to Velox Plans

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [02 · Planning](#doc-02) · Next: [04 · Native runtime](#doc-04)

---

## TL;DR

- **Substrait** is the plan format the JVM sends to C++: a protobuf description of "scan → filter → aggregate …".
- The driver builds one Substrait plan per native stage. On executors, C++ turns it into a **Velox plan** and runs it.
- The same converter checks plans during planning, so "Velox can't run this" is found **before** the query starts.
- When native results look wrong, dump the Substrait plan and the Velox plan. That shows exactly what Velox was asked to do.

**Technical summary**

- **Substrait** is the contract between the JVM planner and the native engine: a protobuf relational-algebra IR. Gluten vendors a **modified** copy (base v0.23.0) under `gluten-substrait/src/main/resources/substrait/proto`.
- On the driver, each `WholeStageTransformer` calls `transform(context)` on its child tree; every transformer's `doTransform` returns a `TransformContext(outputAttributes, RelNode)`, building a Rel tree bottom-up. `PlanBuilder.makePlan` wraps it into a `Plan` with function extensions and output names.
- Functions are referenced by **id** with a **typed signature name** like `gt:i32_i32`; the mapping lives in the plan's `extensions`. Columns are referenced by **ordinal** (field references), named `name#exprId`.
- Scans produce a `ReadRel` with `LocalFiles` whose per-task content (files, offsets, partition values) is shipped separately as **SplitInfo**; stage inputs (shuffle, broadcast, fallback operators) are `ReadRel`s whose `LocalFiles` path is `iterator:<idx>`.
- On the executor, `VeloxRuntime::createResultIterator` uses `SubstraitToVeloxPlanConverter` to turn each `Rel` into a Velox `PlanNode` (`FilterNode`, `ProjectNode`, `AggregationNode`, `HashJoinNode`, `TableScanNode`, ...), and `SubstraitToVeloxExpr` to turn expressions into Velox typed expressions.
- The same converter (in **validation mode**) powers native validation during planning via `SubstraitToVeloxPlanValidator`.
- Debug: `WholeStageTransformer.substraitPlanJson`, `nativePlanString()`, `spark.gluten.sql.injectNativePlanStringToExplain=true`, and dumping plans/splits/data for a task with `spark.gluten.sql.benchmark_task.*` + `spark.gluten.saveDir`.

---

## 1. Why Substrait

| Alternative | Problem |
|---|---|
| Call the native engine per operator via JNI | JNI crossing per operator/batch; no fusion; complex lifetime management |
| Serialize Spark's own physical plan | Spark-internal, unstable, JVM-specific, impossible to consume from C++ |
| Invent a private IR | Each backend would need its own; no ecosystem |
| **Substrait** | Open, protobuf-based, language-neutral; a whole stage = one message → one JNI call to build a native pipeline |

Gluten modifies Substrait where it lacks Spark features (window functions, expand, generate, write, partition columns, kafka read, TopN...). All changes are listed in [docs/developers/SubstraitModifications.md](../../docs/developers/SubstraitModifications.md).

---

## 2. Key concepts

| Concept | Description | Code |
|---|---|---|
| `SubstraitContext` | Per-stage builder state: function name→id map, operator id→rel ids map, iterator indexes, join/agg params (used by metrics) | [SubstraitContext.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/substrait/SubstraitContext.scala) |
| `TransformContext` | `(outputAttributes, root: RelNode)` returned by `doTransform` | `execution/TransformContext` |
| `RelNode` / `RelBuilder` | Java wrappers that build Substrait `Rel` protos (`makeFilterRel`, `makeProjectRel`, `makeJoinRel`, `makeReadRelForInputIterator`, ...) | [RelBuilder.java](../../gluten-substrait/src/main/java/org/apache/gluten/substrait/rel/RelBuilder.java) |
| `ExpressionNode` / `ExpressionBuilder` | Wrappers for Substrait expressions (field refs, literals, scalar/aggregate/window functions, casts, if-then, ...) | `gluten-substrait/src/main/java/org/apache/gluten/substrait/expression` |
| `TypeNode` / `TypeBuilder` | Substrait types with nullability | `substrait/type` |
| `PlanNode` / `PlanBuilder` | Whole plan with extensions + root names | `substrait/plan` |
| `LocalFilesNode` / `SplitInfo` | File list for a scan task | [LocalFilesNode.java](../../gluten-substrait/src/main/java/org/apache/gluten/substrait/rel/LocalFilesNode.java) |
| `InputIteratorRelNode` | `ReadRel` whose source is an input iterator (`iterator:<idx>`) | [InputIteratorRelNode.java](../../gluten-substrait/src/main/java/org/apache/gluten/substrait/rel/InputIteratorRelNode.java) |
| `ConverterUtils` | Type conversion, function signature names, column naming | [ConverterUtils.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/expression/ConverterUtils.scala) |
| `SubstraitToVeloxPlanConverter` | Native Rel → Velox `PlanNode` | [SubstraitToVeloxPlan.cc](../../cpp/velox/substrait/SubstraitToVeloxPlan.cc) |
| `SubstraitToVeloxExpr` | Native expression conversion | [SubstraitToVeloxExpr.cc](../../cpp/velox/substrait/SubstraitToVeloxExpr.cc) |
| `SubstraitParser` | Type parsing, function-name mapping | [SubstraitParser.cc](../../cpp/velox/substrait/SubstraitParser.cc) |
| `SubstraitToVeloxPlanValidator` | Validation using the converter | [SubstraitToVeloxPlanValidator.cc](../../cpp/velox/substrait/SubstraitToVeloxPlanValidator.cc) |
| `VeloxPlanConverter` | Wrapper used by the runtime (splits, input iterators, write paths) | `cpp/velox/compute/VeloxPlanConverter.cc` |

---

## 3. Architecture

Where the plan is built and where it is consumed.

**Driver (JVM):**

1. `WholeStageTransformer.doWholeStageTransform` calls `child.transform(SubstraitContext)`.
2. Each transformer's `doTransform` builds its `RelNode`, bottom up.
3. `PlanBuilder.makePlan` adds function extensions and the root, producing plan protobuf bytes.
4. Separately, scan transformers call `getSplitInfos`, producing a `SplitInfo` protobuf for each partition.
5. Plan bytes and split infos are packed into a `GlutenPartition`.

**Executor (native):**

1. JNI `createKernelWithIterator` hands both over.
2. The runtime parses the plan and split infos.
3. `SubstraitToVeloxPlanConverter.toVeloxPlan` builds the Velox `PlanNode` tree, using `SubstraitToVeloxExpr` for expressions.

---

## 4. JVM side: building the plan

### 4.1 Recursive `transform`

`TransformSupport.transform(context)` ([WholeStageTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/WholeStageTransformer.scala#L101-L119)):

```scala
final def transform(context: SubstraitContext): TransformContext = {
  if (TransformerState.underValidationState) doTransform(context)
  else executeQuery { doTransform(context) }   // materialize subqueries first
}
```

A unary transformer transforms its child first, then wraps the child's `RelNode`:

```scala
// FilterExecTransformerBase.doTransform (simplified)
val childCtx = child.asInstanceOf[TransformSupport].transform(context)
val operatorId = context.nextOperatorId(this.nodeName)
val currRel = getRelNode(context, remainingCondition, child.output,
    operatorId, childCtx.root, validation = false)
TransformContext(output, currRel)
```

`getRelNode` converts the Catalyst condition into a Substrait expression with `ExpressionConverter.replaceWithExpressionTransformer(cond, child.output).doTransform(context)` and calls `RelBuilder.makeFilterRel(input, condExpr, context, operatorId)`.

### 4.2 Column references & naming

- Inside a plan, columns are referenced by **position** in the input schema (Substrait `FieldReference` with struct field index). Converters compute the index by binding attributes to `child.output`.
- Output names use `ConverterUtils.genColumnNameWithExprId(attr)` → `l_quantity#16`. These names appear in `RelRoot.names` and in scan schemas.

### 4.3 Functions & signatures

Each function call is registered in the context and referenced by id:

```scala
// "gt:i32_i32"
val funcName = ConverterUtils.makeFuncName("gt", Seq(IntegerType, IntegerType))
val funcId = context.registerFunction(funcName)
ExpressionBuilder.makeScalarFunction(funcId, args, outputTypeNode)
```

Type signature codes (`ConverterUtils.getTypeSigName`): `bool, i8, i16, i32, i64, fp32, fp64, date, ts, ts_ntz, str, vbin, dec<p,s>, list<...>, struct<...>, map<...>`. Prefixes `:req_` / `:opt_` mark required/optional configs on some functions.

`PlanBuilder.makePlan` writes every registered function into the plan's `extensions` as `extension_function { function_anchor: id, name: "gt:i32_i32" }`.

### 4.4 Stage inputs: input iterators

`InputIteratorTransformer.doTransform` → `RelBuilder.makeReadRelForInputIterator(output, context, operatorId)` → `InputIteratorRelNode` which encodes:

```text
ReadRel {
  base_schema { names: ["k#5", "sum#20"], struct { types: [...] } }
  local_files { items { uri_file: "iterator:0" } }
}
```

At runtime, the corresponding Spark RDD's iterator (shuffle reader, broadcast relation, upstream fallback operator...) is passed to native code in the same order (`ColumnarBatchInIterator[]`).

### 4.5 Scans and splits

`FileSourceScanExecTransformer` (a `LeafTransformSupport`) builds a `ReadRel` with the read schema, filter, and file format, but **not** the files. File lists differ per task, so they're built separately:

- `getSplitInfos` → `VeloxIteratorApi.genSplitInfo(partitionIndex, partitions, partitionSchema, dataSchema, fileFormat, ...)` → `LocalFilesBuilder.makeLocalFiles(paths, starts, lengths, fileSizes, modificationTimes, partitionColumns, metadataColumns, format, preferredLocations, properties, ...)`.
- `VeloxIteratorApi.genPartitions` serializes the plan **once** (`wsCtx.root.toProtobuf.toByteArray`) and creates one `GlutenPartition(index, planBytes, splitInfos)` per task.

More in [09 · Scan](#doc-09).

### 4.6 Bookkeeping for metrics

`SubstraitContext.registerRelToOperator(operatorId)` records which Rel ids belong to which Spark operator (one Spark operator can produce several Rels, e.g. a join with pre/post projections). `JoinParams` and `AggregationParams` remember shape details. After execution, Velox operator metrics are mapped back using these maps (`metricsUpdatingFunction(child, registeredRelMap, registeredJoinParams, registeredAggregationParams)`). See [13](#doc-13).

---

## 5. Native side: Substrait → Velox

### 5.1 Entry

```cpp
// VeloxRuntime::createResultIterator (simplified)
VeloxPlanConverter veloxPlanConverter(leafPool, veloxCfg, inputs,
    connectorIds, writeTempPath, writeFileName);
veloxPlan_ = veloxPlanConverter.toVeloxPlan(substraitPlan_,
    std::move(localFiles_));
// separate leaf nodes into scans (with SplitInfo) and streams (iterators), then
// build WholeStageResultIterator
```

### 5.2 Dispatch

`SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::Plan&)`:
1. `constructFunctionMap(plan)` — reads extensions into `functionMap_` (anchor → signature name).
2. Converts the single `RelRoot`/`Rel` recursively by type:

| Substrait Rel | Velox node(s) |
|---|---|
| `ReadRel` (files) | `TableScanNode` with Hive (or Delta/Iceberg) connector table handle, subfield filters, assignments |
| `ReadRel` (`iterator:N`) | `TableScanNode` with the **iterator (ValueStream) connector**; splits are fed from Java iterators |
| `ReadRel` (virtual table) | `ValuesNode` |
| `FilterRel` | `FilterNode` |
| `ProjectRel` | `ProjectNode` |
| `AggregateRel` | `AggregationNode` (step from Substrait phase: partial/intermediate/final/single) |
| `JoinRel` | `HashJoinNode` / `MergeJoinNode` (by join options) |
| `CrossRel` | `NestedLoopJoinNode` |
| `SortRel` | `OrderByNode` |
| `FetchRel` | `LimitNode` |
| `TopNRel` | `TopNNode` |
| `WindowRel` | `WindowNode` |
| `WindowGroupLimitRel` | `TopNRowNumberNode` / `RowNumberNode` |
| `ExpandRel` | `ExpandNode` |
| `GenerateRel` | `UnnestNode` (explode/posexplode/inline...) |
| `SetRel` | union (`LocalPartitionNode` gather) |
| `WriteRel` | `TableWriteNode` |

Example (`FilterRel`):

```cpp
core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(
    const ::substrait::FilterRel& filterRel) {
  auto childNode = convertSingleInput<::substrait::FilterRel>(filterRel);
  auto filterNode = std::make_shared<core::FilterNode>(
      nextPlanNodeId(), exprConverter_->toVeloxExpr(filterRel.condition(),
          childNode->outputType()), childNode);
  return filterRel.has_common() ? processEmit(filterRel.common(),
      std::move(filterNode)) : filterNode;
}
```

Plan node ids are sequential strings (`"0"`, `"1"`, ...); Gluten keeps the ordered list to map metrics back.

### 5.3 Input iterators → ValueStream connector

```cpp
// constructValueStreamNode (simplified)
auto nodeId = ValueStreamConnectorFactory::nodeIdOf(streamIdx);
auto tableHandle =
    std::make_shared<ValueStreamTableHandle>(connectorIds_.iterator,
        dynamicFilterEnabled);
// columns named node_<id>_<i>, assignments of ValueStreamColumnHandle
auto tableScanNode = std::make_shared<core::TableScanNode>(nodeId,
    outputType, tableHandle, assignments);
splitInfo->leafType = SplitInfo::LeafType::SPLIT_AWARE_STREAM;
```

So **upstream batches enter Velox as splits of a special connector**. `WholeStageResultIterator::addIteratorSplits` adds one split per Java input iterator. A config (`kValueStreamDynamicFilterEnabled`) allows Velox dynamic filters (e.g. from a hash join build) to be pushed onto the stream; it's disabled automatically when the probe side has non-deterministic expressions (see `hasNonDeterministicExprInJoinProbe`).

### 5.4 Expressions & function names

`SubstraitToVeloxExpr::toVeloxExpr` converts:
- field references → `FieldAccessTypedExpr`,
- literals → `ConstantTypedExpr`,
- scalar functions → `CallTypedExpr(name, args, type)` where `name = SubstraitParser::findVeloxFunction(functionMap_, anchor)`,
- casts, if-then, singular-or-list (IN), etc.

`findVeloxFunction` strips the signature (`gt:i32_i32` → `gt`) and maps Substrait names to Velox's Spark-SQL function names when they differ:

```cpp
{"is_not_null", "isnotnull"}, {"equal", "equalto"}, {"lt", "lessthan"},
    {"gte", "greaterthanorequal"},
{"strpos", "instr"}, {"named_struct", "row_constructor"}, {"murmur3hash",
    "hash_with_seed"},
{"xxhash64", "xxhash64_with_seed"}, {"modulus", "remainder"}, {"negative",
    "unaryminus"}, ...
```

Velox resolves the final name against its function registry (Spark SQL functions registered in `registerAllFunctions()` at backend init). More in [11](#doc-11).

### 5.5 Types

`SubstraitParser::parseType` maps Substrait types to Velox types (`i32→INTEGER`, `string→VARCHAR`, `decimal(p,s)→DECIMAL`, `date→DATE`, `timestamp→TIMESTAMP`, lists/maps/structs recursively). Field names are lower-cased unless `spark.sql.caseSensitive=true`.

### 5.6 Validation mode

`SubstraitToVeloxPlanValidator` reuses the converter with `validationMode_ = true` (no split infos needed) and adds rule checks per Rel. For expressions it actually **compiles** them:

```cpp
std::vector<core::TypedExprPtr> expressions{exprConverter_->toVeloxExpr(
    expression, inputType)};
// throws on unknown function/signature
exec::ExprSet exprSet(std::move(expressions), execCtx_.get());
```

Any `VeloxException` is caught, its message recorded, and validation fails → the node falls back ([02](#doc-02)).

---

## 6. End-to-end worked example

Query:

```sql
SELECT k, sum(v * 2) AS s
FROM parquet.`/tmp/t`
WHERE v > 10
GROUP BY k
```

### 6.1 Gluten physical plan (first stage, illustrative)

```text
WholeStageCodegenTransformer (1)
+- ^ ProjectExecTransformer [k, sum, count...]
   +- ^ FlushableHashAggregateExecTransformer(keys=[k],
            functions=[partial_sum(_pre_0)])
      # PullOutPreProject
      +- ^ ProjectExecTransformer [k, (v * 2) AS _pre_0]
         +- ^ FilterExecTransformer (isnotnull(v) AND (v > 10))
            +- ^ FileSourceScanExecTransformer parquet [k, v]
```

### 6.2 Substrait (abridged, illustrative JSON from `substraitPlanJson`)

```json
{
  "extensions": [
    {"extensionFunction": {"functionAnchor": 0, "name": "is_not_null:i64"}},
    {"extensionFunction": {"functionAnchor": 1, "name": "gt:i64_i64"}},
    {"extensionFunction": {"functionAnchor": 2, "name": "and:bool_bool"}},
    {"extensionFunction": {"functionAnchor": 3, "name": "multiply:i64_i64"}},
    {"extensionFunction": {"functionAnchor": 4, "name": "sum:i64"}}
  ],
  "relations": [{"root": {"input": {
    "aggregate": {
      "input": {"project": {
        "input": {"filter": {
          "input": {"read": {"baseSchema": {"names": ["k", "v"]},
              "localFiles": {}}},
          "condition": {"scalarFunction": {"functionReference": 2,
              "...": "..."}}
        }},
        "expressions": [{"scalarFunction": {"functionReference": 3,
            "...": "..."}}]
      }},
      "groupings": [{"groupingExpressions": [{"selection": {"directReference": {"structField": {}}}}]}],
      "measures": [{"measure": {"functionReference": 4,
          "phase": "AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE"}}]
    }},
    "names": ["k#0", "sum#12"]
  }}]
}
```

Note the empty `localFiles` in the plan: the files come per task from `SplitInfo`:

```text
LocalFiles { items: [ {uri_file: "file:/tmp/t/part-00000.parquet", start: 0,
    length: 1048576,
                        parquet: {}, partition_columns: []} ] }
```

### 6.3 Velox plan (illustrative output of `injectNativePlanStringToExplain`)

```text
-- Project[4][expressions: (n4_0:BIGINT, "n3_0"), (n4_1:BIGINT, "n3_1")]
  -- Aggregation[3][PARTIAL [n2_0] n3_1 := sum("n2_1")]
    -- Project[2][expressions: (n2_0:BIGINT, "n0_0"), (n2_1:BIGINT,
           multiply("n0_1", 2))]
      -- Filter[1][expression: and(isnotnull("n0_1"), greaterthan("n0_1", 10))]
        -- TableScan[0][table: hive_table, remaining filter: ...]
```

Notice the name mapping in action: `is_not_null` → `isnotnull`, `gt` → `greaterthan`. In practice the filter is often pushed into the scan (`PushDownFilterToScan` + subfield filters), so the `Filter` node may disappear.

**Try it / verify**

- `spark.conf.set("spark.gluten.sql.injectNativePlanStringToExplain", "true")`, then `df.explain("formatted")`: each native stage prints its Velox plan. Look for the same operators (`TableScan`, `Filter`/remaining filter, `Aggregation`) as in §6.3.
- Print the Substrait JSON of each stage with the snippet in §7 and check function names such as `gt:i64_i64` in `extensions`.

---

## 7. Debugging the plan contract

| Goal | How |
|---|---|
| See the Velox plan in EXPLAIN | `spark.conf.set("spark.gluten.sql.injectNativePlanStringToExplain", "true")`; `df.explain("formatted")` |
| Get Substrait JSON for a stage | In a Scala shell: find `WholeStageTransformer` nodes in `df.queryExecution.executedPlan` and call `.substraitPlanJson` |
| Log plan generation time / plans | `spark.gluten.sql.substrait.plan.logLevel` (default `DEBUG`) controls the "generating the substrait plan took" log |
| Print Velox plan per task | `spark.gluten.sql.debug=true` → `VeloxRuntime` logs the session config and the Velox plan (`veloxPlan_->toString(true, true)`) |
| Dump a task's plan, splits & input data for offline replay | `spark.gluten.sql.benchmark_task.stageId` / `.partitionId` / `.taskId` + `spark.gluten.saveDir=/path`; replay with the C++ `GenericBenchmark` ([docs/developers/MicroBenchmarks.md](../../docs/developers/MicroBenchmarks.md)) |
| Query trace (Velox) | [docs/developers/QueryTrace.md](../../docs/developers/QueryTrace.md) |

```scala
import org.apache.gluten.execution.WholeStageTransformer
df.queryExecution.executedPlan.collect { case w: WholeStageTransformer => w }
  .foreach(w => println(s"== stage ${w.stageId}\n${w.substraitPlanJson}"))
```

> **Note:** With AQE, call this after the query ran (or on `df.queryExecution.executedPlan` after an action), since the final plan with transformers only exists after adaptive planning.

---

## 8. Configuration reference

| Key | Default | Effect |
|---|---|---|
| `spark.gluten.sql.injectNativePlanStringToExplain` | `false` | Append Velox plan to EXPLAIN per stage |
| `spark.gluten.sql.substrait.plan.logLevel` | `DEBUG` | Log level for Substrait plan generation messages |
| `spark.gluten.sql.debug` | `false` | Native debug mode (logs config and Velox plans, verbose glog) |
| `spark.gluten.sql.cacheWholeStageTransformerContext` | `false` | Cache generated Substrait context per stage |
| `spark.gluten.sql.enable.native.validation` | `true` (internal) | Use native validator during planning |
| `spark.gluten.sql.benchmark_task.stageId` / `.partitionId` / `.taskId` | `-1` / `""` / `""` (internal) | Select task(s) to dump |
| `spark.gluten.saveDir` | `""` (internal) | Where dumps are written (required when dumping) |
| `spark.sql.caseSensitive` | `false` | Native lowercases field names when false |

---

## 9. Pitfalls & FAQ

- **"Plan must have readRel and related split info"** — a scan in the plan with no split info supplied. Usually a planning bug (e.g. a leaf transformer not collected by `findAllLeafTransformers`).
- **Name mismatch problems with case-sensitive schemas** — check `spark.sql.caseSensitive`; native lower-cases names otherwise.
- **Deep plans hitting protobuf recursion limits** — Gluten raises the limit at executor init (`CodedInputStreamClassInitializer`).
- **Function exists in Velox but validation fails** — usually a **signature** mismatch (argument types after Spark coercion, e.g. decimal precision) rather than a missing function. Enable `printStackOnFailure` and read the native message.
- **Why are there extra projects?** Native operators only evaluate expressions in projects; `PullOutPreProject/PostProject` creates them, and `CollapseProjectExecTransformer` merges adjacent ones.

---

## 10. Code map

| File | Role |
|---|---|
| [algebra.proto](../../gluten-substrait/src/main/resources/substrait/proto/substrait/algebra.proto) | Modified Substrait relations/expressions |
| [SubstraitContext.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/substrait/SubstraitContext.scala) | Builder state |
| [RelBuilder.java](../../gluten-substrait/src/main/java/org/apache/gluten/substrait/rel/RelBuilder.java) | Rel construction |
| [InputIteratorRelNode.java](../../gluten-substrait/src/main/java/org/apache/gluten/substrait/rel/InputIteratorRelNode.java) | Iterator inputs |
| [LocalFilesNode.java](../../gluten-substrait/src/main/java/org/apache/gluten/substrait/rel/LocalFilesNode.java) | Split description |
| [ConverterUtils.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/expression/ConverterUtils.scala) | Types, names, signatures |
| [WholeStageTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/WholeStageTransformer.scala) | Plan generation entry, debug helpers |
| [VeloxIteratorApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxIteratorApi.scala) | SplitInfo & partitions |
| [VeloxRuntime.cc](../../cpp/velox/compute/VeloxRuntime.cc) | Plan parsing, conversion entry |
| [SubstraitToVeloxPlan.cc](../../cpp/velox/substrait/SubstraitToVeloxPlan.cc) | Rel conversion |
| [SubstraitToVeloxExpr.cc](../../cpp/velox/substrait/SubstraitToVeloxExpr.cc) | Expression conversion |
| [SubstraitParser.cc](../../cpp/velox/substrait/SubstraitParser.cc) | Types & function mapping |
| [SubstraitToVeloxPlanValidator.cc](../../cpp/velox/substrait/SubstraitToVeloxPlanValidator.cc) | Validation |

## 11. Related docs

- [02 · Planning](#doc-02) · [04 · Native runtime](#doc-04) · [09 · Scan](#doc-09) · [11 · Expressions](#doc-11)
- Repo: [SubstraitModifications.md](../../docs/developers/SubstraitModifications.md), [MicroBenchmarks.md](../../docs/developers/MicroBenchmarks.md), [QueryTrace.md](../../docs/developers/QueryTrace.md)

---

<a id="doc-04"></a>

# 04 · Native Execution Runtime & JNI

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [03 · Substrait](#doc-03) · Next: [05 · Batches & transitions](#doc-05)

---

## TL;DR

- A native stage runs **inside an ordinary Spark task**, on the task's own thread. Spark still gives you the parallelism (one task per core).
- The JVM hands the plan to C++ over **JNI** and then just pulls batches. Data stays in C++; the JVM only holds numeric handles to it.
- C++ errors come back as Java exceptions, and every native resource is freed automatically when the task ends.

**Technical summary**

- A native stage executes inside a **normal Spark task**. Spark calls `RDD.compute`; Gluten's RDD (`GlutenWholeStageColumnarRDD` or `WholeStageZippedPartitionsRDD`) asks `VeloxIteratorApi` to build a **native iterator**.
- `NativePlanEvaluator` gets the task's **`Runtime`** and calls JNI `nativeCreateKernelWithIterator(plan, splits, inputIterators, ...)`. C++ parses the Substrait plan, converts it to a Velox plan, and creates a **`WholeStageResultIterator`** wrapping a **Velox `TaskCursor`**.
- Velox runs **serially on the Spark task thread** (`serialExecution = true`, `maxDrivers = 1`): each `next()` pulls one `RowVector` through the pipeline. Spark tasks provide the parallelism.
- Batches cross JNI as **opaque `long` handles** into a per-runtime **`ObjectStore`**; data is never copied into the JVM. Upstream JVM iterators are pulled from C++ by calling back into Java (`ColumnarBatchInIterator.next()` returns a handle).
- Every JNI entry point is wrapped in `JNI_METHOD_START/END`, converting C++ exceptions into Java `GlutenException`.
- Task-scoped native resources (runtime, memory manager, iterators) are owned by **`TaskResources`** and released on task completion/failure. Metrics are fetched once when the iterator finishes and mapped to SQL metrics.

---

## 1. Why this design

| Question | Answer |
|---|---|
| Why not run Velox with its own thread pool? | Spark already schedules tasks across cores; running Velox serially per task avoids oversubscription and keeps Spark's task-level memory accounting, cancellation and fault tolerance semantics. |
| Why handles instead of copying batches? | Copying columnar data JVM↔native per batch would erase most of the speedup. Handles keep data native; the JVM only orchestrates. |
| Why a pull model with callbacks into Java? | Fits Spark's iterator model and lets native pipelines consume any Spark RDD (shuffle readers, broadcast, fallback operators) without materializing it. |
| Why `TaskResources`? | JVM GC can't free native memory; resources must be closed deterministically, including on failures and cancellations. |

---

## 2. Key concepts

- `GlutenWholeStageColumnarRDD` (JVM): RDD for stages **with** leaf scans (partitions carry plan + splits) Code: [GlutenWholeStageColumnarRDD.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/GlutenWholeStageColumnarRDD.scala)
- `WholeStageZippedPartitionsRDD` (JVM): RDD for stages **without** leaf scans; zips input RDD partitions. Code: [WholeStageZippedPartitionsRDD.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/WholeStageZippedPartitionsRDD.scala)
- `VeloxIteratorApi` (JVM): `genFirstStageIterator` / `genFinalStageIterator` Code: [VeloxIteratorApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxIteratorApi.scala)
- `NativePlanEvaluator` (JVM): Creates native kernels; registers the plan **spiller**. Code: [NativePlanEvaluator.java](../../gluten-arrow/src/main/java/org/apache/gluten/vectorized/NativePlanEvaluator.java)
- `ColumnarBatchOutIterator` (JVM): Java iterator over native output (`nativeHasNext`, `nativeNext`, `nativeSpill`, `nativeClose`) Code: [ColumnarBatchOutIterator.java](../../gluten-arrow/src/main/java/org/apache/gluten/vectorized/ColumnarBatchOutIterator.java)
- `ColumnarBatchInIterator` (JVM): Adapter that native code calls to pull upstream batches. Code: [ColumnarBatchInIterator.java](../../gluten-arrow/src/main/java/org/apache/gluten/vectorized/ColumnarBatchInIterator.java)
- `Runtime` / `VeloxRuntime` (both): Per-task native context: plan, splits, memory manager, Velox config, object store. Code: [Runtime.scala](../../gluten-arrow/src/main/scala/org/apache/gluten/runtime/Runtime.scala), [VeloxRuntime.cc](../../cpp/velox/compute/VeloxRuntime.cc)
- `ResultIterator` (C++): Generic iterator wrapper used by JNI. Code: `cpp/core/compute/ResultIterator.h`
- `WholeStageResultIterator` (C++): Owns the Velox `TaskCursor`, adds splits, collects metrics, spills. Code: [WholeStageResultIterator.cc](../../cpp/velox/compute/WholeStageResultIterator.cc)
- `JniColumnarBatchIterator` (C++): Calls back into `ColumnarBatchInIterator` Code: [JniCommon.h](../../cpp/core/jni/JniCommon.h), [JniCommon.cc](../../cpp/core/jni/JniCommon.cc)
- `ObjectStore` (C++): Handle → `shared_ptr` registry per runtime. Code: [ObjectStore.h](../../cpp/core/utils/ObjectStore.h)
- `JniWrapper.cc` (C++): Most JNI entry points. Code: [JniWrapper.cc](../../cpp/core/jni/JniWrapper.cc)
- `TaskResources` (JVM): Task-scoped resource registry. Code: [TaskResources.scala](../../gluten-core/src/main/scala/org/apache/spark/task/TaskResources.scala)
- `Iterators` (JVM): Wrapper builder: cleanup, timing, interruption, invocation protection. Code: [Iterators.scala](../../gluten-core/src/main/scala/org/apache/gluten/iterator/Iterators.scala)

---

## 3. Architecture

Everything below runs on the Spark task thread. Data flows in two directions.

**Creating the native iterator (JVM → C++):**

1. `GlutenWholeStageColumnarRDD.compute(partition)` calls `VeloxIteratorApi.genFirstStageIterator`.
2. That creates a `NativePlanEvaluator` (via `Runtimes.contextInstance`) and calls `createKernelWithBatchIterator(plan, splits, inIters, spillDir)`.
3. JNI `nativeCreateKernelWithIterator` reaches C++. `VeloxRuntime` parses the plan and splits.
4. `createResultIterator` builds the Velox plan and a `WholeStageResultIterator` around a Velox `TaskCursor` (serial, one driver).
5. The JVM gets back a `ColumnarBatchOutIterator`.

**Output (C++ → JVM):** the JVM pulls results with `nativeHasNext` / `nativeNext`. Each call drives the Velox operators (TableScan, Filter, Project, Agg, Join, …) one step.

**Input (JVM → C++):** when a stage reads from shuffle, broadcast or a fallen-back operator, its Velox ValueStream connector calls back into Java (`ColumnarBatchInIterator`) to get the next batch handle.

---

## 4. Detailed walkthrough

### 4.1 From `executeColumnar` to an RDD

`WholeStageTransformer.doExecuteColumnar` (driver):
1. Generates the Substrait plan (`doWholeStageTransform`), logging the time.
2. Collects `columnarInputRDDs` from non-leaf inputs (anything below `InputIteratorTransformer`).
3. If the stage has leaf scans → `GlutenWholeStageColumnarRDD(partitions with plan + splits, inputRDDs, ...)`; else → `WholeStageZippedPartitionsRDD(inputRDDs, plan, ...)`.
4. Passes `metricsUpdatingFunction(...)` built from the Substrait context maps, so native metrics can be attributed to operators later.

`GlutenWholeStageColumnarRDD.compute(split, context)`:

```scala
val (inputPartition, inputColumnarRDDPartitions) = castNativePartition(split)
// upstream iterators
val inputIterators = rdds.getIterators(inputColumnarRDDPartitions, context)
BackendsApiManager.getIteratorApiInstance.genFirstStageIterator(
  inputPartition, context, pipelineTime, updateInputMetrics,
      updateNativeMetrics, split.index, inputIterators, enableCudf)
```

Preferred locations come from split infos (enables locality / soft affinity).

### 4.2 Creating the native kernel

`VeloxIteratorApi.genFirstStageIterator` ([source](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxIteratorApi.scala#L185-L240)):

```scala
val columnarNativeIterators = inputIterators.map(iter =>
    new ColumnarBatchInIterator(backendName, iter.asJava))
// gets/creates the task Runtime
val transKernel = NativePlanEvaluator.create(backendName, extraConf)
val splitInfoByteArray = inputPartition.splitInfos.map(_.toProtobuf.toByteArray)
val spillDirPath =
    SparkDirectoryUtil.get().namespace("gluten-spill").mkChildDirRoundRobin(
        UUID...).getAbsolutePath
val resIter = transKernel.createKernelWithBatchIterator(plan,
    splitInfoByteArray, columnarNativeIterators, partitionIndex, spillDirPath)
resIter.noMoreSplits()
Iterators.wrap(resIter.asScala)
  // safe repeated hasNext after close
  .protectInvocationFlow()
  .recycleIterator { updateNativeMetrics(itrMetrics.fetch(resIter));
      updateInputMetrics(...); resIter.close() }
  // close each batch after consumer is done
  .recyclePayload(batch => batch.close())
  .collectLifeMillis(millis => pipelineTime += millis)
  .asInterruptible(context)                                 // honor task kill
  .create()
```

`NativePlanEvaluator.createKernelWithBatchIterator` calls `PlanEvaluatorJniWrapper.nativeCreateKernelWithIterator(plan, splits, iters, stageId, partitionIndex, taskAttemptId, dumpingEnabled, spillDir)` and registers a **spiller** on the runtime's memory manager that calls `out.spill(size)` in the SPILL phase ([07 · Spill](#doc-07)).

### 4.3 JNI: `nativeCreateKernelWithIterator`

From [JniWrapper.cc](../../cpp/core/jni/JniWrapper.cc#L515-L569):

```cpp
// VeloxRuntime for this task
auto ctx = getRuntime(env, wrapper);
ctx->setSparkTaskInfo({stageId, partitionId, taskId});
if (enableDumping) ctx->enableDumping();
ctx->parsePlan(planBytes, planSize);
for (i in splitInfos) ctx->parseSplitInfo(splitBytes, size, i);
// wrap each Java ColumnarBatchInIterator
for (idx in batchItrArray)
  inputIters.push_back(std::make_shared<ResultIterator>(
      std::make_unique<JniColumnarBatchIterator>(env, iter, ctx, idx)));
// handle
return ctx->saveObject(ctx->createResultIterator(spillDir, inputIters));
```

`VeloxRuntime::createResultIterator` ([source](../../cpp/velox/compute/VeloxRuntime.cc#L428-L473)) converts Substrait → Velox plan, separates scan leaves (with `SplitInfo`) from stream leaves (input iterators), constructs `WholeStageResultIterator`, and adds remaining input iterators as **iterator splits**.

### 4.4 `WholeStageResultIterator`: running Velox

Constructor ([source](../../cpp/velox/compute/WholeStageResultIterator.cc#L111-L200)):

```cpp
facebook::velox::exec::CursorParameters params;
params.planNode = planNode;
params.maxDrivers = 1;
// QueryConfig from session conf, connector configs, AsyncDataCache, task
// aggregate memory pool, spill executor
params.queryCtx = createNewVeloxQueryCtx();
params.executionStrategy = velox::core::ExecutionStrategy::kUngrouped;
params.spillDirectory = spillDir;
params.serialExecution = true;                   // run on the caller thread
// hand out vectors without copying
params.copyResult = false;
params.outputPool = memoryManager_->getLeafMemoryPool();
cursor_ = velox::exec::TaskCursor::create(params);
task_ = cursor_->task().get();
// then build ConnectorSplits for every scan from SplitInfo (Hive/Iceberg/Delta
// splits, partition keys, metadata cols)
```

- The Velox `QueryCtx` is named `Gluten_Stage_<stage>_TID_<task>_VTID_<n>` — useful when reading Velox logs.
- `noMoreSplits()` adds all file splits to the scan nodes and calls `task_->noMoreSplits(...)` for scans and streams.

`next()`:

```cpp
std::shared_ptr<ColumnarBatch> WholeStageResultIterator::next() {
  while (true) {
    if (!cursor_->moveNext()) return nullptr;            // end of stream
    RowVectorPtr vector = cursor_->current();
    if (vector->size() == 0) continue;                   // skip empty batches
    // materialize lazy vectors (timed)
    for (auto& child : vector->children()) child->loadedVector();
    return std::make_shared<VeloxColumnarBatch>(vector);
  }
}
```

Each `moveNext()` drives the single driver until the pipeline produces an output vector (or finishes). Operators like hash join build or aggregation accumulate internally before emitting.

### 4.5 Pulling upstream batches

When a ValueStream (iterator) table scan needs input, Velox asks the iterator split's source, which ends in `JniColumnarBatchIterator::nextInternal`:

```cpp
attachCurrentThreadAsDaemonOrThrow(vm_, &env);
// calls ColumnarBatchInIterator.hasNext()
if (!env->CallBooleanMethod(jColumnarBatchItr_, hasNext)) return nullptr;
// returns native handle of the batch
jlong handle = env->CallLongMethod(jColumnarBatchItr_, next);
return ObjectStore::retrieve<ColumnarBatch>(handle);
```

On the Java side, `ColumnarBatchInIterator.next()` checks the batch is **offloaded** (native-backed) and returns its handle. If the upstream produced an Arrow/Java batch, it must be offloaded first (see [05](#doc-05)).

### 4.6 Returning output batches

`ColumnarBatchOutIterator.next0()` → JNI `nativeNext`:

```cpp
auto iter = ObjectStore::retrieve<ResultIterator>(iterHandle);
// -1 => Java returns null
if (!iter->hasNext()) return kInvalidObjectHandle;
auto batch = iter->next();
return ctx->saveObject(batch);                           // new handle
```

Java wraps the handle with `ColumnarBatches.create(handle)` into a Spark `ColumnarBatch` whose columns are placeholder vectors pointing to native memory. Downstream Gluten operators pass the handle on; a `VeloxColumnarToRow` converts it to rows when needed.

### 4.7 Handles and lifetime (`ObjectStore`)

- Each runtime owns an `ObjectStore`. `save(shared_ptr<T>)` returns a 64-bit handle encoding store id + resource id; `retrieve<T>(handle)` looks it up; `release(handle)` drops the reference.
- Java `ColumnarBatch.close()` → releases the batch handle (`recyclePayload` in the iterator wrapper ensures this happens).
- `ColumnarBatchOutIterator.close()` → `nativeClose` → releases the result iterator (and with it the Velox task).
- When the task ends, `TaskResources` releases the `Runtime` (priority `30`) and memory managers; the object store reports leaked objects if any remain.

### 4.8 Errors

```cpp
#define JNI_METHOD_START try {
#define JNI_METHOD_END(fallback_expr) \
  } catch (std::exception& e) { env->ThrowNew(
      gluten::getJniErrorState()->glutenExceptionClass(), e.what());
          return fallback_expr; }
```

So a `VeloxUserError`/`VeloxRuntimeError` surfaces in Spark as `org.apache.gluten.exception.GlutenException: <velox message>` in the task failure. Velox stack traces in messages are controlled by `kEnableUserExceptionStacktrace` / `kEnableSystemExceptionStacktrace` backend configs.

Java exceptions thrown while native code calls back into Java (e.g. a shuffle fetch failure inside `ColumnarBatchInIterator.next()`) are checked with `checkException(env)` and rethrown as C++ exceptions, then converted back at the outer JNI boundary — preserving Spark semantics like `FetchFailedException` handling as far as possible.

### 4.9 Threads

- **Task thread**: all Velox driver work (`serialExecution`).
- **IO executor** (`ioExecutor_`, size `kVeloxIOThreads`, default = task slots): split preloading / async reads for table scans.
- **Spill executor** (`spillExecutor_`, `spillThreadNum`, default `0` = no dedicated threads).
- **Native threads calling Java** must be attached to the JVM (`attachCurrentThreadAsDaemonOrThrow`); `NativeThreadManager` + `TaskChildThreadInitializer` propagate the Spark `TaskContext` to child threads so memory accounting and task-local state work.

### 4.10 Metrics

On iterator completion (`recycleIterator`), `IteratorMetricsJniWrapper.fetch(resIter)` calls `nativeFetchMetrics`, which returns per-Velox-operator arrays (input/output rows & bytes, CPU and wall time, peak memory, spill bytes/rows/files, dynamic filter stats, scan IO stats, write stats, lazy-load time, ...). The `metricsUpdatingFunction` maps them onto each transformer's `SQLMetric`s using the operator→rel id maps. Details: [13](#doc-13).

---

## 5. End-to-end worked example: one task of a scan + aggregate stage

One task of a scan + aggregate stage (partition 7), step by step.

1. The Spark executor calls `compute(partition 7)` on `GlutenWholeStageColumnarRDD`.
2. The RDD calls `VeloxIteratorApi.genFirstStageIterator(plan, splits)`.
3. `NativePlanEvaluator.create(velox)` asks `TaskResources` for this task's `Runtime`, creating it (memory manager + native runtime) on first use.
4. `createKernelWithBatchIterator` → JNI `nativeCreateKernelWithIterator` → C++ parses the plan and splits and creates the result iterator.
5. C++ returns an iterator handle. The JVM wraps it in a `ColumnarBatchOutIterator` and registers a spiller.
6. Until the input is exhausted:
   - the downstream consumer (shuffle writer or C2R) calls `nativeHasNext` / `nativeNext`;
   - C++ calls `cursor.moveNext()`, stores the resulting `VeloxColumnarBatch`, and returns its handle;
   - the consumer uses the batch, then calls `batch.close()`.
7. On close, the JVM fetches metrics and calls `nativeClose`.
8. When the task ends, `TaskResources` releases the `Runtime` and its memory manager.

What you can observe:
- Spark UI → SQL tab: per-operator native metrics on the transformer nodes (e.g. `time of scan`, `peak memory bytes`, `number of output rows`).
- With `spark.gluten.sql.debug=true`: executor logs print the Velox plan per task (illustrative):

```text
############### Velox plan for task Stage 3 TID 57 ... ###############
-- Aggregation[3][PARTIAL ...] -> ...
```

**Try it / verify**

- Set `spark.gluten.sql.debug=true` and run a scan + aggregate. Each task's executor log prints its Velox plan (the `Velox plan for task ...` block above).
- In the SQL UI, the transformer nodes show native metrics such as `time of scan` and `peak memory bytes`.
- Take a thread dump (Executors tab) while the query runs. The task thread's stack shows a native frame under `ColumnarBatchOutIterator.nativeHasNext`/`nativeNext`: Velox runs on the Spark task thread.

---

## 6. Configuration reference

| Key | Default | Effect |
|---|---|---|
| `spark.gluten.sql.debug` | `false` | Native debug mode: log session config, Velox plans; verbose glog |
| `spark.gluten.sql.columnar.maxBatchSize` | `4096` | Target rows per batch for native operators |
| `spark.gluten.sql.columnar.backend.velox.IOThreads` | task slots | Native IO thread pool size |
| `spark.gluten.sql.columnar.backend.velox.spillThreadNum` | `0` | Dedicated spill threads |
| `spark.gluten.sql.columnar.backend.velox.glogVerboseLevel` | see `VeloxConfig.h` | glog `-v` level (when not in debug mode) |
| `spark.gluten.sql.benchmark_task.*`, `spark.gluten.saveDir` | off | Dump plan/splits/input of selected tasks (see [03](#doc-03)) |
| `spark.gluten.sql.columnar.backend.velox.enableUserExceptionStacktrace` / `enableSystemExceptionStacktrace` | `true` / `true` | Include stack traces in Velox exceptions |

> **Tip:** Velox-related keys are prefixed `spark.gluten.sql.columnar.backend.velox.` (see [docs/velox-configuration.md](../../docs/velox-configuration.md) and `cpp/velox/config/VeloxConfig.h`). Verify exact names there before use.

---

## 7. Pitfalls, debugging & FAQ

| Symptom | Meaning / action |
|---|---|
| `When hasNext() is called on a closed iterator, an exception is thrown...` | Iterator used after close; ensure `protectInvocationFlow()` wrapping (Gluten code) |
| `Task doesn't support single threaded execution: <plan>` | A Velox plan node doesn't support serial mode; report with the plan string |
| `GlutenException: ... VeloxUserError ...` | Native runtime error; read the Velox message (often a semantic/unsupported case) |
| Executor crash (SIGSEGV) | Native bug; collect `hs_err_pid*.log` + core dump; reproduce with dumped task (`benchmark_task`) in `GenericBenchmark` |
| Leaked objects warning at task end | Some batch/iterator wasn't closed; check custom operators |
| Tasks hang on cancellation | Native loops not interruptible between batches; `asInterruptible(context)` checks between `next()` calls |

Breakpoints/logs:
- JVM: `GlutenWholeStageColumnarRDD.compute`, `VeloxIteratorApi.genFirstStageIterator`, `ColumnarBatchOutIterator.next0`.
- C++: `Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_nativeCreateKernelWithIterator`, `VeloxRuntime::createResultIterator`, `WholeStageResultIterator::next`.
- Attaching gdb to an executor: see [docs/developers/NewToGluten.md](../../docs/developers/NewToGluten.md).

---

## 8. Code map

| File | Role |
|---|---|
| [WholeStageTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/WholeStageTransformer.scala) | `doExecuteColumnar` → RDD |
| [GlutenWholeStageColumnarRDD.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/GlutenWholeStageColumnarRDD.scala) | First-stage RDD |
| [VeloxIteratorApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxIteratorApi.scala) | Iterator creation |
| [NativePlanEvaluator.java](../../gluten-arrow/src/main/java/org/apache/gluten/vectorized/NativePlanEvaluator.java) | Kernel creation + spiller |
| [ColumnarBatchOutIterator.java](../../gluten-arrow/src/main/java/org/apache/gluten/vectorized/ColumnarBatchOutIterator.java) | Output iterator |
| [ColumnarBatchInIterator.java](../../gluten-arrow/src/main/java/org/apache/gluten/vectorized/ColumnarBatchInIterator.java) | Input adapter |
| [Iterators.scala](../../gluten-core/src/main/scala/org/apache/gluten/iterator/Iterators.scala) | Iterator wrappers |
| [JniWrapper.cc](../../cpp/core/jni/JniWrapper.cc) | JNI entry points |
| [JniError.h](../../cpp/core/jni/JniError.h) | Exception bridging |
| [JniCommon.cc](../../cpp/core/jni/JniCommon.cc) | `JniColumnarBatchIterator` |
| [ObjectStore.h](../../cpp/core/utils/ObjectStore.h) | Handles |
| [VeloxRuntime.cc](../../cpp/velox/compute/VeloxRuntime.cc) | Runtime |
| [WholeStageResultIterator.cc](../../cpp/velox/compute/WholeStageResultIterator.cc) | Velox task driving |

## 9. Related docs

- [03 · Substrait](#doc-03) — what gets parsed
- [05 · Batches & transitions](#doc-05) — what flows through handles
- [06 · Memory](#doc-06), [07 · Spill](#doc-07) — memory managers & spillers created per runtime
- [13 · Metrics & troubleshooting](#doc-13)

---

<a id="doc-05"></a>

# 05 · Columnar Batches, Conventions & Transitions

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [04 · Native runtime](#doc-04) · Next: [06 · Memory](#doc-06)

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
  if (batch.column(0) instanceof IndicatorVector) {
    // the other columns must be PlaceholderVector
    return BatchType.LIGHT;
  }
  // all columns must be ArrowWritableColumnVector
  return BatchType.HEAVY;
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
  // per node: child convention vs. required convention
  val filled = fillWithTransitions(removed)
  // root requirement (rows, or Velox batch if caching)
  Transitions.enforceReq(filled, convReq)
}
private def applyForNode(node: SparkPlan): SparkPlan = {
  val convReqs = convFunc.conventionReqOf(node)
  val newChildren = node.children.zip(convReqs).map { case (child, req) =>
    val from = convFunc.conventionOf(child)
    if (from.isNone) child
    else Transition.factory.findTransitionOrThrow(from,
        req)(Transition.notFound(node)).apply(child)
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
    // convert next chunk natively
    info = jniWrapper.nativeColumnarToRowConvert(c2rId, batchHandle, rowId)
  }
  val (offset, length) = (info.offsets(rowId - baseLength),
      info.lengths(rowId - baseLength))
  // zero-copy UnsafeRow view
  row.pointTo(null, info.memoryAddress + offset, length)
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
                  # transition #2 (rows -> native)
                  +- RowToVeloxColumnar
                     # vanilla Spark (fell back)
                     +- Project [UDF(name) AS r, id]
                        # transition #1 (native -> rows)
                        +- VeloxColumnarToRow
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
plan.collect { case p if p.nodeName.contains("ColumnarToRow") ||
    p.nodeName.contains("RowToVeloxColumnar") => p.nodeName }
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

- [02 · Planning](#doc-02) (where `InsertTransitions` runs) · [04 · Runtime](#doc-04) (handles) · [08 · Shuffle](#doc-08) (resize batches) · [12 · Operators](#doc-12) (Python UDF, cache)

---

<a id="doc-06"></a>

# 06 · Memory Management

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [05 · Batches & transitions](#doc-05) · Next: [07 · Spill](#doc-07)

---

## TL;DR

- Native memory is invisible to the JVM, so Gluten **reserves every native allocation from Spark's off-heap memory**. Spark then knows the real usage and won't overschedule.
- When Spark has no memory left, it asks Gluten to give some back: first unused reserved memory, then by spilling native operators to disk.
- Some native memory can't be tracked. It must fit in `spark.executor.memoryOverhead`; if the container gets killed, raise that first.

**Technical summary**

- Native (C++) memory is invisible to the JVM.
  - If Gluten didn't account for it, Spark would happily schedule more work than the machine can hold and executors would get OOM-killed.
  - So **every native allocation is reserved against Spark's off-heap execution memory** (`spark.memory.offHeap.size`) through Spark's `TaskMemoryManager`.
- On the JVM side, Gluten builds a **tree of memory targets** under one Spark `MemoryConsumer` (`TreeMemoryConsumer`) per task. Each native memory manager (one per `Runtime`) gets its own node with **spillers** attached.
- On the native side, each runtime has a **`VeloxMemoryManager`** whose Velox memory pools are governed by Gluten's **`ListenableArbitrator`**. When a pool needs to grow, the arbitrator calls an `AllocationListener` → JNI `ReservationListener.reserve(bytes)` → memory target tree → Spark.
- Reservations are made in **blocks** (`spark.gluten.memory.reservationBlockSize`, default 8MB) to avoid JNI calls on every small allocation.
- If Spark can't grant the memory: Spark asks consumers to spill (Gluten **shrinks** then **spills** native operators — see [07](#doc-07)):
  - `RetryOnOomMemoryTarget` retries after spilling the whole task tree;
  - `ThrowOnOomMemoryTarget` GCs, retries with backoff, and finally throws `OutOfMemoryException: Not enough spark off-heap execution memory...` with a detailed memory dump.
- Process-global native allocations (e.g. broadcast build relations) go through **`GlobalOffHeapMemory`**, which borrows Spark **storage** memory.
- Some native memory is **not tracked** (third-party libs, allocator fragmentation, untracked buffers): it must fit in `spark.executor.memoryOverhead`, which Gluten defaults to `max(0.3 × off-heap, 384MB)` if you don't set it.
- Alternative modes: **dynamic off-heap sizing** (share the JVM heap budget; experimental), **memory isolation** (cap per-task native memory), **untracked** (testing only).

---

## 1. Why memory management matters

### The three budgets of an executor

| Budget | Enforced by | Consequence when exceeded |
|---|---|---|
| JVM heap (`spark.executor.memory`) | JVM | `java.lang.OutOfMemoryError` → executor exit |
| Spark memory pools (on-heap/off-heap execution & storage) | Spark `MemoryManager` (bookkeeping) | Consumers asked to spill; `SparkOutOfMemoryError`-style task failure |
| Container/process resident memory (RSS: heap + off-heap + overhead) | YARN/K8s/cgroup | Executor killed (exit 137 / "container killed by YARN for exceeding memory limits") |

Gluten's goal: make native memory **participate in the second budget** so Spark can arbitrate and trigger spills, and keep untracked usage small enough to fit in overhead so the third budget isn't blown.

### Why "off-heap"?

Velox allocates with its own allocator outside the JVM heap. Spark's **off-heap** execution pool is the natural budget for it. That's why `spark.memory.offHeap.enabled=true` with a size is required by default ([01](#doc-01)).

---

## 2. Key concepts

- `MemoryTarget` (JVM): `borrow(size)`, `repay(size)`, `usedBytes()` Code: [MemoryTarget.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/MemoryTarget.java)
- `TreeMemoryTarget` (JVM): Target with children, capacity, and a `Spiller` Code: [TreeMemoryTarget.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/TreeMemoryTarget.java)
- `TreeMemoryConsumer` (JVM): The Spark `MemoryConsumer` at the root of the tree; implements `spill()` by spilling the tree. Code: [TreeMemoryConsumer.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/spark/TreeMemoryConsumer.java)
- `TreeMemoryConsumers.Factory` (JVM): Per-task factory; `legacyRoot()` (unlimited) or `isolatedRoot()` (capped) Code: [TreeMemoryConsumers.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/spark/TreeMemoryConsumers.java)
- `MemoryTargets` (JVM): Factory composing decorators. Code: [MemoryTargets.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/MemoryTargets.java)
- `RetryOnOomMemoryTarget` (JVM): On short grant, spill the whole task tree and retry once. Code: [RetryOnOomMemoryTarget.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/RetryOnOomMemoryTarget.java)
- `ThrowOnOomMemoryTarget` (JVM): GC + retry with backoff; then throw with a memory report. Code: [ThrowOnOomMemoryTarget.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/ThrowOnOomMemoryTarget.java)
- `OverAcquire` (JVM): Probe-acquire extra `ratio × used` memory to make sure headroom exists. Code: [OverAcquire.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/OverAcquire.java)
- `DynamicOffHeapSizingMemoryTarget` (JVM): Quota check against JVM heap when dynamic sizing is on. Code: [DynamicOffHeapSizingMemoryTarget.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/DynamicOffHeapSizingMemoryTarget.java)
- `NativeMemoryManager` (JVM): Per-runtime handle to native memory manager; owns the reservation listener & spillers. Code: [NativeMemoryManager.scala](../../gluten-arrow/src/main/scala/org/apache/gluten/memory/NativeMemoryManager.scala)
- `ReservationListeners` / `ManagedReservationListener` (JVM): Bridge called from native to borrow/repay. Code: [ReservationListeners.java](../../gluten-arrow/src/main/java/org/apache/gluten/memory/listener/ReservationListeners.java), [ManagedReservationListener.java](../../gluten-arrow/src/main/java/org/apache/gluten/memory/listener/ManagedReservationListener.java)
- `GlobalOffHeapMemory` (JVM): Process-wide target using Spark storage memory. Code: [GlobalOffHeapMemory.scala](../../gluten-core/src/main/scala/org/apache/spark/memory/GlobalOffHeapMemory.scala), [GlobalOffHeapMemoryTarget.scala](../../gluten-core/src/main/scala/org/apache/spark/memory/GlobalOffHeapMemoryTarget.scala)
- `AllocationListener` / `BlockAllocationListener` (C++): Listener interface; block-granular reservation. Code: [AllocationListener.h](../../cpp/core/memory/AllocationListener.h)
- `VeloxMemoryManager` (C++): Velox `MemoryManager` + root/leaf pools + Arrow pools. Code: [VeloxMemoryManager.cc](../../cpp/velox/memory/VeloxMemoryManager.cc)
- `ListenableArbitrator` (C++): Velox `MemoryArbitrator` that asks the listener for capacity. Code: same file
- `ArrowMemoryPool` (C++): Arrow pool (used by shuffle, etc.) charging the same listener. Code: [ArrowMemoryPool.cc](../../cpp/core/memory/ArrowMemoryPool.cc)

---

## 3. Architecture

### 3.1 JVM memory-target tree for one task

The JVM-side memory-target tree for one task, from the top:

1. Spark's `TaskMemoryManager` for the task.
2. One `TreeMemoryConsumer`: a Spark `MemoryConsumer` in `OFF_HEAP` mode.
3. A root capacity node: unlimited (legacy root) or capped (isolated root).
4. One child per native runtime, for example:
   - `NativePlanEvaluator-0`, with spillers (shrink, plan spill), plus its `NativePlanEvaluator-0.OverAcquire` sibling;
   - a `ShuffleWriter` runtime node, with spillers (shrink, shuffle evict);
   - a `VeloxColumnarToRow` runtime node.

Each runtime node also reports the native pool usage below it (root, `default_leaf`, operator pools) as virtual child stats.

Each `Runtime` → one `NativeMemoryManager` → one `ReservationListener` → one consumer node (plus an `.OverAcquire` sibling). The decorator chain around a node:

```text
ThrowOnOom( OverAcquire( [DynamicOffHeapSizing]( RetryOnOom( TreeNode ) ),
    [DynamicOffHeapSizing]( OverAcquireNode ), ratio ) )
```

(`RetryOnOom` is added by `MemoryTargets.newConsumer` only in legacy mode with more than one task slot; `DynamicOffHeapSizing` only if enabled.)

### 3.2 Native side for one runtime

The native side, from the JNI boundary down:

1. The Java `ReservationListener` is called through the C++ `AllocationListener`.
2. One branch: a `BlockAllocationListener` (8MB blocks) for the Arrow memory pools used by shuffle and serializers.
3. Other branch: the `ListenableArbitrator` (a Velox `MemoryArbitrator`) governs the Velox `MemoryManager`.
4. Under the Velox memory manager:
   - the aggregate root pool `root`;
   - under it, `default_leaf` and the Velox task / `QueryCtx` pools;
   - under those, operator pools such as HashBuild, Aggregation and OrderBy.

- `VeloxMemoryManager` creates a Velox `MemoryManager` with `arbitratorKind` = a freshly registered `ListenableArbitrator` factory, a root aggregate pool `"root"` (capacity `kMaxMemory`, with a `MemoryReclaimer`) and a leaf `"default_leaf"`.
- The Velox task's `QueryCtx` uses `getAggregateMemoryPool()` as its pool, so all operator pools are children of `"root"`.
- Arrow pools (used by shuffle writers, serializers) charge the same listener through `BlockAllocationListener`.

---

## 4. Detailed walkthrough: one allocation end to end

Scenario: a Velox hash aggregation inserts rows and its pool needs 20MB more.

1. **Velox pool growth.** The operator's leaf pool can't reserve more from the root's capacity → Velox calls `ListenableArbitrator::growCapacity(pool, targetBytes)`.
2. **Arbitrator** ([source](../../cpp/velox/memory/VeloxMemoryManager.cc#L117-L185)):

   ```cpp
   void growCapacityInternal(velox::memory::MemoryPool* pool, uint64_t bytes) {
     // enough free capacity
     if (pool->freeBytes() > bytes && growPool(pool, 0, bytes)) return;
     // release unused capacity first
     auto reclaimedFreeBytes = shrinkPool(pool, 0);
     auto neededBytes = velox::bits::roundUp(bytes - reclaimedFreeBytes,
         memoryPoolTransferCapacity_);
     try {
       // ask Spark (may throw)
       listener_->allocationChanged(neededBytes);
     } catch (const std::exception&) {
       listener_->allocationChanged(-reclaimedFreeBytes);
       std::rethrow_exception(std::current_exception());
     }
     VELOX_CHECK(growPool(pool, reclaimedFreeBytes + neededBytes, bytes), ...);
   }
   ```

   `memoryPoolTransferCapacity_` = reservation block size (8MB), so growth happens in 8MB multiples.
3. **JNI listener** → Java `ManagedReservationListener.reserve(size)` (synchronized on a task-shared lock to avoid ABBA deadlocks between listeners) → `target.borrow(size)`.
4. **Decorators**: `ThrowOnOom` → `OverAcquire` (borrow, then briefly borrow & return `0.3 × used` on the `.OverAcquire` node to verify headroom) → `RetryOnOom` → tree node.
5. **Tree node** borrows from its parent up to `TreeMemoryConsumer`, which calls Spark `acquireExecutionMemory(size, OFF_HEAP)`.
6. **Spark** grants from the off-heap execution pool. If not enough is free, it calls `spill()` on other consumers of the task (and possibly this one) → Gluten spills ([07](#doc-07)).
7. **Outcome**:
   - Granted → returns up the stack; Velox pool grows; aggregation continues.
   - Short → `RetryOnOom` spills from the root of the task's tree (`spillTree(root, Long.MAX_VALUE)`) and retries.
   - Still short → `ThrowOnOom`: `System.gc()`, retries up to 9 times with exponential sleep (≤1s total), then throws `OutOfMemoryException` with a dump of all targets and the relevant configs.
     - The exception propagates through JNI into Velox, failing the operator, and back to Spark as a task failure.
8. **Release**: when pools shrink (operator finished, spilled, or task ends), `allocationChanged(-bytes)` → `repay` → Spark `releaseExecutionMemory`.

Typical failure message (abbreviated, illustrative):

```text
org.apache.gluten.memory.memtarget.ThrowOnOomMemoryTarget$OutOfMemoryException:
Not enough spark off-heap execution memory. Acquired: 8.0 MiB,
    granted: 0.0 B. Try tweaking config option
spark.memory.offHeap.size to get larger space to run this application ...
Current config settings:
  spark.gluten.memory.offHeap.size.in.bytes=...
  spark.gluten.memory.task.offHeap.size.in.bytes=...
  spark.gluten.memory.conservative.task.offHeap.size.in.bytes=...
  spark.memory.offHeap.enabled=true
Memory consumer stats:
  Task.123:  Current used bytes: 3.9 GiB, peak bytes: N/A
  \- Gluten.Tree.0: ...
     \- NativePlanEvaluator-0: ...
        \- single: ...
           \- root: ...
              \- task.Gluten_Stage_4_TID_123_VTID_0: ...
                 \- node.3 (HashAggregation): ...
```

This dump is the best place to see **which operator** held memory.

---

## 5. Modes and variants

### 5.1 Legacy (default) vs. isolated

- **Legacy (`spark.gluten.memory.isolation=false`)**, root `Capacity[unlimited]`: A task can grab as much off-heap as Spark gives it (Spark's own per-task fairness `1/2N..1/N` applies). Adds `RetryOnOom` when task slots > 1. *Use when:* Default.
- **Isolated (`spark.gluten.memory.isolation=true`)**, root `Capacity[conservative per-task size]`: Caps each task's native memory at `off-heap × (1 − storageFraction) / slots` *Use when:* Concurrent queries in one session; protects against non-spillable large allocations (config doc)

### 5.2 Over-acquire

`spark.gluten.memory.overAcquiredMemoryRatio` (default `0.3`): after each successful borrow, Gluten tries to borrow an extra `0.3 × used` on a sibling node and immediately returns it. If that probe fails, Spark will have been asked to spill others **now**, while operators are still spillable — a workaround for two-phase operators where phase B can't spill (see `OverAcquire` comments).

### 5.3 Dynamic off-heap sizing (experimental)

`spark.gluten.memory.dynamic.offHeap.sizing.enabled=true`:
- Spark off-heap is disabled; Gluten derives a budget from executor memory × `...memory.fraction` (default 0.6).
- Consumers use `MemoryMode.ON_HEAP`, and `DynamicOffHeapSizingMemoryTarget` checks a total quota (JVM committed heap + tracked native) — may trigger GC to shrink the heap.
- See [docs/developers/VeloxDynamicSizingOffheap.md](../../docs/developers/VeloxDynamicSizingOffheap.md) (JDK 11+ heap free ratio flags).

### 5.4 Untracked

`spark.gluten.memory.untracked=true`: listeners become no-ops. Spark never sees native memory → no spills, no Spark OOMs, but high risk of container kills. Testing only.

### 5.5 Global (process-wide) memory

Some allocations outlive tasks, e.g. **broadcast hash table build relations** kept off-heap (`spark.gluten.velox.offHeapBroadcastBuildRelation.enabled`) or the native backend's global memory manager. `VeloxListenerApi` passes a listener tied to `GlobalOffHeapMemory.target`, which calls Spark `MemoryManager.acquireStorageMemory(...)` with a synthetic block id. Using **storage** memory means Spark can evict cached blocks to make room, and execution memory isn't permanently reduced.

---

## 6. End-to-end worked example: sizing an executor

Cluster: 16 cores, 64GB container, Velox backend.

```text
spark.executor.cores=8
# JVM heap (Spark row ops, driver-side stuff, Arrow Java)
spark.executor.memory=12g
spark.memory.offHeap.enabled=true
# native execution + broadcast (storage) budget
spark.memory.offHeap.size=36g
# untracked native + JVM overhead (>= 0.3 × off-heap recommended)
spark.executor.memoryOverhead=10g
```

Derived by Gluten (driver log / Spark UI environment):

| Derived | Value |
|---|---|
| `spark.gluten.numTaskSlotsPerExecutor` | 8 |
| `spark.gluten.memory.task.offHeap.size.in.bytes` | 36g / 8 = 4.5g |
| `spark.gluten.memory.conservative.task.offHeap.size.in.bytes` | 36g × 0.5 / 8 = 2.25g |

Reasoning:
- Native operators do almost all heavy lifting → give most memory to off-heap.
- Overhead must absorb untracked allocations (allocator caches, IO buffers, libraries). If the container is killed while Spark shows no memory pressure, **increase overhead** first.
- If tasks fail with "Not enough spark off-heap execution memory", either increase off-heap, reduce concurrency (`executor.cores`), or make sure spilling is enabled for the operators involved.

**Try it / verify**

- Spark UI → Environment: the derived keys `spark.gluten.memory.task.offHeap.size.in.bytes` and `spark.gluten.memory.conservative.task.offHeap.size.in.bytes` should match the table above.
- If `spark.executor.memoryOverhead` isn't set, the driver log shows `Memory overhead is not set. Setting it to ... automatically`.
- In the SQL UI, compare `peak memory bytes` on the heaviest operators with the per-task budget (4.5g here). If the peak is close to the budget, expect spilling.

---

## 7. Observing memory

| Where | What you see |
|---|---|
| OOM exception text | Full target tree with current/peak bytes per node and operator pool |
| Spark UI SQL metrics | `peak memory bytes` / `number of memory allocations` per transformer (from Velox operator stats) |
| Spark UI executors tab | Off-heap storage/execution usage |
| Executor log (debug) | Memory usage stats from `collectMemoryUsageStats` when releasing managers |
| Profiling | [docs/developers/ProfileMemoryOfGlutenWithVelox.md](../../docs/developers/ProfileMemoryOfGlutenWithVelox.md) (jemalloc/heap profiling) |
| Leak check | `spark.gluten.sql.columnar.backend.velox.checkUsageLeak` (default `true`) checks pools at destruction; `TreeMemoryConsumers.Factory.release` warns `... still used N bytes when task is ending, this may cause memory leak` |

---

## 8. Configuration reference

| Key | Default | Effect / when to change |
|---|---|---|
| `spark.memory.offHeap.enabled`, `spark.memory.offHeap.size` | Spark defaults | Required native budget |
| `spark.executor.memoryOverhead` | auto `max(0.3 × off-heap, 384MB)` if unset | Raise on container kills |
| `spark.gluten.memory.reservationBlockSize` | `8MB` | Reservation granularity (fewer JNI calls vs. coarser accounting) |
| `spark.gluten.memory.overAcquiredMemoryRatio` | `0.3` | Headroom probe ratio; `0` disables |
| `spark.gluten.memory.isolation` | `false` | Cap per-task native memory |
| `spark.gluten.memory.dynamic.offHeap.sizing.enabled` | `false` (static) | Use heap-derived budget (experimental) |
| `spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction` | `0.6` (static) | Fraction of executor memory for native |
| `spark.gluten.memory.untracked` | `false` (static) | Disable accounting (testing only) |
| `spark.gluten.sql.columnar.backend.velox.memInitCapacity` | `8MB` | Velox pool initial capacity (passed to arbitrator config) |
| `spark.gluten.sql.columnar.backend.velox.reclaimMaxWaitMs` | `3600000` (60 min) | Max wait for memory reclaim in arbitration |
| `spark.gluten.sql.columnar.backend.velox.memoryPoolCapacityTransferAcrossTasks` | `true` | Velox flag allowing capacity transfer |
| `spark.gluten.sql.columnar.backend.velox.memoryUseHugePages` | `false` | Use huge pages in Velox allocator |
| `spark.gluten.sql.columnar.backend.velox.checkUsageLeak` | `true` | Velox pool leak check |
| `spark.memory.storageFraction` | `0.5` | Affects conservative per-task size |

---

## 9. Pitfalls, debugging & FAQ

| Symptom | Likely cause | Action |
|---|---|---|
| `Not enough spark off-heap execution memory` | Budget too small or non-spillable operator (e.g. large hash build) | Increase off-heap; enable/verify operator spill configs; reduce cores; consider isolation |
| Container killed, no Spark OOM | Untracked native memory beyond overhead | Increase `memoryOverhead`; profile with jemalloc; check large IO buffers/caches |
| Frequent tiny spills | Off-heap too small relative to cores; block size too large | Increase off-heap or reduce cores |
| `... still used N bytes when task is ending` | A resource didn't release reservations | Usually a bug; report with plan |
| Driver OOM during planning of huge queries | JVM heap on driver | Not native memory; raise driver memory |

**FAQ**

- *Does Gluten use on-heap memory?* Yes, for JVM-side work (planning, Arrow Java batches, row operators). Native execution uses off-heap.
- *Why does Spark UI show off-heap storage used with no cache?* Global allocations (e.g. off-heap broadcast relations) borrow storage memory.
- *Is accounting exact?* No. Reservations are block-granular and some allocations are untracked; plan overhead accordingly.

---

## 10. Code map

| File | Role |
|---|---|
| [MemoryTargets.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/MemoryTargets.java) | Target composition |
| [TreeMemoryConsumer.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/spark/TreeMemoryConsumer.java) | Spark consumer + tree |
| [TreeMemoryConsumers.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/spark/TreeMemoryConsumers.java) | Roots |
| [TreeMemoryTargets.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/TreeMemoryTargets.java) | spillTree |
| [ThrowOnOomMemoryTarget.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/ThrowOnOomMemoryTarget.java) | OOM handling |
| [RetryOnOomMemoryTarget.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/RetryOnOomMemoryTarget.java) | Retry |
| [OverAcquire.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/OverAcquire.java) | Headroom probe |
| [NativeMemoryManager.scala](../../gluten-arrow/src/main/scala/org/apache/gluten/memory/NativeMemoryManager.scala) | Native MM handle |
| [ReservationListeners.java](../../gluten-arrow/src/main/java/org/apache/gluten/memory/listener/ReservationListeners.java) | Listener creation |
| [GlobalOffHeapMemoryTarget.scala](../../gluten-core/src/main/scala/org/apache/spark/memory/GlobalOffHeapMemoryTarget.scala) | Global memory |
| [AllocationListener.h](../../cpp/core/memory/AllocationListener.h) | Block listener |
| [VeloxMemoryManager.cc](../../cpp/velox/memory/VeloxMemoryManager.cc) | Arbitrator & pools |
| [GlutenPlugin.scala](../../gluten-core/src/main/scala/org/apache/gluten/GlutenPlugin.scala) | Derived sizes |
| [VeloxListenerApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxListenerApi.scala) | Overhead defaults, global listener |

## 11. Related docs

- [07 · Spill](#doc-07) — what happens when Spark asks for memory back
- [08 · Shuffle](#doc-08) — shuffle writer memory behavior
- [13 · Troubleshooting](#doc-13)
- Repo: [VeloxDynamicSizingOffheap.md](../../docs/developers/VeloxDynamicSizingOffheap.md), [ProfileMemoryOfGlutenWithVelox.md](../../docs/developers/ProfileMemoryOfGlutenWithVelox.md)

---

<a id="doc-07"></a>

# 07 · Spill

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [06 · Memory](#doc-06) · Next: [08 · Shuffle](#doc-08)

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
  // SHRINK, then SPILL
  for (Spiller.Phase phase : Spiller.Phase.values()) {
    if (remainingBytes <= 0) break;
    remainingBytes -= spillTree(node, phase, remainingBytes);
  }
  return bytes - remainingBytes;
}
private static long spillTree(TreeMemoryTarget node, Spiller.Phase phase,
    final long bytes) {
  // children sorted by usedBytes descending; recurse; then the node's own
  // spiller if still short
}
```

Why this order?
- **SHRINK before SPILL** — returning idle pool capacity costs nothing; spilling costs disk I/O and later re-reads.
- **Largest first** — frees the most memory with the fewest disruptions.

`Spillers.withMinSpillSize(spiller, reservationBlockSize)` wraps each node's spiller so a request is at least one reservation block (8MB): native releases smaller than a block might not be reported back through the block-granular listener.

### 4.3 SHRINK phase (native memory manager)

```scala
spillers.append(new Spiller() {
  override def spill(self: MemoryTarget, phase: Spiller.Phase,
      size: Long): Long = {
    // only respond for shrinking
    if (!Spillers.PHASE_SET_SHRINK_ONLY.contains(phase)) return 0L
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
  // ColumnarBatchOutIterator.nativeSpill -> ResultIterator::spillFixedSize
  return out.spill(size);
}
```

```cpp
int64_t WholeStageResultIterator::spillFixedSize(int64_t size) {
  int64_t shrunken = memoryManager_->shrink(size);
  if (spillStrategy_ == "auto") {
    int64_t remaining = size - shrunken;
    // conducts spill
    uint64_t spilledOut =
        memoryManager_->getMemoryManager()->arbitrator()->shrinkCapacity(
            remaining);
    return shrunken + spilledOut;
  }
  LOG(WARNING) << "Spill-to-disk was disabled since " << kSpillStrategy <<
      " was not configured.";
  return shrunken;
}
```

`ListenableArbitrator::shrinkCapacity(targetBytes, allowSpill, allowAbort)`:

```cpp
velox::memory::ScopedMemoryArbitrationContext ctx{};
// Velox walks child pools & operators
pool->reclaim(targetBytes, memoryReclaimMaxWaitMs_, status);
// release freed capacity -> repay Spark
return shrinkCapacityInternal(pool, 0);
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
  override def spill(self: MemoryTarget, phase: Spiller.Phase,
      size: Long): Long = phase match {
    case Spiller.Phase.SPILL =>
      val spilled = shuffleWriterJniWrapper.reclaim(nativeShuffleWriter, size)
      spilled
    case _ => 0L
  }
})
```

The native writer frees memory by evicting cached partition payloads / partition buffers to a spill file (hash writer) or sorting and writing buffered rows (sort writer). Details in [08](#doc-08).

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

`ScopedMemoryArbitrationContext` (set in `growCapacity`/`shrinkCapacity`) lets Velox perform reclaim while the requesting operator is mid-allocation (allowing temporary over-use per Velox's rules). If reclaim can't free enough, `RetryOnOom` spills the whole task tree once more, then `ThrowOnOom` fails the task ([06](#doc-06)).

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
INFO WholeStageResultIterator: Spill[root/root]: trying to request spill for
    160.00MB.
INFO WholeStageResultIterator: Spill[root/root]: successfully reclaimed total
    200.12MB with shrunken 40.00MB and spilled 160.12MB.
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
# 8 spill partitions per level
spark.gluten.sql.columnar.backend.velox.spillPartitionBits=3
spark.gluten.sql.columnar.backend.velox.maxSpillLevel=4
# smaller spill files, more CPU
spark.gluten.sql.columnar.backend.velox.spillCompressionCodec=zstd
# spread spill I/O
spark.local.dir=/mnt/disk1,/mnt/disk2
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
| Shuffle writer spilling a lot | Partition buffers too large / many partitions | See shuffle tuning in [08](#doc-08) |

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

- [06 · Memory](#doc-06) — the reservation path that triggers spills
- [08 · Shuffle](#doc-08) — shuffle writer eviction
- [12 · Operators](#doc-12) — flushable aggregation, joins
- Repo: [docs/get-started/Velox.md#spill](../../docs/get-started/Velox.md)

---

<a id="doc-08"></a>

# 08 · Columnar Shuffle

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [07 · Spill](#doc-07) · Next: [09 · Scan](#doc-09)

---

## TL;DR

- Gluten replaces Spark's shuffle with a **columnar, native shuffle**: batches are split by partition, compressed and written by C++, and read back as native batches.
- It still uses Spark's shuffle files, index files, AQE and the external shuffle service, so operations work the same as before.
- There are two main writers: **hash** (fast; memory grows with partitions × columns) and **sort** (memory grows with data size). Gluten switches to sort automatically at ≥ 4000 partitions.

**Technical summary**

- Gluten replaces Spark's `ShuffleExchangeExec` with **`ColumnarShuffleExchangeExec`** (`ColumnarExchange` in EXPLAIN). Data stays columnar end to end: native batches are partitioned, serialized, compressed and written by **native shuffle writers**, and read back into native batches by a **native reader**.
- It plugs into Spark's shuffle framework through **`ColumnarShuffleManager`** (`spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager`) and a **`ColumnarShuffleDependency`**, so map outputs, index files, the external shuffle service, AQE and fetch-failure handling keep working.
- For hash partitioning the partition key is computed **natively** by a `ProjectExecTransformer` that prepends `hash_partition_key = murmur3(keys)`; the native `HashPartitioner` turns it into a partition id (`pmod`).
- Writer types:
  - **Hash** (`VeloxHashShuffleWriter`) — default; splits each batch into **per-partition, per-column buffers**, evicts them as compressed payloads. Very fast for moderate partition counts, but memory grows with *partitions × columns*.
  - **Sort** (`VeloxSortShuffleWriter`) — converts rows to compact row format in pages, sorts 64-bit row ids by partition (radix sort), writes partition runs.
    - Memory grows with *data size*, not partitions.
    - Chosen when `numPartitions ≥ spark.gluten.sql.columnar.shuffle.sort.partitions.threshold` (default **4000**) or `#columns ≥ ...sort.columns.threshold` (default **100000**, i.e. effectively off).
  - **RSS sort** (`VeloxRssSortShuffleWriter`, RSS = *remote shuffle service*) — buffers whole `RowVector`s and serializes per-partition row ranges with Velox's Presto serde; used with **Celeborn** (`useRssSort`, default `true`).
  - **GPU hash** (`VeloxGpuShuffleWriter`) — cuDF builds.
- `LocalPartitionWriter` manages caching/merging payloads, spilling to disk under memory pressure, and producing the final **data + index** files. `RssPartitionWriter` pushes to remote shuffle services.
- Before shuffle, `VeloxResizeBatches` coalesces small batches (hash writer). Codecs: `lz4` (default via Spark), `zstd`, optional QAT/IAA hardware codec backends.

---

## 1. Why a columnar shuffle

Vanilla Spark shuffle serializes rows (`UnsafeRow`) — a native pipeline would need C2R before every exchange and R2C after it. Columnar shuffle:

- avoids two full data conversions per exchange,
- compresses better (column-wise buffers),
- partitions with vectorized loops in C++,
- keeps memory accounting & spilling inside Gluten's framework ([06](#doc-06), [07](#doc-07)).

It reuses Spark's shuffle *infrastructure* (map status, block resolver, fetch) instead of re-implementing it, which keeps compatibility with ESS, AQE, speculation and retries.

---

## 2. Key concepts

- `ColumnarShuffleExchangeExec` (JVM): Columnar exchange node; builds dependency, metrics, serializer. Code: [ColumnarShuffleExchangeExec.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/execution/ColumnarShuffleExchangeExec.scala)
- `ColumnarShuffleDependency` (JVM): `ShuffleDependency` carrying native partitioning, writer type, metrics. Code: [ColumnarShuffleDependency.scala](../../gluten-substrait/src/main/scala/org/apache/spark/shuffle/ColumnarShuffleDependency.scala)
- `ColumnarShuffleManager` (JVM): Spark `ShuffleManager` that creates Gluten writers/readers for columnar deps. Code: [ColumnarShuffleManager.scala](../../gluten-substrait/src/main/scala/org/apache/spark/shuffle/sort/ColumnarShuffleManager.scala)
- `GlutenShuffleManager` (JVM): Registry-based manager delegating per dependency (columnar → `ColumnarShuffleManager`) Code: [GlutenShuffleManager.scala](../../gluten-core/src/main/scala/org/apache/spark/shuffle/GlutenShuffleManager.scala)
- `ShuffleWriterType` (JVM): `HashShuffleWriterType`, `SortShuffleWriterType`, `RssSortShuffleWriterType`, `GpuHashShuffleWriterType` Code: [GlutenConfig.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/config/GlutenConfig.scala#L32-L60)
- `ColumnarShuffleWriter` (JVM): Spark `ShuffleWriter`; creates native writer, feeds batches, registers spiller, commits map output. Code: [ColumnarShuffleWriter.scala](../../backends-velox/src/main/scala/org/apache/spark/shuffle/ColumnarShuffleWriter.scala)
- `ColumnarBatchSerializer` (JVM): Spark `Serializer` used on the read side; wraps native reader. Code: [ColumnarBatchSerializer.scala](../../backends-velox/src/main/scala/org/apache/gluten/vectorized/ColumnarBatchSerializer.scala)
- `Partitioner` (C++) (native): `HashPartitioner`, `RoundRobinPartitioner`, `SinglePartitioner`, `FallbackRangePartitioner`, `RandomPartitioner` Code: `cpp/core/shuffle/*Partitioner*`
- `VeloxHashShuffleWriter` (native): Split-to-buffers writer. Code: [VeloxHashShuffleWriter.cc](../../cpp/velox/shuffle/VeloxHashShuffleWriter.cc)
- `VeloxSortShuffleWriter` (native): Row-pages + radix sort writer. Code: [VeloxSortShuffleWriter.cc](../../cpp/velox/shuffle/VeloxSortShuffleWriter.cc)
- `VeloxRssSortShuffleWriter` (native): Buffered vectors + Presto serde writer (RSS) Code: [VeloxRssSortShuffleWriter.cc](../../cpp/velox/shuffle/VeloxRssSortShuffleWriter.cc)
- `PartitionWriter` (native): Destination abstraction: `hashEvict`, `sortEvict`, `evict`, `stop`, `reclaimFixedSize` Code: [PartitionWriter.h](../../cpp/core/shuffle/PartitionWriter.h)
- `LocalPartitionWriter` (native): Local files: payload cache/merge, spills, final data file. Code: [LocalPartitionWriter.cc](../../cpp/core/shuffle/LocalPartitionWriter.cc)
- `Payload` (native): `InMemoryPayload` (raw buffers), `BlockPayload` (serialized/compressed) Code: [Payload.cc](../../cpp/core/shuffle/Payload.cc)
- `VeloxShuffleReader` + deserializers (native): Hash / Sort / RssSort deserializers → Velox batches. Code: [VeloxShuffleReader.cc](../../cpp/velox/shuffle/VeloxShuffleReader.cc)
- `Options.h` (native): Defaults for writer/reader options. Code: [Options.h](../../cpp/core/shuffle/Options.h)

---

## 3. Architecture

The life of shuffle data.

**Map task:**

1. A native stage outputs Velox batches.
2. A `ProjectExecTransformer` adds `hash_partition_key = murmur3(keys)`.
3. `VeloxResizeBatches` merges small batches.
4. `ColumnarShuffleWriter` (JVM) passes each batch handle over JNI to the native writer (hash, sort or RSS sort).
5. `LocalPartitionWriter` caches, merges, compresses and spills partition data.
6. The output is a data file plus an index file (`IndexShuffleBlockResolver`), the same layout Spark uses.

**Reduce task:**

1. Spark's `ShuffleBlockFetcherIterator` fetches the blocks.
2. `ColumnarBatchSerializer.deserializeStream` hands them over JNI to `VeloxShuffleReader`, which decompresses, deserializes and merges them.
3. The resulting Velox batches feed the next native stage through `InputIteratorTransformer`.

---

## 4. Planning side

### 4.1 Offload and partition key projection

`OffloadExchange` → `VeloxSparkPlanExecApi.genColumnarShuffleExchange(shuffle)` ([source](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxSparkPlanExecApi.scala#L395-L478)):

| Partitioning | What Gluten builds |
|---|---|
| `HashPartitioning(exprs, n)` | `ProjectExecTransformer(Alias(Murmur3Hash(exprs), "hash_partition_key") +: child.output)` under the exchange. If this project fails validation, the **exchange falls back**. |
| `RoundRobinPartitioning(n)` with `spark.sql.execution.sortBeforeRepartition` | hash all columns → local `SortExecTransformer` on the hash → drop it (deterministic round-robin like Spark) |
| `RangePartitioning` | Range bounds are computed on the JVM (Spark's sampling) and a pid column is prepended; native `FallbackRangePartitioner` reads it |
| `SinglePartition` | No pid; single partitioner |

Because the hash is computed in a native project, the partitioning logic matches Spark's `Murmur3Hash` semantics (Velox implements Spark's hash), so co-partitioning with vanilla Spark shuffles stays correct.

### 4.2 Choosing the writer type

`VeloxSparkPlanExecApi.getShuffleWriterType(partitioning, output)` ([source](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxSparkPlanExecApi.scala#L601-L630)):

```scala
if (conf.isUseCelebornShuffleManager) {
  if (conf.celebornShuffleWriterType == "sort") {
    if (conf.useCelebornRssSort) RssSortShuffleWriterType
    else if (partitioning != SinglePartition) SortShuffleWriterType
    else HashShuffleWriterType
  } else HashShuffleWriterType
} else {
  if (partitioning != SinglePartition &&
      // default 4000
      (partitioning.numPartitions >= columnarShuffleSortPartitionsThreshold ||
       // default 100000
       output.size >= columnarShuffleSortColumnsThreshold))
    SortShuffleWriterType
  else HashShuffleWriterType
}
```

In v1.7.0 the EXPLAIN node name is just `ColumnarExchange`; tell the writer apart by its metrics in the SQL UI (`time to split` → hash writer, `time to shuffle sort` → sort writer).

### 4.3 Read side in the plan

The next stage sees `ShuffleQueryStage → (optional VeloxResizeBatches) → InputIteratorTransformer`. With AQE, `AQEShuffleReadExec` (coalesced/skew partitions) works unchanged because Gluten reuses Spark's map statuses.

---

## 5. Map side in depth

### 5.1 `ColumnarShuffleWriter` (JVM)

For each input `ColumnarBatch`:
1. On first batch, create the native partition writer (local dirs from `SparkDirectoryUtil.namespace("shuffle-write")`, codec, buffer sizes) and the native shuffle writer for the dependency's writer type (hash: `nativeBufferSize`, `reallocThreshold`, `partitionBufferEvictThreshold`; sort: sort buffer sizes, radix sort...).
2. Register a **spiller** on the runtime's memory manager that calls `shuffleWriterJniWrapper.reclaim(writer, size)` in the SPILL phase ([07](#doc-07)).
3. `shuffleWriterJniWrapper.write(writer, numRows, batchHandle, availableMemoryPerTask)` — the last argument (`memLimit`) lets the hash writer size buffers relative to available memory.
4. On `stop(success)`: native `stop` → partition lengths & metrics → commit via Spark's `IndexShuffleBlockResolver` (`writeMetadataFileAndCommit`) → `MapStatus`.

### 5.2 Hash writer (`VeloxHashShuffleWriter`)

**Data structures** (per writer):
- `partitionBuffers_[column][partition]` — Arrow buffers (validity, offsets/lengths, values) for each *simple* column and partition.
- Complex types (array/map/struct) are serialized per partition with Velox's Presto serde (`complexTypeFlushBuffer_`).
- `row2Partition_`, `partition2RowCount_`, `partition2RowOffsetBase_`, `partitionBufferBase_` — split bookkeeping.

**Per batch** (`write` → `partitioningAndDoSplit` → `doSplit`):

```text
1. compute pid per row (HashPartitioner: pmod(hash_partition_key,
    n)); count rows per partition
2. strip the pid column
3. calculatePartitionBufferSize(rv, memLimit):
      bytesPerRow = fixed-width bytes + avg binary bytes (running) + avg
          complex bytes
      memLimit   += cachedPayloadSize(); memLimit = max(memLimit, 128MB)
      preAllocRowCnt = min(memLimit / bytesPerRow / numPartitions >> 2,
          splitBufferSize)
4. preAllocPartitionBuffers: allocate/resize/evict buffers for partitions that
    receive rows
   (realloc when size differs by more than
       spark.gluten.sql.columnar.shuffle.realloc.threshold)
5. splitRowVector: column-by-column scatter into partition buffers
   (fixed-width, validity, booleans, binary/strings, complex)
6. if partitionBufferEvictThreshold > 0: evict partitions whose buffers exceed
    it
```

**Eviction**: when a partition's buffer is full (or at stop / under memory pressure), `evictPartitionBuffers` → `assembleBuffers` → `InMemoryPayload` → `partitionWriter->hashEvict(pid, payload, Evict::kCache|kSpill, reuseBuffers, ...)`.

**Memory reclaim** (`reclaimFixedSize`): (1) evict cached payloads to disk (`partitionWriter->reclaimFixedSize`), (2) shrink partition buffers, (3) evict partition buffers.

**Why it's fast**: no sorting, no row conversion, tight per-column loops, output already columnar.
**Why it can use lots of memory**: buffers exist per (partition × column). With `P` partitions, `C` columns and `R` rows per buffer, memory ≈ `P × C × R × width` plus allocator overhead — e.g. 3000 partitions × 50 columns × 4096 rows × 8 bytes ≈ 4.9GB worst case. The `memLimit` heuristic shrinks `R`, but very large `P` means tiny buffers, many small payloads and poor compression. That's why the sort writer exists.

### 5.3 Sort writer (`VeloxSortShuffleWriter`)

**Data structures**: pages of serialized **compact rows** (`velox::row::CompactRow`), and an array of 64-bit **compact row ids**:

```text
row id = [ partitionId : 24 bits | pageNumber : 13 bits | offsetInPage : 27
    bits ]
```

**Per batch** (`insert`): compute pid (same partitioner), serialize rows into the current page (allocate new pages as needed), append row ids; `maybeSpill` triggers `evictAllPartitions` when the array would overflow `uint32` rows or pages exceed `kMaxPageNumber` (8191).

**Evict** (`evictAllPartitions`): sort row ids by partition id — **radix sort** on the partition-id bytes (`spark.shuffle.sort.useRadixSort`, default true; uses half of the array as scratch) or `std::sort` — then walk runs of equal pid and `evictPartition(pid, begin, end)`: copy row bytes into a disk-write buffer (`spark.shuffle.spill.diskWriteBufferSize`, default 1MB), compress, `partitionWriter->sortEvict(...)`.

**Trade-offs**: memory ≈ data size + 8 bytes/row, independent of partition count; extra CPU for row conversion + sort; the reader must convert rows back to columns (`VeloxSortShuffleReaderDeserializer`).

### 5.4 RSS sort writer (`VeloxRssSortShuffleWriter`)

Used with Celeborn when `spark.gluten.sql.columnar.shuffle.celeborn.useRssSort=true` (default):
- keeps input `RowVector`s, records `(batchIdx, rowIdx)` per partition,
- when buffered bytes exceed `sortBufferMaxSize` (64MB default) or on reclaim/stop, serializes each partition's rows directly from the vectors with `VectorStreamGroup::append(rowVector, IndexRanges)` (Presto serde, no row conversion) and pushes compressed blocks through `RssPartitionWriter`.
- Read side: `VeloxRssSortShuffleReaderDeserializer` (Presto deserialize).

### 5.5 `LocalPartitionWriter`

- **Hash path (`hashEvict`)**: payloads go to a `PayloadMerger` (small payloads for the same partition are merged up to `mergeBufferSize` while below `spark.gluten.sql.columnar.shuffle.merge.threshold` × buffer size), then into a `PayloadCache` (compressed in memory) — or straight to a spill file when `Evict::kSpill`.
- **Sort path (`sortEvict`)**: partitions arrive in ascending order per eviction round, so each round streams into a spill file; a lower pid than the last one starts a new spill.
- **Compression**: payloads with fewer rows than `spark.gluten.sql.columnar.shuffle.compression.threshold` (default 100) are written uncompressed (compression overhead isn't worth it).
- **`reclaimFixedSize`**: writes cached payloads to a spill file and frees memory.
- **`stop`**: opens the final data file and, partition by partition, concatenates spilled ranges + cached payloads + remaining buffers, recording partition lengths for the index file.

### 5.6 Optional encodings

- `spark.gluten.sql.columnar.shuffle.dictionary.enabled` (default `false`): dictionary-encode string columns in payloads (`ArrowShuffleDictionaryWriter`).
- `spark.gluten.sql.columnar.shuffle.typeAwareCompress.enabled` (default `false`): type-aware compression for numeric buffers (`VeloxTypeAwareCompress.h`).
- `spark.gluten.sql.columnar.shuffle.codecBackend`: hardware codec backends (QAT/IAA) when built with support (see [docs/get-started/VeloxQAT.md](../../docs/get-started/VeloxQAT.md)).

---

## 6. Reduce side

`ColumnarBatchSerializer.deserializeStream(in)` creates a native reader stream via `ShuffleReaderJniWrapper` with reader options: writer type, codec, `batchSize = spark.gluten.sql.columnar.maxBatchSize`, `readerBufferSize` (1MB), `deserializerBufferSize` (1MB, sort only).

| Deserializer | Input format | Output |
|---|---|---|
| `VeloxHashShuffleReaderDeserializer` | Block payloads of Arrow-style buffers (+ Presto-serialized complex columns) | Velox `RowVector`s; small payloads are **merged** (`merged_`) up to batch size before conversion |
| `VeloxSortShuffleReaderDeserializer` | Compact rows | Rows deserialized into Velox vectors (`CompactRow::deserialize`) in batches |
| `VeloxRssSortShuffleReaderDeserializer` | Presto serde blocks | Velox vectors |

Batches flow into the next native stage through `InputIteratorTransformer` (iterator connector). `spark.gluten.sql.columnar.backend.velox.hashShuffle.reader.streamMerge.enabled` (default `false`) enables a raw payload merge fast path within one stream.

---

## 7. End-to-end worked example

```sql
SELECT k, count(*) FROM t GROUP BY k   -- spark.sql.shuffle.partitions=200
```

Map task (illustrative):

```text
^ FlushableHashAggregateExecTransformer (partial)  -> batch [k, count]
^ ProjectExecTransformer [murmur3hash(k) AS hash_partition_key, k, count]
VeloxResizeBatches                                   -> coalesce tiny batches
ColumnarExchange hashpartitioning(k,
    200) -> writer type chosen at planning (hash here)
```

1. 200 partitions < 4000 → **hash writer**.
2. Per batch: pid = `pmod(hash_partition_key, 200)`; buffers for `k` (and validity) and `count` per partition; scatter.
3. Buffers fill → `InMemoryPayload` → merged & cached compressed in `LocalPartitionWriter`.
4. Memory pressure (e.g. partial agg grows) → Spark spill → shuffle spiller → cached payloads written to a spill file.
5. `stop` → final data file (200 contiguous partition ranges) + index → `MapStatus`.

Reduce task for partition 17:
1. Spark fetches block 17 from each map output.
2. `VeloxHashShuffleReaderDeserializer` decompresses payloads, merges small ones to ~4096 rows, builds `RowVector`s.
3. Final `RegularHashAggregateExecTransformer` consumes them.

SQL UI metrics on `ColumnarExchange` (names from `VeloxMetricsApi`): `shuffle bytes spilled`, `time to spill`, `time to compress`, `time to decompress`, `time to deserialize`, `shuffle wall time`, `peak bytes allocated`, `time to split` (hash) / `time to shuffle sort` (sort), `data size`, `number of partitions`, `dictionary size`.

Now with `spark.sql.shuffle.partitions=8000` the same query uses the **sort writer** (≥ 4000) and you'll see `time to shuffle sort` instead of `time to split`.

**Try it / verify**

```scala
spark.conf.set("spark.sql.shuffle.partitions", "200")
// hash writer: "time to split"
spark.range(0, 10000000)
  .selectExpr("id % 1000 AS k")
  .groupBy("k").count().collect()
spark.conf.set("spark.sql.shuffle.partitions", "8000")
// sort writer: "time to shuffle sort"
spark.range(0, 10000000)
  .selectExpr("id % 1000 AS k")
  .groupBy("k").count().collect()
```

In the SQL UI, open `ColumnarExchange`. The first run reports `time to split`, the second `time to shuffle sort`. That confirms the writer switch at 4000 partitions.

---

## 8. Configuration reference

| Key | Default | Effect |
|---|---|---|
| `spark.shuffle.manager` | — | `org.apache.spark.shuffle.sort.ColumnarShuffleManager` (or Celeborn/Uniffle Gluten managers) |
| `spark.gluten.sql.columnar.shuffle` | `true` | Enable columnar shuffle offload |
| `spark.gluten.sql.columnar.shuffle.sort.partitions.threshold` | `4000` | Use sort writer when partitions ≥ value |
| `spark.gluten.sql.columnar.shuffle.sort.columns.threshold` | `100000` | Use sort writer when columns ≥ value |
| `spark.gluten.sql.columnar.shuffle.celeborn.useRssSort` | `true` | Use RSS sort writer with Celeborn sort mode |
| `spark.gluten.sql.columnar.shuffle.codec` | unset (→ Spark `spark.io.compression.codec`) | Shuffle codec (lz4/zstd/...) |
| `spark.gluten.sql.columnar.shuffle.codecBackend` | unset | Hardware codec backend (QAT/IAA) |
| `spark.gluten.sql.columnar.shuffle.compression.threshold` | `100` | Min rows to compress a payload |
| `spark.gluten.sql.columnar.shuffle.merge.threshold` | `0.25` | Merge small payloads below this fraction of buffer size |
| `spark.gluten.sql.columnar.shuffle.realloc.threshold` | `0.25` | Hash writer buffer realloc threshold |
| `spark.gluten.sql.columnar.shuffle.partitionBufferEvictThreshold` | `-1` | Evict partition buffers larger than N bytes after each split (`-1` off) |
| `spark.gluten.shuffleWriter.bufferSize` | unset → `maxBatchSize` (4096) | Max rows per partition buffer (hash) |
| `spark.gluten.sql.columnar.shuffle.readerBufferSize` | `1MB` | Reader input buffer |
| `spark.gluten.sql.columnar.shuffle.sort.deserializerBufferSize` | `1MB` | Sort reader deserialization buffer |
| `spark.gluten.sql.columnar.shuffle.dictionary.enabled` | `false` | Dictionary encoding of strings |
| `spark.gluten.sql.columnar.shuffle.typeAwareCompress.enabled` | `false` | Type-aware compression |
| `spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInput` / `.shuffleOutput` | `true` / `false` | Batch coalescing around shuffle |
| `spark.shuffle.sort.useRadixSort` | `true` | Radix sort in sort writer |
| `spark.shuffle.spill.diskWriteBufferSize` | `1MB` | Sort writer disk write buffer |
| `spark.shuffle.file.buffer` | `32k` | Output stream buffer |

---

## 9. Pitfalls, debugging & FAQ

| Symptom | Cause | Action |
|---|---|---|
| Exchange falls back to vanilla `Exchange` | Hash key projection failed validation (e.g. unsupported key type) | Check fallback reason; cast key types |
| Map tasks OOM / heavy spill with many partitions | Hash writer buffers ∝ partitions × columns | Lower `sort.partitions.threshold` to switch to sort writer; reduce partitions; enable AQE coalescing |
| Many tiny shuffle blocks, poor compression | Small batches or huge partition count | Keep `resizeBatches.shuffleInput=true`; review `bufferSize` |
| High `time to deserialize` with sort writer | Row→column on read | Expected trade-off; prefer hash writer when partitions are moderate |
| Mixed vanilla/columnar join partitioning mismatch | Should not happen (Spark-compatible murmur3) | If seen, report with plan |
| `ColumnarShuffleManager` not used | `spark.shuffle.manager` not set | Set it (or use `GlutenShuffleManager`) |

**FAQ**
- *Is the external shuffle service supported?* Yes: files and index use Spark's formats/resolver.
- *Can I mix vanilla and columnar shuffles in one app?* Yes; `ColumnarShuffleManager` delegates non-columnar dependencies to Spark's sort shuffle manager.
- *Celeborn/Uniffle?* Build with `-Pceleborn` / `-Puniffle` and use their Gluten shuffle managers (see [docs/get-started/Velox.md](../../docs/get-started/Velox.md#L270-L350)).

---

## 10. Code map

| File | Role |
|---|---|
| [VeloxSparkPlanExecApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxSparkPlanExecApi.scala) | Exchange offload, writer type, dependency, serializer |
| [ColumnarShuffleExchangeExec.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/execution/ColumnarShuffleExchangeExec.scala) | Exchange node |
| [ColumnarShuffleManager.scala](../../gluten-substrait/src/main/scala/org/apache/spark/shuffle/sort/ColumnarShuffleManager.scala) | Shuffle manager |
| [ColumnarShuffleWriter.scala](../../backends-velox/src/main/scala/org/apache/spark/shuffle/ColumnarShuffleWriter.scala) | Writer glue |
| [ShuffleWriterJniWrapper.java](../../gluten-arrow/src/main/java/org/apache/gluten/vectorized/ShuffleWriterJniWrapper.java) | JNI |
| [ColumnarBatchSerializer.scala](../../backends-velox/src/main/scala/org/apache/gluten/vectorized/ColumnarBatchSerializer.scala) | Read side |
| [VeloxHashShuffleWriter.cc](../../cpp/velox/shuffle/VeloxHashShuffleWriter.cc) | Hash writer |
| [VeloxSortShuffleWriter.cc](../../cpp/velox/shuffle/VeloxSortShuffleWriter.cc), [RadixSort.h](../../cpp/velox/shuffle/RadixSort.h) | Sort writer |
| [VeloxRssSortShuffleWriter.cc](../../cpp/velox/shuffle/VeloxRssSortShuffleWriter.cc) | RSS sort writer |
| [LocalPartitionWriter.cc](../../cpp/core/shuffle/LocalPartitionWriter.cc) | Local output |
| [Payload.cc](../../cpp/core/shuffle/Payload.cc) | Payload format |
| [VeloxShuffleReader.cc](../../cpp/velox/shuffle/VeloxShuffleReader.cc) | Reader |
| [Options.h](../../cpp/core/shuffle/Options.h) | Defaults |
| `cpp/velox/tests/VeloxShuffleWriterTest.cc`, `cpp/velox/benchmarks/` | Tests & micro benchmarks |

## 11. Related docs

- [05 · Transitions](#doc-05) (VeloxResizeBatches) · [06 · Memory](#doc-06) · [07 · Spill](#doc-07) · [12 · Operators](#doc-12) (broadcast exchange)
- Repo: [docs/get-started/Velox.md](../../docs/get-started/Velox.md) (Celeborn/Uniffle), [docs/velox-configuration.md](../../docs/velox-configuration.md)

---

<a id="doc-09"></a>

# 09 · Scan & Data Sources

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [08 · Shuffle](#doc-08) · Next: [10 · Native write](#doc-10)

---

## TL;DR

- Native scans read Parquet/ORC directly in C++ with **filters pushed into the reader**, so rows that don't match are never materialized.
- Spark still lists files and plans splits; Gluten passes the file list (paths, offsets, partition values) to Velox for each task.
- Scans are validated for file format, filesystem and types. Anything unsupported falls back to Spark's reader.
- Lake formats (Iceberg, Delta, Hudi, Paimon) plug in as components; see [16](#doc-16) for Delta and Iceberg.

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
  // Map[colName -> value] per file (from Hive-style partition dirs)
  partitionColumns,
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

> Deep dive for Delta and Iceberg (deletion vectors, delete files, column mapping, write paths): [16 · Delta & Iceberg](#doc-16).

Each is a **component** (built with its Maven profile) that injects offload rules for its scans:

- **Iceberg** (`VeloxIcebergComponent`, `gluten-iceberg`): copy-on-write (COW) & merge-on-read (MOR); position deletes offloaded; **equality deletes fall back** (`supportIcebergEqualityDeleteRead=false`) Writes fall back by default; partitioned tables may partially fall back due to unsupported `StaticInvoke` functions ([VeloxIceberg.md](../../docs/get-started/VeloxIceberg.md))
- **Delta** (`VeloxDeltaComponent` (+ `VeloxDelta33/40WriteComponent`), `gluten-delta`): Read incl. column mapping; deletion vectors partial. Native write experimental (`...velox.delta.enableNativeWrite=false` by default) ([VeloxDelta.md](../../docs/get-started/VeloxDelta.md))
- **Hudi** (`VeloxHudiComponent`, `gluten-hudi`): **Copy-on-write (COW) only**.
- **Paimon** (`VeloxPaimonComponent`, `gluten-paimon`): **Non-PK tables**, Spark ≥ 3.3.
- **Kafka** (`gluten-kafka` (`MicroBatchScanExecTransformer`)): Streaming micro-batch source.

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

- [03 · Substrait](#doc-03) (ReadRel & splits) · [10 · Native write](#doc-10) · [13 · Metrics](#doc-13)
- Repo: [VeloxLocalCache.md](../../docs/get-started/VeloxLocalCache.md), [VeloxIceberg.md](../../docs/get-started/VeloxIceberg.md), [VeloxDelta.md](../../docs/get-started/VeloxDelta.md), [VeloxS3.md](../../docs/get-started/VeloxS3.md)

---

<a id="doc-10"></a>

# 10 · Native Write Path

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [09 · Scan](#doc-09) · Next: [11 · Expressions](#doc-11)

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

- [09 · Scan](#doc-09) · [16 · Delta & Iceberg](#doc-16) (lake-format writes) · [05 · Transitions](#doc-05) (carrier rows for Delta writer path) · [13 · Troubleshooting](#doc-13)
- Repo: [velox-parquet-write-configuration.md](../../docs/velox-parquet-write-configuration.md), [VeloxDelta.md](../../docs/get-started/VeloxDelta.md)

---

<a id="doc-11"></a>

# 11 · Expressions, Functions & UDFs

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [10 · Native write](#doc-10) · Next: [12 · Operators](#doc-12)

---

## TL;DR

- Every expression in a native operator (e.g. `upper(name)`) must map to a function that Velox implements **with Spark semantics**. If one doesn't, that operator falls back.
- Mapping is a name lookup plus a native check of the exact argument types, so a function can be supported for some types and not others.
- UDFs run natively only if you ship a native UDF library. Otherwise **partial project** evaluates just the UDF in the JVM and keeps the rest native.

**Technical summary**

- Every Catalyst expression inside an offloaded operator must be converted to an **`ExpressionTransformer`** that can emit a Substrait expression. This is done by **`ExpressionConverter.replaceWithExpressionTransformer`**.
- The core lookup is **`ExpressionMappings`**: a table `Sig[SparkExpressionClass](substraitName)` (e.g.
  - `Sig[Upper]("upper")`, `Sig[Substring]("substring")`) for scalar, aggregate and window functions, plus backend extras.
  - If an expression class isn't in the map → `GlutenNotSupportException("Not supported to map spark function name to substrait function name ...")` → the operator **falls back**.
- Functions are emitted with typed signature names (`substring:str_i32_i32`) and resolved natively by stripping the signature and (if needed) remapping the name to Velox's registry; Velox registers **Spark-semantics** functions (`velox/functions/sparksql`), Presto+Spark aggregates, window functions, and Gluten overwrites at startup.
- Native validation compiles expressions with Velox (`exec::ExprSet`), so a **missing signature/type combination** also causes fallback, even when the name exists.
- **ANSI mode**: with `spark.sql.ansi.enabled=true`, Gluten falls back everything by default (`spark.gluten.sql.ansiFallback.enabled=true`); the ANSI flag is otherwise passed to Velox's Spark query config.
- **UDFs**: Scala/Python/Hive UDFs run natively only if a **native Velox UDF library** registers an implementation under the same name (`spark.gluten.sql.columnar.backend.velox.udfLibraryPaths`); otherwise the projection uses **partial project** (JVM evaluates only the UDF) or falls back.
- To disable a function natively: `spark.gluten.expression.blacklist`.

---

## 1. Why expressions need special care

- Spark has hundreds of expressions with subtle semantics (null handling, overflow, timezones, collation, legacy flags). A native engine must match them **exactly** or results differ.
- Velox implements many Spark functions in its `sparksql` function library. Gluten's job is to: (1) know which Spark expression maps to which native function, (2) send correct types/arguments, (3) refuse anything uncertain so it falls back.

---

## 2. Key concepts

| Concept | Description | Code |
|---|---|---|
| `ExpressionTransformer` | Base trait: `doTransform(SubstraitContext): ExpressionNode`, children, original expression | [ExpressionTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/expression/ExpressionTransformer.scala) |
| `GenericExpressionTransformer` | Default: scalar function call with converted children | same |
| `ExpressionConverter` | Catalyst → transformer conversion, special cases (UDFs, StaticInvoke, Invoke, casts, literals, attribute binding) | [ExpressionConverter.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/expression/ExpressionConverter.scala) |
| `ExpressionMappings` | Class → Substrait name tables (`SCALAR_SIGS`, `AGGREGATE_SIGS`, `WINDOW_SIGS`), blacklist | [ExpressionMappings.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/expression/ExpressionMappings.scala) |
| `ExpressionNames` | String constants for names | [ExpressionNames.scala](../../shims/common/src/main/scala/org/apache/gluten/expression/ExpressionNames.scala) |
| `SparkPlanExecApi` expression hooks | Backend overrides: `extraExpressionMappings`, `extraExpressionConverter`, `gen*Transformer` (e.g. casts, JSON, hash, regex, decimal arithmetic) | [VeloxSparkPlanExecApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxSparkPlanExecApi.scala) |
| `ValidatorApi.doExprValidate` | Backend veto per expression (Velox returns `true`; native validation decides) | [VeloxValidatorApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxValidatorApi.scala) |
| `registerAllFunctions` | Native function registration at backend init | [RegistrationAllFunctions.cc](../../cpp/velox/operators/functions/RegistrationAllFunctions.cc) |
| `SubstraitToVeloxExpr` / `SubstraitParser` | Native conversion & name mapping | [SubstraitToVeloxExpr.cc](../../cpp/velox/substrait/SubstraitToVeloxExpr.cc), [SubstraitParser.cc](../../cpp/velox/substrait/SubstraitParser.cc) |
| `UDFMappings`, `UDFResolver` | Map Spark UDF names to native UDFs; load UDF libraries | [UDFResolver.scala](../../backends-velox/src/main/scala/org/apache/spark/sql/expression/UDFResolver.scala), [UdfLoader.cc](../../cpp/velox/udf/UdfLoader.cc), [Udf.h](../../cpp/velox/udf/Udf.h) |
| Partial project | Evaluate only unsupported expressions in JVM | [docs/developers/PartialProject.md](../../docs/developers/PartialProject.md) |

---

## 3. Architecture

How one Catalyst expression becomes a native function call.

1. `ExpressionConverter.replaceWithExpressionTransformer` receives the expression.
2. Special cases first (UDF, `StaticInvoke`, `Invoke`, Hive UDF): `UDFMappings`, the static-invoke maps, or a backend generator handle them.
3. Otherwise, look up `ExpressionMappings` (class → function name), minus anything in the blacklist. If it's missing, throw `GlutenNotSupportException`, and the operator falls back.
4. The backend may adjust it in `SparkPlanExecApi.extraExpressionConverter`.
5. `transformExpression` builds an `ExpressionTransformer` tree.
6. `doTransform` emits a Substrait `ScalarFunction` whose function id points to `name:signature`.
7. Native code resolves it (`findVeloxFunction`, `toVeloxExpr`) and compiles it with `ExprSet`. During planning, this same compile is the native validation.
8. The function comes from the Velox registry: Spark SQL functions, aggregates, window functions, Gluten overwrites and UDF libraries.

---

## 4. Detailed walkthrough

### 4.1 Conversion algorithm

```scala
private def replaceWithExpressionTransformer0(expr, attributeSeq,
    expressionsMap): ExpressionTransformer =
  // UDFs, StaticInvoke, Invoke
  tryTransformWithoutExpressionMapping(expr, attributeSeq,
      expressionsMap).getOrElse {
    // class lookup (+ doExprValidate)
    val substraitExprName = getAndCheckSubstraitName(expr, expressionsMap)
    BackendsApiManager.getSparkPlanExecApiInstance
      // backend override
      .extraExpressionConverter(substraitExprName, expr, attributeSeq)
      // big match on expression kinds
      .getOrElse(transformExpression(expr, attributeSeq, expressionsMap,
          substraitExprName))
  }
```

`transformExpression` handles attribute binding (`BoundReference` by ordinal), literals, casts (with backend-specific cast transformers), `If`/`CaseWhen`, `In`/`InSet`, struct/array/map accessors, decimal arithmetic (with result-type rescaling, `rescaleDecimalArithmetic = true` for Velox), date/time functions with timezone, regex functions, JSON, hashes, etc., falling back to `GenericExpressionTransformer(name, children, expr)`.

Failure points (all lead to operator fallback with a message):

| Message | Meaning |
|---|---|
| `Not supported to map spark function name to substrait function name: <expr>, class name: X` | Class not in `ExpressionMappings` (or blacklisted) |
| `Not supported scala udf: ...` / `Not supported python udf: ...` | No native UDF registered under that name |
| `Not supported to transform StaticInvoke with object: ..., function: ...` | Unhandled `StaticInvoke` |
| Native validation messages (e.g. scalar function not registered / signature mismatch) | Velox compile failed |

### 4.2 `ExpressionMappings`

```scala
val SCALAR_SIGS: Seq[Sig] = Seq(
  Sig[Add](ADD), Sig[Subtract](SUBTRACT), Sig[Multiply](MULTIPLY),
      Sig[Divide](DIVIDE),
  Sig[And](AND), Sig[Or](OR), ...
  Sig[Upper](UPPER), Sig[Substring](SUBSTRING), ...
  Sig[MapContainsKey](MAP_CONTAINS_KEY)
) ++ SparkShimLoader.getSparkShims.runtimeReplaceableExpressionMappings
```

- `expressionsMap = (SCALAR_SIGS ++ AGGREGATE_SIGS ++ WINDOW_SIGS ++ backend.extraExpressionMappings) − blacklist`.
- `spark.gluten.expression.blacklist` (comma-separated **Substrait names**, e.g. `regexp_extract,from_json`) removes entries → those expressions fall back (or go through partial project).
- `listExpressionMappings()` powers [tools/scripts/gen-function-support-docs.py](../../tools/scripts/gen-function-support-docs.py), which generates [docs/velox-backend-scalar-function-support.md](../../docs/velox-backend-scalar-function-support.md) and the aggregate/window/generator pages.

### 4.3 Substrait emission

`GenericExpressionTransformer.doTransform` converts children, computes the typed name via `ConverterUtils.makeFuncName(name, childTypes)` (e.g. `substring:str_i32_i32`), registers it in the `SubstraitContext` and builds `ScalarFunction(funcId, args, outputType)`. See [03](#doc-03).

### 4.4 Native resolution

- `SubstraitParser::findVeloxFunction` strips `:...` and maps names that differ between Substrait-ish names and Velox (e.g. `equal→equalto`, `lt→lessthan`, `is_not_null→isnotnull`, `strpos→instr`, `murmur3hash→hash_with_seed`, `modulus→remainder`, `negative→unaryminus`).
- `registerAllFunctions()` (at `VeloxBackend::init`):

```cpp
// Spark-semantics scalar functions
velox::functions::sparksql::registerFunctions("");
// + companion functions
velox::aggregate::prestosql::registerAllAggregateFunctions("", true, false,
    true);
// Spark aggs overwrite
velox::functions::aggregate::sparksql::registerAggregateFunctions("", true,
    true);
velox::window::prestosql::registerAllWindowFunctions();
velox::functions::window::sparksql::registerWindowFunctions("");
// name mismatch fixes
registerFunctionOverwrite();
velox::functions::iceberg::registerFunctions();
registerDeltaBitmapAggregator();
```

- Validation compiles expressions with `exec::ExprSet`; at runtime Velox evaluates them vectorized.

### 4.5 Aggregates and windows

- Aggregate functions map through `AGGREGATE_SIGS` (sum, count, avg, min/max, stddev/variance, collect_list/set, approx functions, first/last, bit ops, ...). Partial/final phases use **companion functions** (`*_partial`, `*_merge`, `*_extract`) registered by Velox; intermediate types are handled by `HashAggregateExecTransformer` ([12](#doc-12)).
- Window functions map through `WINDOW_SIGS` (rank, dense_rank, row_number, ntile, lag/lead, nth_value, percent_rank, cume_dist...).

### 4.6 ANSI mode

- `FallbackOnANSIMode`: if `spark.sql.ansi.enabled=true` and `spark.gluten.sql.ansiFallback.enabled=true` (default) → every node tagged `does not support ansi mode`.
- If you set `ansiFallback.enabled=false`, the ANSI flag reaches Velox (`SparkQueryConfig::kAnsiEnabled`) and functions with ANSI-specific behavior use it where implemented. Treat as experimental; check [docs/velox-backend-limitations.md](../../docs/velox-backend-limitations.md).

### 4.7 UDFs

**Native Velox UDFs/UDAFs** ([VeloxUDF.md](../../docs/developers/VeloxUDF.md)):
1. Implement functions in C++ against Velox APIs; expose `getNumUdf()`, `getUdfEntries()`, `registerUdf()` via macros in `cpp/velox/udf/Udf.h` (`Udaf.h` for aggregates).
2. Build a shared library linked against `libvelox.so`.
3. Configure `spark.gluten.sql.columnar.backend.velox.udfLibraryPaths` (and `...driver.udfLibraryPaths` when driver paths differ, e.g. YARN client mode).
4. Register the Java/Hive UDF as usual (`CREATE TEMPORARY FUNCTION myudf AS 'org.apache.gluten.sql.hive.MyUDF'`). At startup `UDFResolver` loads the libraries' signatures; `UDFMappings` maps the Spark UDF name to the native name; `ExpressionConverter` emits it; `UdfLoader` registers it in Velox.
5. If types don't match a registered signature → fallback to the Java UDF.

**JVM UDFs without native implementation**: partial project (default on) evaluates only the UDF expressions in the JVM:

```text
expr: hash(udf(col0)), col1, col2
-> native project computes everything except udf(col0)
-> only col0 is converted to the JVM; udf(col0) computed as partialProject1_
-> result merged back into the native batch
```

**Python UDFs**: `ArrowEvalPythonExec` → `ColumnarArrowEvalPythonExec` exchanges Arrow batches with the Python worker without row conversion (`spark.gluten.sql.columnar.arrowUdf`).

---

## 5. End-to-end worked examples

### 5.1 Trace `upper(substr(name, 1, 3))`

```scala
spark.read.parquet("/tmp/people").select(upper(substring($"name", 1,
    3)).as("p"))
```

1. **Catalyst**: `Alias(Upper(Substring(name#0, Literal(1), Literal(3))), "p")` inside `ProjectExec`.
2. **Offload**: `ProjectExecTransformer`; validation calls `ExpressionConverter.replaceWithExpressionTransformer(projectList, child.output)`.
3. **Mapping**: `Upper → "upper"`, `Substring → "substring"`; `name#0` → `BoundReference(0)`; literals → Substrait literals.
4. **Substrait**:
   - extensions: `0 → "substring:str_i32_i32"`, `1 → "upper:str"`
   - expression: `ScalarFunction(1, [ScalarFunction(0, [field(0), lit(1), lit(3)])])`
5. **Native**: names resolve to Velox Spark functions `substring` and `upper`; `ExprSet` compiles (validation OK); runtime evaluates vectorized over each batch.
6. **EXPLAIN** (illustrative): `^ ProjectExecTransformer [upper(substring(name#0, 1, 3)) AS p#5]`.

### 5.2 Add a new function mapping (walkthrough)

Goal: offload Spark expression `Foo` (class `org.apache.spark.sql.catalyst.expressions.Foo`, SQL `foo(str)`), assuming Velox's `sparksql` library has `foo(varchar) -> varchar`.

1. **Check Velox**: search Velox `functions/sparksql` for `foo`; if missing, implement it in Velox first (see [velox-function-development-guide.md](../../docs/developers/velox-function-development-guide.md)) and bump the Velox version used by Gluten.
2. **Name constant**: add `final val FOO = "foo"` to `ExpressionNames`.
3. **Mapping**: add `Sig[Foo](FOO)` to `SCALAR_SIGS` in `ExpressionMappings` (or to `VeloxSparkPlanExecApi.extraExpressionMappings` if Velox-only).
4. **Special handling (only if needed)**:
   - if arguments need reordering/casting or a different native name, add a case in `VeloxSparkPlanExecApi.extraExpressionConverter` / a custom transformer;
   - if Velox needs extra validation (e.g. unsupported pattern arguments), add a check in `SubstraitToVeloxPlanValidator::validateScalarFunction`;
   - if the native name differs and can't be overwritten, add to `substraitVeloxFunctionMap_`.
5. **Test** in `backends-velox`:

```scala
// backends-velox/src/test/scala/org/apache/gluten/functions/ScalarFunctionsValidateSuite.scala
test("foo") {
  runQueryAndCompare("SELECT foo(l_comment) FROM lineitem LIMIT 10") {
    checkGlutenPlan[ProjectExecTransformer]   // asserts the plan offloaded
  }
}
```

6. **Spark UTs**: enable relevant Spark function tests in `gluten-ut` (`VeloxTestSettings`).
7. **Docs**: regenerate function support docs with `tools/scripts/gen-function-support-docs.py`.

**Try it / verify**

- Example 5.1: `df.explain()` should show `^ ProjectExecTransformer` containing `upper(substring(...))`, and `df.fallbackSummary().numFallbackNodes == 0`.
- To see the failure path, add the function to `spark.gluten.expression.blacklist` and repeat. The project falls back with "Not supported to map spark function name ..." (walkthrough in [13 §6.1](#doc-13)).
- After adding a mapping (5.2), the new test with `checkGlutenPlan[ProjectExecTransformer]` is the proof.

---

## 6. Configuration reference

| Key | Default | Effect |
|---|---|---|
| `spark.gluten.expression.blacklist` | empty | Comma-separated Substrait names forced to fall back |
| `spark.gluten.sql.columnar.fallback.expressions.threshold` | `50` | Max expression depth to offload |
| `spark.gluten.sql.columnar.partial.project` | `true` | Partial project for unsupported expressions |
| `spark.gluten.sql.columnar.partial.generate` | see `GlutenConfig` | Partial generate |
| `spark.gluten.sql.ansiFallback.enabled` | `true` | Fall back all operators in ANSI mode |
| `spark.gluten.sql.columnar.arrowUdf` | see `GlutenConfig` | Columnar Arrow Python UDF |
| `spark.gluten.sql.columnar.backend.velox.udfLibraryPaths` | — | Native UDF libraries (executors) |
| `spark.gluten.sql.columnar.backend.velox.driver.udfLibraryPaths` | — | Native UDF libraries for driver |
| `spark.gluten.sql.columnar.backend.velox.maxCompiledRegexes` | see `VeloxConfig` | Regex compile cache per expression |
| `spark.gluten.sql.columnar.backend.velox.floatingPointMode` | see `VeloxConfig` | Floating-point semantics mode |

---

## 7. Pitfalls & FAQ

- **Same function name, different result?** Check [velox-backend-limitations.md](../../docs/velox-backend-limitations.md) (e.g. regex dialect differences, floating point, timezone edge cases). Blacklist the function as a workaround and file an issue.
- **Decimal precision mismatches** cause signature failures → fallback. Look for `dec<p,s>` signatures in native messages.
- **Regex**: Velox uses RE2; patterns with unsupported features (lookarounds, backreferences) are rejected in validation → fallback.
- **Map-typed expressions**: `alwaysFailOnMapExpression = true` for Velox in some contexts (e.g. hash on map types requires `spark.sql.legacy.allowHashOnMapType`).
- **Where's the list of supported functions?** `docs/velox-backend-scalar-function-support.md`, `...aggregate-function-support.md`, `...window-function-support.md`, `...generator-function-support.md`.

---

## 8. Code map

| File | Role |
|---|---|
| [ExpressionConverter.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/expression/ExpressionConverter.scala) | Conversion |
| [ExpressionMappings.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/expression/ExpressionMappings.scala) | Mapping tables |
| [ExpressionNames.scala](../../shims/common/src/main/scala/org/apache/gluten/expression/ExpressionNames.scala) | Names |
| [ExpressionTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/expression/ExpressionTransformer.scala) | Base transformers |
| [backends-velox ExpressionTransformer.scala](../../backends-velox/src/main/scala/org/apache/gluten/expression/ExpressionTransformer.scala) | Velox-specific transformers |
| [VeloxSparkPlanExecApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxSparkPlanExecApi.scala) | Backend expression hooks |
| [RegistrationAllFunctions.cc](../../cpp/velox/operators/functions/RegistrationAllFunctions.cc) | Native registry |
| [SubstraitToVeloxPlanValidator.cc](../../cpp/velox/substrait/SubstraitToVeloxPlanValidator.cc) | Expression validation |
| [UDFResolver.scala](../../backends-velox/src/main/scala/org/apache/spark/sql/expression/UDFResolver.scala), [UdfLoader.cc](../../cpp/velox/udf/UdfLoader.cc) | UDFs |
| [ScalarFunctionsValidateSuite.scala](../../backends-velox/src/test/scala/org/apache/gluten/functions/ScalarFunctionsValidateSuite.scala) | Function tests |

## 9. Related docs

- [02 · Planning](#doc-02) (partial fallback rules) · [03 · Substrait](#doc-03) · [12 · Operators](#doc-12) · [14 · Build & test](#doc-14)
- Repo: [VeloxUDF.md](../../docs/developers/VeloxUDF.md), [velox-function-development-guide.md](../../docs/developers/velox-function-development-guide.md), [PartialProject.md](../../docs/developers/PartialProject.md)

---

<a id="doc-12"></a>

# 12 · Operators: Joins, Aggregation, Broadcast, Window, Sort & Others

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [11 · Expressions](#doc-11) · Next: [13 · Metrics & troubleshooting](#doc-13)

---

## TL;DR

- Each common Spark operator (scan, filter, project, aggregate, join, window, sort, limit, …) has a native twin that runs the matching **Velox operator**.
- Gluten prefers **shuffled hash join** over sort-merge join, and builds a broadcast hash table **once per executor** instead of once per task.
- Partial aggregations can **flush or skip** pre-aggregation under memory pressure, and aggregation, join, sort and window can spill.

**Technical summary**

- Each offloadable Spark operator has a **transformer** that (1) validates, (2) emits a Substrait `Rel`, (3) maps to a **Velox operator**, (4) exposes native metrics.
- **Aggregation**: `HashAggregateExec`/`ObjectHashAggregateExec`/`SortAggregateExec` → `RegularHashAggregateExecTransformer`, `SortHashAggregateExecTransformer`, and — for partial aggregates feeding a shuffle — **`FlushableHashAggregateExecTransformer`**, which can emit partial results early or abandon pre-aggregation to save memory.
- **Joins**: Gluten prefers **shuffled hash join (SHJ)** over **sort-merge join (SMJ)** (`spark.gluten.sql.columnar.forceShuffledHashJoin=true`), uses Velox `HashJoinNode` for SHJ and **broadcast hash join (BHJ)**, `MergeJoinNode` for SMJ when used, `NestedLoopJoinNode` for **broadcast nested-loop join (BNLJ)**/cartesian.
- **Broadcast**: `ColumnarBroadcastExchangeExec` collects **serialized Velox batches** into a `ColumnarBuildSideRelation` (or off-heap `UnsafeColumnarBuildSideRelation`), broadcasts it, and executors build the hash table — by default **once per executor** and cached (`VeloxBroadcastBuildSideCache`).
- **Window**: `WindowExecTransformer` → Velox `WindowNode` (spillable); `WindowGroupLimitExecTransformer` → `TopNRowNumberNode`/`RowNumberNode`.
- **Sort / TopN / Limit**: `SortExecTransformer` → `OrderByNode` (spillable); `TakeOrderedAndProjectExecTransformer` → `TopNNode`; limits → `LimitNode`.
- Others: `ProjectExecTransformer`, `FilterExecTransformer`, `ExpandExecTransformer`, `GenerateExecTransformer` (`UnnestNode`), `ColumnarUnionExec`, `ColumnarCoalesceExec`, `ColumnarRangeExec`, `SampleExecTransformer`, `ColumnarCollectLimitExec`/`ColumnarCollectTailExec`, partial project/generate, Arrow Python UDF, and the **columnar table cache** (enabled by default).

---

## 1. Operator mapping overview

- `FileSourceScanExec` / `BatchScanExec` → `FileSourceScanExecTransformer` / `BatchScanExecTransformer` → Velox `TableScanNode`
- `FilterExec` → `FilterExecTransformer` → Velox `FilterNode` (or pushed into scan)
- `ProjectExec` → `ProjectExecTransformer` / `ColumnarPartialProjectExec` → Velox `ProjectNode`
- `HashAggregateExec` (partial → exchange) → `FlushableHashAggregateExecTransformer` → Velox `AggregationNode` (partial, flushable). Spill: flushes or abandons instead.
- `HashAggregateExec` (final/complete) → `RegularHashAggregateExecTransformer` → Velox `AggregationNode`. Spill: yes.
- `SortAggregateExec` → `SortHashAggregateExecTransformer` (EXPLAIN prefixes it with `SortToHash`) → Velox `AggregationNode`. Spill: yes.
- `ShuffledHashJoinExec` → `ShuffledHashJoinExecTransformer` → Velox `HashJoinNode`. Spill: yes.
- `SortMergeJoinExec` → `ShuffledHashJoinExecTransformer` (rewritten) or `SortMergeJoinExecTransformer` → Velox `HashJoinNode` / `MergeJoinNode`. Spill: yes for the hash join, no for the merge join.
- `BroadcastHashJoinExec` → `BroadcastHashJoinExecTransformer` → Velox `HashJoinNode` with prebuilt table
- `BroadcastNestedLoopJoinExec` → `VeloxBroadcastNestedLoopJoinExecTransformer` → Velox `NestedLoopJoinNode`
- `CartesianProductExec` → `CartesianProductExecTransformer` → Velox `NestedLoopJoinNode`
- `SortExec` → `SortExecTransformer` → Velox `OrderByNode`. Spill: yes.
- `TakeOrderedAndProjectExec` → `TakeOrderedAndProjectExecTransformer` → Velox `TopNNode` (+ project)
- `GlobalLimitExec` / `LocalLimitExec` → `LimitExecTransformer` → Velox `LimitNode`
- `WindowExec` → `WindowExecTransformer` → Velox `WindowNode`. Spill: yes.
- `WindowGroupLimitExec` → `WindowGroupLimitExecTransformer` → Velox `TopNRowNumberNode` / `RowNumberNode`. Spill: yes.
- `ExpandExec` → `ExpandExecTransformer` → Velox `ExpandNode`
- `GenerateExec` (explode, posexplode, inline, ...) → `GenerateExecTransformer` → Velox `UnnestNode`
- `UnionExec` → `ColumnarUnionExec` (or native union) → Velox `LocalPartitionNode` gather
- `ShuffleExchangeExec` → `ColumnarExchange` → native shuffle writer. Spill: yes.
- `BroadcastExchangeExec` → `ColumnarBroadcastExchange` → no Velox node
- `WriteFilesExec` → `WriteFilesExecTransformer` → Velox `TableWriteNode`
- `ArrowEvalPythonExec` → `ColumnarArrowEvalPythonExec` → no Velox node (Arrow IPC to the Python worker)
- `InMemoryTableScanExec` → columnar cache scan → no Velox node

Full status: [docs/velox-backend-support-progress.md](../../docs/velox-backend-support-progress.md).

---

## 2. Aggregation

### 2.1 Phases and companion functions

Spark plans aggregations as **partial → (shuffle) → final**. Velox has a native notion of aggregation steps (partial, intermediate, final, single). `HashAggregateExecTransformer` ([source](../../backends-velox/src/main/scala/org/apache/gluten/execution/HashAggregateExecTransformer.scala)) maps Spark modes to Substrait phases; native code picks the Velox step and **companion functions** (`sum_partial`, `sum_merge`, `avg_merge_extract`...).

Intermediate types differ between Spark and Velox for some functions (e.g. `avg` intermediate is a struct in Velox vs two columns in Spark). The transformer handles this with:
- **row construction** before the final aggregation (pack Spark's intermediate columns into a struct: `AggregationParams.rowConstructionNeeded`),
- **extraction** after the partial aggregation (split Velox's struct into Spark's columns: `extractionNeeded`).

So a native partial aggregate can feed a native or vanilla final aggregate across a shuffle.

### 2.2 Flushable partial aggregation

`FlushableHashAggregateRule` (post-transform): if an exchange follows a hash aggregate whose functions are all `Partial`/`PartialMerge`, convert it to **`FlushableHashAggregateExecTransformer`** (unless the aggregate is a protected distinct merge, or uses floating-point `sum`/`avg` with strict floating-point mode).

Why: a partial aggregate doesn't need to be complete — duplicates are merged later by the final aggregate. Velox can therefore:
- **flush** the hash table to output when it reaches a memory cap (`maxPartialAggregationMemoryRatio` × task off-heap, default 0.1, min 16MB; extended ratio 0.15),
- **abandon** partial aggregation if it isn't reducing rows (`abandonPartialAggregationMinPct=90`, `abandonPartialAggregationMinRows=100000`) and just pass rows through.

This avoids spilling partial aggregates and handles high-cardinality keys gracefully. Disable with `spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false`.

### 2.3 Sort aggregate

`SortAggregateExec` is offloaded as a hash aggregate (`SortHashAggregateExecTransformer`, shown as `SortToHash...`) that preserves sort-aggregate semantics so upstream sort-elimination rules can drop the now-unneeded sort (`EliminateLocalSort`). Flushing is disallowed for it.

### 2.4 Count rewrite & distinct

`RewriteMultiChildrenCount` rewrites `count(a, b)` (unsupported natively with multiple children) and `MergeTwoPhasesHashBaseAggregate` merges partial+final when no shuffle is between them.

---

## 3. Joins

### 3.1 Strategy

| Spark plans | Gluten does | Why |
|---|---|---|
| `SortMergeJoin` | With `forceShuffledHashJoin=true` (default), `RewriteJoin` converts eligible SMJs into **SHJ** | Velox hash join is fast and avoids two sorts; spill handles large builds |
| `ShuffledHashJoin` | `ShuffledHashJoinExecTransformer` | Build side chosen by Spark; `spark.gluten.sql.columnar.shuffledHashJoin.optimizeBuildSide` can swap to the smaller side |
| `BroadcastHashJoin` | `BroadcastHashJoinExecTransformer` | Prebuilt hash table per executor |
| Non-equi / no keys | BNLJ / cartesian transformers | Nested loop join |

Join type mapping (`substraitJoinType`): Inner, FullOuter, Left/Right Outer (with build side), Left/Right Semi, Left Anti (incl. null-aware anti for BHJ), Existence. Unsupported types fail validation.

### 3.2 Hash join internals

`JoinExecTransformer` builds a Substrait `JoinRel` with keys, optional post-join filter, and pre/post projections (`JoinParams`: `streamPreProjectionNeeded`, `buildPreProjectionNeeded`, `postProjectionNeeded`). Native creates `HashJoinNode`:
- **Build** side → Velox `HashBuild` (spillable, partitioned spill, parallel build above `minTableRowsForParallelJoinBuild`), dedup hash map heuristics (`abandonDedupHashMap.*`).
- **Probe** side → `HashProbe`, which can push **dynamic filters** into probe-side scans (`hashProbe.dynamicFilterPushdown.enabled=true`) and optionally bloom filters (`hashProbe.bloomFilterPushdown.maxSize`).

### 3.3 Broadcast hash join end to end

A broadcast hash join, end to end.

1. The driver asks `ColumnarBroadcastExchangeExec` for its `relationFuture`.
2. The exchange runs the build-side native stage on executors. They serialize their Velox batches and send them back (collect).
3. The exchange builds a `ColumnarBuildSideRelation` (or the off-heap `UnsafeColumnarBuildSideRelation`) and broadcasts it with `sparkContext.broadcast`.
4. A probe task on an executor calls `VeloxBroadcastBuildSideCache.getOrBuildBroadcastHashTable(broadcast, id)`.
5. The first task on that executor builds the Velox hash table and caches a native pointer.
6. Every probe task gets the same `BroadcastHashTable`. Velox `HashProbe` uses the shared table, so it is not rebuilt per task.

- `ColumnarBroadcastExchangeExec.relationFuture` runs `createBroadcastRelation(mode, child, ...)` (metrics `collectTime`, `broadcastTime`, `dataSize`, `numOutputRows`) and broadcasts it.
- Gluten ignores `HashedRelationBroadcastMode.isNullAware` in the relation (documented in code) — null-aware semantics are handled in the join itself.
- `spark.gluten.velox.offHeapBroadcastBuildRelation.enabled` (default `false`, experimental): store the relation off-heap (`UnsafeColumnarBuildSideRelation`), charged to global off-heap (storage) memory.
- `spark.gluten.velox.broadcastBuild.mergeBatches` (default `false`): serialize all batches into one buffer to reduce HashBuild `addInput` calls.
- `spark.gluten.velox.buildHashTableOncePerExecutor.enabled` (default `true`): build the hash table once per executor and share it across tasks (`VeloxBroadcastBuildSideCache`, keyed by broadcast id, invalidated on removal; handles exchange reuse where multiple BHJs share one relation).

Real approved plan (TPC-H Q3 with BHJ, [v1-bhj/spark35/3.txt](../../backends-velox/src/test/resources/tpch-approved-plan/v1-bhj/spark35/3.txt)):

```text
^ BroadcastHashJoinExecTransformer Inner BuildRight (21)
:- ^ ProjectExecTransformer (12)
:  +- ^ BroadcastHashJoinExecTransformer Inner BuildLeft (11)
:     :- ^ InputIteratorTransformer (8)
:     :  +- BroadcastQueryStage (6)
:     :     +- ColumnarBroadcastExchange (5)
:     :        +- ^ ProjectExecTransformer (3)
:     :           +- ^ FilterExecTransformer (2)
:     :              +- ^ FileSourceScanExecTransformer parquet (1)
:     +- ^ FilterExecTransformer (10)
:        +- ^ FileSourceScanExecTransformer parquet (9)
+- ^ InputIteratorTransformer (20)
   +- BroadcastQueryStage (18)
      +- ColumnarBroadcastExchange (17) ...
```

Note how both joins and the probe-side scan live in **one** native stage; broadcast sides enter through `InputIteratorTransformer`.

### 3.4 Sort-merge join

Used when SHJ isn't possible/desired (`forceShuffledHashJoin=false`, or SMJ required). `SortMergeJoinExecTransformer` → Velox `MergeJoinNode` (supported join types are narrower; check validation messages). Enabled via `spark.gluten.sql.columnar.sortMergeJoin` and `supportSortMergeJoinExec`.

---

## 4. Window functions

- `WindowExecTransformer` → Velox `WindowNode` with partition keys, sort keys and frames (rows/range; range frames with expressions may need a pre-computed boundary column: `needPreComputeRangeFrameBoundary = true` for Velox).
- `RewriteUnboundedWindow` (optimizer rule) simplifies unbounded frames.
- Window spill: `spark.gluten.sql.columnar.backend.velox.windowSpillEnabled` (default true).
- `WindowGroupLimitExec` (Spark 3.5+: e.g. `row_number() ... <= N`) → `TopNRowNumberNode` (`rank`, `dense_rank`, `row_number`) or `RowNumberNode`; Velox doesn't require child ordering (`requiredChildOrderingForWindowGroupLimit = false`), so Gluten can drop the sort before it.
- Supported functions: [docs/velox-backend-window-function-support.md](../../docs/velox-backend-window-function-support.md).

Example:

```sql
SELECT * FROM (
  SELECT *, row_number() OVER (PARTITION BY store ORDER BY amount DESC) AS rn
      FROM sales
) WHERE rn <= 3
```

Illustrative plan (Spark 3.5): `^ WindowExecTransformer` over `^ WindowGroupLimitExecTransformer (rank limit 3)` over `InputIteratorTransformer ← ColumnarExchange hashpartitioning(store)`. The group limit runs as Velox `TopNRowNumberNode` and keeps only the top 3 rows per `store` **before** the window is computed, so far fewer rows reach `WindowExecTransformer`. Velox doesn't need sorted input for it, so Gluten can also drop the separate sort Spark would have planned.

---

## 5. Sort, TopN, Limit

- `SortExecTransformer` → `OrderByNode` (spill with `orderBySpillEnabled`, prefix sort optional). `EnsureLocalSortRequirements` / `EliminateLocalSort` add or remove local sorts as needed after offload.
- `TakeOrderedAndProjectExec` (ORDER BY ... LIMIT n) → `TakeOrderedAndProjectExecTransformer`: per-partition `TopNNode`, single-partition exchange, final `TopNNode` + project. Requires columnar sort, shuffle and project to be enabled.
- Limits → `LimitExecTransformer` (`LimitNode` with offset/count); `CollectLimitExec`/`CollectTailExec` → `ColumnarCollectLimitExec`/`ColumnarCollectTailExec`.

---

## 6. Expand, Generate, Union, misc.

- **Expand** (grouping sets / rollup / cube): `ExpandExecTransformer` → `ExpandNode`.
- **Generate**: `GenerateExecTransformer` → `UnnestNode` for `explode`, `posexplode`, `inline`, `json_tuple`/`stack` where supported; `insertPostProjectForGenerate = true`. Partial generate (`spark.gluten.sql.columnar.partial.generate`) keeps unsupported generators in the JVM while the rest stays native.
- **Union**: `UnionTransformerRule` can turn unions of native children into a native union inside one stage (`LocalPartitionNode` gather); otherwise `ColumnarUnionExec` concatenates RDDs.
- **Coalesce**: `ColumnarCoalesceExec`. **Range**: `ColumnarRangeExec`. **Sample**: `SampleExecTransformer` (`spark.gluten.sql.columnarSampleEnabled`).
- **Python UDFs**: `ColumnarArrowEvalPythonExec` (Arrow IPC to Python worker, no row conversion).

---

## 7. Columnar table cache

`spark.gluten.sql.columnar.tableCache` (static, default **`true`** in v1.7.0) makes `VeloxListenerApi` set `spark.sql.cache.serializer = ColumnarCachedBatchSerializer`:
- `df.cache()` stores **serialized Velox batches** (not Spark rows), with optional per-partition statistics for filter pruning (`...tableCache.partitionStats.enabled`).
- Reading the cache yields native batches directly; final rules (`RemoveGlutenTableCacheColumnarToRow`, `PreventBatchTypeMismatchInTableCache`) avoid unnecessary transitions and protect against batch-type mismatches.

---

## 8. End-to-end worked example: joins + aggregation (TPC-H Q3)

This example uses the **real approved plan** that Gluten's own test suite checks against: [v1/spark35/3.txt](../../backends-velox/src/test/resources/tpch-approved-plan/v1/spark35/3.txt). It is produced by `VeloxTPCHV1Suite`, which sets `spark.sql.autoBroadcastJoinThreshold=-1` so that no broadcast joins happen.

```sql
-- TPC-H Q3 (shipping priority)
SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
  AND o_orderdate < date '1995-03-15' AND l_shipdate > date '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
```

**Before: Spark's initial plan** (excerpt). Two sort-merge joins, each with a `Sort` on both sides, and two `HashAggregate`s:

```text
TakeOrderedAndProject (65)
+- HashAggregate (64)
   +- HashAggregate (63)
      +- Project (62)
         +- SortMergeJoin Inner (61)
            :- Sort (55)
            :  +- Exchange (54)
            :     +- Project (53)
            :        +- SortMergeJoin Inner (52)
            :           :- Sort (47) ... Scan parquet customer (43)
            :           +- Sort (51) ... Scan parquet orders (48)
            +- Sort (60) ... Scan parquet lineitem (56)
```

**After: Gluten's final plan** (excerpt; `^` = runs natively):

```text
VeloxColumnarToRow (42)
+- TakeOrderedAndProjectExecTransformer (41)
   +- ^ ProjectExecTransformer (39)
      +- ^ RegularHashAggregateExecTransformer (38)
         +- ^ ProjectExecTransformer (37)
            +- ^ ShuffledHashJoinExecTransformer Inner BuildLeft (36)
               :- ^ InputIteratorTransformer (26)
               :  +- ShuffleQueryStage (24)
               :     +- ColumnarExchange (23)
               :        +- VeloxResizeBatches (22)
               :           +- ^ ProjectExecTransformer (20)
               :              +- ^ ShuffledHashJoinExecTransformer Inner
               :                       BuildLeft (19)
               :                 :- ... ColumnarExchange (6) <- Filter/Scan
               :                 :      customer (1)
               :                 +- ... ColumnarExchange (15) <- Filter/Scan
               :                        orders (10)
               +- ^ InputIteratorTransformer (35)
                  +- ... ColumnarExchange (32) <- Filter/Scan lineitem (27)
```

What changed, and which rule did it:

| Change | Why | Where explained |
|---|---|---|
| `SortMergeJoin` → `ShuffledHashJoinExecTransformer` and all four `Sort`s are gone | `forceShuffledHashJoin=true` (default) lets `RewriteJoin` turn eligible SMJs into hash joins; hash joins don't need sorted input | §3.1 |
| `BuildLeft`: the smaller, filtered side builds the hash table | Build side picked from Spark's plan or from stats (`shuffledHashJoin.optimizeBuildSide`) | §3.1 |
| Two `HashAggregate`s → one `RegularHashAggregateExecTransformer` | The group-by keys include the join key, so there is no shuffle between partial and final aggregation. `MergeTwoPhasesHashBaseAggregate` merges them into one native aggregation | §2.4 |
| `Exchange` → `ColumnarExchange` + `VeloxResizeBatches` | Native columnar shuffle; small batches are merged before shuffle | [08](#doc-08) |
| The top-level join, aggregation and projects form **one native stage** | Shuffle outputs come in through `InputIteratorTransformer` | [02](#doc-02) |
| `VeloxColumnarToRow` only at the very top | `collect()` needs rows; there are no other transitions, i.e. **no fallbacks** | [05](#doc-05) |

**Try it / verify.** Run Q3 on TPC-H data with `spark.sql.autoBroadcastJoinThreshold=-1`, then:

```scala
// expect ShuffledHashJoinExecTransformer, no Sort/SortMergeJoin
spark.sql(q3).explain("formatted")
import org.apache.spark.sql.execution.GlutenImplicits._
spark.sql(q3).fallbackSummary().numFallbackNodes   // expect 0
```

In the SQL UI, open the `ShuffledHashJoinExecTransformer` nodes. The build-side metrics (`time of hash build`, `hash build peak memory bytes`, `bytes written for spilling of hash build`) tell you whether the build side fits in memory. Now set `spark.sql.autoBroadcastJoinThreshold=30M` and rerun: the plan matches [v1-bhj/spark35/3.txt](../../backends-velox/src/test/resources/tpch-approved-plan/v1-bhj/spark35/3.txt) (§3.3), with `BroadcastHashJoinExecTransformer` and `ColumnarBroadcastExchange` instead of the shuffles on the small sides.

---

## 9. Configuration reference

| Key | Default | Effect |
|---|---|---|
| `spark.gluten.sql.columnar.forceShuffledHashJoin` | `true` | Prefer SHJ over SMJ |
| `spark.gluten.sql.columnar.shuffledHashJoin` | `true` | Offload SHJ |
| `spark.gluten.sql.columnar.shuffledHashJoin.optimizeBuildSide` | see `GlutenConfig` | Pick smaller build side |
| `spark.gluten.sql.columnar.sortMergeJoin` | `true` | Offload SMJ |
| `spark.gluten.sql.columnar.broadcastJoin` / `.broadcastExchange` | `true` | Offload BHJ / broadcast |
| `spark.gluten.sql.columnar.broadcastNestedLoopJoin.enabled` | `true` | Offload BNLJ |
| `spark.gluten.sql.columnar.cartesianProduct.enabled` | `true` | Offload cartesian |
| `spark.gluten.velox.buildHashTableOncePerExecutor.enabled` | `true` | Share BHJ hash table per executor |
| `spark.gluten.velox.offHeapBroadcastBuildRelation.enabled` | `false` | Off-heap broadcast relation (experimental) |
| `spark.gluten.velox.broadcastBuild.mergeBatches` | `false` | Merge broadcast batches into one buffer |
| `spark.gluten.velox.minTableRowsForParallelJoinBuild` | `1000` | Parallel hash build threshold |
| `spark.gluten.sql.columnar.backend.velox.hashProbe.dynamicFilterPushdown.enabled` | `true` | Dynamic filters from joins |
| `spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation` | `true` | Flushable partial agg |
| `spark.gluten.sql.columnar.backend.velox.maxPartialAggregationMemoryRatio` | `0.1` | Partial agg memory cap |
| `spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinPct` / `MinRows` | `90` / `100000` | Abandon partial agg |
| `spark.gluten.sql.columnar.hashagg` / `.force.hashagg` | `true` / see config | Offload aggregates / sort agg as hash agg |
| `spark.gluten.sql.columnar.window` / `.window.group.limit` | `true` | Offload windows |
| `spark.gluten.sql.columnar.sort`, `.limit`, `.takeOrderedAndProject`, `.expand`, `.generate`, `.union`, `.coalesce`, `.range` | mostly `true` | Per-operator switches |
| `spark.gluten.sql.columnar.tableCache` | `true` (static) | Columnar table cache |

---

## 10. Pitfalls & FAQ

- **Huge build sides** with forced SHJ: spill handles them but can be slow; consider `forceShuffledHashJoin=false` or AQE skew handling.
- **BHJ OOM on driver**: the relation is collected on the driver; standard Spark advice applies (`spark.sql.autoBroadcastJoinThreshold`, driver memory). Error message mentions this explicitly.
- **Final aggregate falls back but partial is native**: works thanks to intermediate-type extraction, but costs transitions. Check the fallback reason (often an unsupported aggregate function).
- **`SortToHash` in EXPLAIN**: a sort aggregate turned into a hash aggregate — expected.
- **Window with complex range frames falls back**: check frame expressions and types.

---

## 11. Code map

| File | Role |
|---|---|
| [HashAggregateExecTransformer.scala](../../backends-velox/src/main/scala/org/apache/gluten/execution/HashAggregateExecTransformer.scala) | Velox aggregates |
| [FlushableHashAggregateRule.scala](../../backends-velox/src/main/scala/org/apache/gluten/extension/FlushableHashAggregateRule.scala) | Flushable conversion |
| [JoinExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/JoinExecTransformer.scala) | Join base |
| [HashJoinExecTransformer.scala](../../backends-velox/src/main/scala/org/apache/gluten/execution/HashJoinExecTransformer.scala) | SHJ/BHJ |
| [RewriteJoin.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/rewrite/RewriteJoin.scala) | SMJ → SHJ |
| [ColumnarBroadcastExchangeExec.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/execution/ColumnarBroadcastExchangeExec.scala) | Broadcast exchange |
| [ColumnarBuildSideRelation.scala](../../backends-velox/src/main/scala/org/apache/spark/sql/execution/ColumnarBuildSideRelation.scala), [UnsafeColumnarBuildSideRelation.scala](../../backends-velox/src/main/scala/org/apache/spark/sql/execution/unsafe/UnsafeColumnarBuildSideRelation.scala) | Broadcast relations |
| [VeloxBroadcastBuildSideCache.scala](../../backends-velox/src/main/scala/org/apache/gluten/execution/VeloxBroadcastBuildSideCache.scala) | Hash table reuse |
| [VeloxBroadcastNestedLoopJoinExecTransformer.scala](../../backends-velox/src/main/scala/org/apache/gluten/execution/VeloxBroadcastNestedLoopJoinExecTransformer.scala) | BNLJ |
| [WindowExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/WindowExecTransformer.scala), [WindowGroupLimitExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/WindowGroupLimitExecTransformer.scala) | Windows |
| [SortExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/SortExecTransformer.scala), [TakeOrderedAndProjectExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/TakeOrderedAndProjectExecTransformer.scala), [LimitExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/LimitExecTransformer.scala) | Sort/TopN/Limit |
| [ExpandExecTransformer.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/execution/ExpandExecTransformer.scala), [GenerateExecTransformer.scala](../../backends-velox/src/main/scala/org/apache/gluten/execution/GenerateExecTransformer.scala) | Expand/Generate |
| [ColumnarCachedBatchSerializer.scala](../../backends-velox/src/main/scala/org/apache/spark/sql/execution/ColumnarCachedBatchSerializer.scala) | Table cache |
| [SubstraitToVeloxPlan.cc](../../cpp/velox/substrait/SubstraitToVeloxPlan.cc) | Native operator construction |

## 12. Related docs

- [02 · Planning](#doc-02) · [07 · Spill](#doc-07) · [08 · Shuffle](#doc-08) · [11 · Expressions](#doc-11)
- Repo: [velox-backend-support-progress.md](../../docs/velox-backend-support-progress.md), [velox-backend-aggregate-function-support.md](../../docs/velox-backend-aggregate-function-support.md)

---

<a id="doc-13"></a>

# 13 · Metrics, UI, Configuration & Troubleshooting

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [12 · Operators](#doc-12) · Next: [14 · Build & test](#doc-14)

---

## TL;DR

- Native operator stats (rows, time, memory, spill, IO) show up as **normal Spark SQL UI metrics** on the Gluten nodes.
- The **Gluten UI tab** lists, for each query, which operators fell back and why.
- Configs live in three Scala classes; only selected ones are forwarded to C++. `spark.gluten.sql.debug=true` prints what native code actually received.
- Troubleshooting: first classify the problem (slow / wrong result / OOM / crash), then follow the playbook in §5 and the worked cases in §6.

**Technical summary**

- **Metrics**: Velox collects per-operator stats for each task; when the native iterator finishes, Gluten fetches them through JNI and maps them onto the **`SQLMetric`s of the matching transformers** using the operator↔Rel id maps recorded during Substrait generation.
  - So the Spark SQL UI shows native time, rows, bytes, memory, spill and IO stats on Gluten nodes.
- **Gluten UI**: a "Gluten SQL / DataFrame" tab shows build info and per-query **fallback reasons** (from `GlutenBuildInfoEvent` / `GlutenPlanFallbackEvent`), also in the history server.
- **Configuration**: all keys are `ConfigEntry`s in `GlutenCoreConfig`, `GlutenConfig` (common), `VeloxConfig` (backend). Native code receives (a) **backend config** once at init and (b) **session config** per runtime (task) — only selected keys are forwarded.
- **Troubleshooting** in practice: classify the problem (fallback / wrong result / OOM / crash / slow), then pick the tool:
  - fallback → fallback summary & logs;
  - wrong plan or result → debug mode & native plan strings;
  - OOM → the memory dump in the error message;
  - native bugs → task dumps + `GenericBenchmark` + query trace;
  - crashes → core dumps.

---

## 1. Metrics

### 1.1 How native metrics reach the UI

How native metrics reach the Spark UI.

1. **Planning (driver).** During `doWholeStageTransform`, `SubstraitContext` records which Substrait rels belong to which operator, plus join and aggregation parameters.
2. The RDD carries a `metricsUpdatingFunction` built from that information.
3. **Execution (task).** Velox collects per-node stats while the task runs (`collectMetrics`, ordered by node id).
4. When the iterator finishes, the JVM calls `IteratorMetricsJniWrapper.fetch` → `nativeFetchMetrics`. Native code returns one array per metric: input/output rows, CPU, wall time, peak memory, spill, scan IO, ….
5. `MetricsUtil` walks the `MetricsUpdaterTree` and gives each transformer's `MetricsUpdater` its operator metrics.
6. The `SQLMetric` accumulators update, and the numbers appear in the SQL UI.

- `MetricsUtil.genMetricsUpdatingFunction` ([source](../../backends-velox/src/main/scala/org/apache/gluten/metrics/MetricsUtil.scala#L30-L80)) builds a `MetricsUpdaterTree` mirroring the transformer tree (joins visit build + streamed sides; transformers with `MetricsUpdater.None` are skipped) and consumes native metrics in Rel-id order.
  - One Spark operator may correspond to several Velox nodes (e.g. join + pre/post projections); metrics are **merged** (`mergeMetrics`: input from the last, output from the first, times summed).
- Each transformer defines its metrics via `MetricsApi` (`VeloxMetricsApi.gen*Metrics`) and a `*MetricsUpdater` class in `backends-velox/.../metrics`.
- A "velox task stats" accumulator (`TaskStatsAccumulator`) aggregates Velox task-level stats.

### 1.2 Native metric types (from `nativeFetchMetrics`)

Every native operator reports the same set of counters. Not every operator fills in every group.

| Group | Metrics | Look here when… |
|---|---|---|
| Rows / data volume | `inputRows`, `inputVectors`, `inputBytes`, `rawInputRows`, `rawInputBytes`, `outputRows`, `outputVectors`, `outputBytes` | You want to check that filters and joins shrink the data as expected |
| Time | `cpuCount`, `wallNanos`, `loadLazyVectorTime`, `veloxToArrow` | Finding the slowest operator in a stage |
| Memory | `peakMemoryBytes`, `numMemoryAllocations` | Sizing off-heap memory; spotting memory-hungry operators |
| Spill | `spilledInputBytes`, `spilledBytes`, `spilledRows`, `spilledPartitions`, `spilledFiles` | A stage is slow and disks are busy (see [07](#doc-07)) |
| Aggregation | `flushRowCount`, `abandonedPartialAggregationRows` | Checking whether partial aggregation helps (see [12](#doc-12)) |
| Dynamic / bloom filters | `numDynamicFiltersProduced`, `numDynamicFiltersAccepted`, `numReplacedWithDynamicFilterRows`, `bloomFilterBlocksByteSize` | Checking whether join filters reach the probe-side scan |
| Scan & IO | `scanTime`, `skippedSplits`, `processedSplits`, `skippedStrides`, `processedStrides`, `remainingFilterTime`, `ioWaitTime`, `storageReadBytes`, `storageReads`, `localReadBytes`, `ramReadBytes`, `preloadSplits`, `pageLoadTime`, `dataSourceAddSplitWallNanos`, `dataSourceReadWallNanos`, `loadedToValueHook` | Scans are slow: IO-bound (`ioWaitTime`) or filter-bound (`remainingFilterTime`)? Is pruning working (`skipped*`)? |
| Write | `physicalWrittenBytes`, `writeIOTime`, `numWrittenFiles` | Native write cost and file counts (see [10](#doc-10)) |

### 1.3 Reading metrics in the UI (what to look for)

| Node | Key metrics | Interpretation |
|---|---|---|
| `WholeStageCodegenTransformer` | pipeline time | Wall time of the native stage iterator |
| Scan transformer | time of scan, io wait time, storage read bytes, number of skipped row groups / splits | IO-bound? Filters effective? |
| Aggregate | peak memory bytes, bytes written for spilling, number of flushed rows, number of rows after partial aggregation abandonment | Memory pressure, partial agg effectiveness |
| Join | build/probe times, peak memory, spill, dynamic filter stats | Build-side size, skew |
| `ColumnarExchange` | time to split / shuffle sort, compress, spill, shuffle bytes spilled, peak bytes | Shuffle cost & memory |
| `VeloxColumnarToRow` | time to convert | Transition cost |

Extra: `spark.gluten.sql.columnar.backend.velox.showTaskMetricsWhenFinished=true` logs full Velox task stats per task; `spark.gluten.sql.columnar.backend.velox.taskMetricsToEventLog.threshold` (seconds) writes stats of slow tasks to the event log.

---

## 2. Gluten UI

- Tab **"Gluten SQL / DataFrame"** ([docs/get-started/GlutenUI.md](../../docs/get-started/GlutenUI.md)):
  1. **Build info** — Gluten/Velox revisions, Java/Scala/Spark/GCC versions, backend (from `GlutenBuildInfoEvent`, posted by `SubstraitBackend.onDriverStart`).
  2. **Fallback info per execution** — number of Gluten vs fallback nodes and reasons (from `GlutenPlanFallbackEvent`, posted by `GlutenFallbackReporter`; with AQE, per stage).
- Implementation: [GlutenSQLAppStatusListener.scala](../../gluten-ui/src/main/scala/org/apache/spark/sql/execution/ui/GlutenSQLAppStatusListener.scala), [GlutenSQLTab.scala](../../gluten-ui/src/main/scala/org/apache/spark/sql/execution/ui/GlutenSQLTab.scala), [GlutenAllExecutionsPage.scala](../../gluten-ui/src/main/scala/org/apache/spark/sql/execution/ui/GlutenAllExecutionsPage.scala).
- History server: put the gluten-ui jar on the history server classpath (`GlutenSQLHistoryServerPlugin`).
- Disable: `spark.gluten.ui.enabled=false`.
- You can register your own `SparkListener` for these events (e.g. to export fallback stats to a metrics system).

---

## 3. Configuration system

### 3.1 Where configs are defined

| Class | Scope | Examples |
|---|---|---|
| [GlutenCoreConfig.scala](../../gluten-core/src/main/scala/org/apache/gluten/config/GlutenCoreConfig.scala) | Core/memory | `spark.gluten.enabled`, `spark.gluten.memory.*` |
| [GlutenConfig.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/config/GlutenConfig.scala) | Common planning/runtime | `spark.gluten.sql.columnar.*`, `spark.gluten.sql.debug`, shuffle, fallback |
| [VeloxConfig.scala](../../backends-velox/src/main/scala/org/apache/gluten/config/VeloxConfig.scala) | Velox backend | `spark.gluten.sql.columnar.backend.velox.*`, `spark.gluten.velox.*` |
| [cpp/core/config/GlutenConfig.h](../../cpp/core/config/GlutenConfig.h), [cpp/velox/config/VeloxConfig.h](../../cpp/velox/config/VeloxConfig.h) | Native key constants & native defaults | `kSpillStrategy`, `kMemoryReservationBlockSize`, ... |

Configs are built with `buildConf` (dynamic, session-settable) or `buildStaticConf` (must be set at startup). Some are `.internal()` (not shown in generated docs). Reference docs: [Configuration.md](../../docs/Configuration.md), [velox-configuration.md](../../docs/velox-configuration.md), [velox-spark-configuration.md](../../docs/velox-spark-configuration.md).

### 3.2 How configs reach native code

- **Backend config**
  - When: Once per JVM at `NativeBackendInitializer.initialize`
  - Forwards: `GlutenConfig.getNativeBackendConf`: backend-prefixed keys (`spark.gluten.sql.columnar.backend.velox.*`), Hadoop/S3/GCS/ABFS settings, task slots, shuffle codec, selected Spark SQL keys
  - Code: [NativeBackendInitializer.java](../../gluten-arrow/src/main/java/org/apache/gluten/init/NativeBackendInitializer.java)
- **Session config**
  - When: Per `Runtime` (task)
  - Forwards: `GlutenConfig.getNativeSessionConf`: a fixed set of `nativeKeys` (debug, save dir, task off-heap size, ...) + defaults for case sensitivity, ignore missing files, timezone, ANSI, map key dedup, legacy time parser + byte-size Spark configs (spill buffers, shuffle file buffer) + **all dynamic `spark.gluten.sql.columnar.backend.velox.*`** + all `spark.gluten.velox.*` + UGI tokens
  - Code: [GlutenConfig.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/config/GlutenConfig.scala#L535-L612)

Native code turns session config into Velox `QueryConfig` and Hive connector session config in `WholeStageResultIterator::getQueryContextConf` / `createNewVeloxQueryCtx`.

> **Tip:** To verify what native code actually received, enable `spark.gluten.sql.debug=true`: `VeloxBackend` logs its config at init and `VeloxRuntime` logs the session config when creating result iterators.

### 3.3 Most important configs (with v1.7.0 defaults)

| Area | Key | Default |
|---|---|---|
| Enable | `spark.plugins` | must contain `org.apache.gluten.GlutenPlugin` |
| Enable | `spark.gluten.enabled` | `true` |
| Memory | `spark.memory.offHeap.enabled` / `.size` | required |
| Memory | `spark.executor.memoryOverhead` | auto `max(0.3 × off-heap, 384MB)` |
| Memory | `spark.gluten.memory.reservationBlockSize` | `8MB` |
| Memory | `spark.gluten.memory.overAcquiredMemoryRatio` | `0.3` |
| Memory | `spark.gluten.memory.isolation` | `false` |
| Shuffle | `spark.shuffle.manager` | `org.apache.spark.shuffle.sort.ColumnarShuffleManager` |
| Shuffle | `spark.gluten.sql.columnar.shuffle.sort.partitions.threshold` | `4000` |
| Batch | `spark.gluten.sql.columnar.maxBatchSize` | `4096` |
| Join | `spark.gluten.sql.columnar.forceShuffledHashJoin` | `true` |
| Fallback | `spark.gluten.sql.columnar.wholeStage.fallback.threshold` / `query.fallback.threshold` | `-1` / `-1` |
| Fallback | `spark.gluten.sql.ansiFallback.enabled` | `true` |
| Fallback | `spark.gluten.sql.columnar.partial.project` | `true` |
| Fallback | `spark.gluten.expression.blacklist` | empty |
| Spill | `spark.gluten.sql.columnar.backend.velox.spillStrategy` | `auto` |
| Scan | `spark.gluten.sql.columnar.backend.velox.cacheEnabled` | `false` |
| Write | `spark.gluten.sql.native.writer.enabled` | unset (on for Spark 3.4+) |
| Cache | `spark.gluten.sql.columnar.tableCache` | `true` |
| Debug | `spark.gluten.sql.debug` | `false` |
| Debug | `spark.gluten.sql.injectNativePlanStringToExplain` | `false` |
| Debug | `spark.gluten.sql.validation.logLevel` | `WARN` |
| Logging | `spark.gluten.sql.columnar.backend.velox.glogVerboseLevel` / `glogSeverityLevel` | `0` / `1` |
| UI | `spark.gluten.ui.enabled` | `true` |

---

## 4. Logging

- **JVM**: standard Spark log4j; useful loggers: `org.apache.gluten` (planning, memory, fallback reporter), `org.apache.spark.sql.execution.GlutenFallbackReporter`.
- **Native**: glog to stderr (`FLAGS_logtostderr = true`) — appears in executor stderr logs.
  - `glogSeverityLevel` (default `1` = WARNING minimum), `glogVerboseLevel` (`-v`, default `0`).
  - `spark.gluten.sql.debug=true` sets maximum verbosity (unless `glogVerboseLevel` is set), prints configs and Velox plans.
- Useful native log lines: `Spill[...]` (spill), `Shrink[...]` (VLOG 2), `ListenableArbitrator growCapacityInternal failed` (VLOG 2), Velox task termination lines with task id `Gluten_Stage_<s>_TID_<t>_VTID_<n>`.

---

## 5. Troubleshooting playbook

A troubleshooting decision tree. Start from the symptom.

**Query slower than vanilla Spark?**

1. Check fallbacks: Gluten UI tab, `df.fallbackSummary()`, log lines `Validation failed for plan`.
2. Count transitions in EXPLAIN. Turn on partial project, fix unsupported expressions, or set a stage fallback threshold.
3. Inspect metrics: scan IO, shuffle split/sort time, spill, C2R time.

**Wrong results?**

1. Reproduce on small data and compare with `spark.gluten.enabled=false`.
2. Bisect by disabling operators (`spark.gluten.sql.columnar.<op>=false`) or blacklisting functions.
3. Check `velox-backend-limitations.md`. File an issue with the plan and data.

**Task fails with an exception?**

- "Not enough spark off-heap execution memory": read the memory dump; raise off-heap, lower cores, or check spill configs (worked case in §6.2).
- "GlutenException: Velox error": read the native message, enable `printStackOnFailure`, and dump the task with `benchmark_task` + `saveDir`.

**Executor lost or crashed?**

- Exit code 137 / container killed: raise `memoryOverhead` and profile native memory (jemalloc).
- SIGSEGV or `hs_err` file: collect `hs_err` and the core dump, replay the task in `GenericBenchmark`, use a debug build.

### 5.1 Fallback investigation

```scala
import org.apache.spark.sql.execution.GlutenImplicits._
val s = df.fallbackSummary()
s.numGlutenNodes; s.numFallbackNodes; s.fallbackNodeToReason.foreach(println)
```

- For CI: `spark.gluten.sql.columnar.failOnFallback=true` throws on any fallback.
- For more detail: `spark.gluten.sql.validation.printStackOnFailure=true`, lower `spark.gluten.sql.validation.logLevel`.

### 5.2 Plan inspection

- `spark.gluten.sql.injectNativePlanStringToExplain=true` + `df.explain("formatted")` → Velox plan per stage.
- Substrait JSON: `WholeStageTransformer.substraitPlanJson` ([03](#doc-03)).

### 5.3 Reproducing a native problem offline

1. Dump a task: `spark.gluten.sql.benchmark_task.stageId=<s>` (+ `.partitionId` or `.taskId`) and `spark.gluten.saveDir=/tmp/saveDir` → files like `conf_<s>_<p>.ini`, `plan_<s>_<p>.json`, `split_<s>_<p>_<i>.json`, `data_<s>_<p>_<i>.parquet`.
2. Replay with the C++ `GenericBenchmark` ([MicroBenchmarks.md](../../docs/developers/MicroBenchmarks.md)) — debug under gdb, profile with perf.
3. For operator-level replay use Velox **query trace** ([QueryTrace.md](../../docs/developers/QueryTrace.md); `...velox.queryTraceEnabled`, `queryTraceDir`, `queryTraceNodeIds`, `queryTraceTaskRegExp`).

### 5.4 Memory issues

- Spark-level OOM: the exception includes current config and the **full memory target tree** with per-operator pools ([06](#doc-06)).
- Container kills: increase overhead; profile native heap ([ProfileMemoryOfGlutenWithVelox.md](../../docs/developers/ProfileMemoryOfGlutenWithVelox.md)).

### 5.5 Known environment issues ([velox-backend-troubleshooting.md](../../docs/velox-backend-troubleshooting.md))

| Symptom | Fix |
|---|---|
| Fatal error after a native exception (validation crashes the JVM) | `libunwind-dev` installed (seen on Ubuntu 20.04) → purge it and rebuild |
| Jar conflicts (protobuf, flatbuffers, arrow) | Remove conflicting jars from `$SPARK_HOME/jars` or ensure Gluten jar loads first |
| Incompatible class error with native writer | Put the Gluten jar first on the classpath (see doc for YARN client/cluster/local configs) |
| `free(): invalid pointer` at startup | Rebuild with `--compile_arrow_java=ON` |

---

## 6. End-to-end worked examples

### 6.1 Case A — "Why is this query not fully native?"

This case reproduces a check in `GlutenFallbackSuite` (the test that blacklists `add` and expects "Not supported to map spark function name") ([GlutenFallbackSuite.scala](../../gluten-ut/spark34/src/test/scala/org/apache/spark/sql/gluten/GlutenFallbackSuite.scala)). We force a fallback by blacklisting the `add` function:

```scala
spark.conf.set("spark.gluten.expression.blacklist", "add")
val df = spark.sql("SELECT sum(id + 1) FROM range(10)")
df.collect()

import org.apache.spark.sql.execution.GlutenImplicits._
val s = df.fallbackSummary()
println(s"native=${s.numGlutenNodes} fallback=${s.numFallbackNodes}")
s.fallbackNodeToReason.foreach(_.foreach { case (node,
    reason) => println(s"$node -> $reason") })
```

Output (illustrative; node ids vary, but the reason text comes from `ExpressionConverter`):

```text
native=4 fallback=1
002 Project -> ... Not supported to map spark function name to substrait
    function name: (id#0L + 1), class name: Add.
```

How to read and act on it:
1. **Find the node.** Keys look like `<operator id> <node name>` (see `GlutenExplainUtils.addFallbackNodeWithReason`). Look up `(2)` in `df.explain("formatted")`.
2. **Read the reason.** "Not supported to map spark function name" means an **expression** isn't mapped or failed native validation ([11](#doc-11)). Other common reasons are listed in [02](#doc-02) §5.
3. **Count the cost.** In EXPLAIN, `Project` now sits between `RowToVeloxColumnar` and `VeloxColumnarToRow`: two extra conversions for one operator.
4. **Fix it**, from cheapest to most work:
   - remove the blacklist entry;
   - rewrite the expression;
   - for UDFs, rely on **partial project** (`spark.gluten.sql.columnar.partial.project=true`, default);
   - add the function natively ([11](#doc-11) §5.2);
   - if many small fallbacks make a stage slower than vanilla Spark, set `spark.gluten.sql.columnar.wholeStage.fallback.threshold`.
5. **Verify**: `numFallbackNodes` drops to 0 and the Gluten UI tab shows no fallback for this query.

> **Tip:** In CI, set `spark.gluten.sql.columnar.failOnFallback=true` so that a new fallback fails the test instead of quietly slowing it down.

### 6.2 Case B — Task fails with "Not enough spark off-heap execution memory"

The message is built in [ThrowOnOomMemoryTarget.java](../../gluten-core/src/main/java/org/apache/gluten/memory/memtarget/ThrowOnOomMemoryTarget.java). Its structure, annotated. Values and node names are illustrative: tree names come from `Runtime` names such as `NativePlanEvaluator-<n>` and `ShuffleWriter`, and Velox operator pools are named `op.<planNodeId>.<pipeline>.<driver>.<Operator>`.

```text
# (1) how much was asked vs. granted
Not enough spark off-heap execution memory. Acquired: 64.0 MiB,
    granted: 8.0 MiB. Try tweaking config option
    spark.memory.offHeap.size ...
Current config settings:
  # (2) the memory configs in effect
  spark.memory.offHeap.enabled=true
  spark.gluten.memory.dynamic.offHeap.sizing.enabled=N/A
  spark.gluten.memory.offHeap.size.in.bytes=4.0 GiB
  spark.gluten.memory.task.offHeap.size.in.bytes=1024.0 MiB
  spark.gluten.memory.conservative.task.offHeap.size.in.bytes=512.0 MiB
# (3) the memory-target tree for this task
Memory consumer stats:
  Task.57: Current used bytes: 1016.0 MiB, peak bytes: N/A
  \- Gluten.Tree.0: Current used bytes: 1016.0 MiB, peak bytes: 1016.0 MiB
     # native stage runtime
     +- NativePlanEvaluator-0.0: Current used bytes: 960.0 MiB,
            peak bytes: 960.0 MiB
     # the operator holding the memory
     |  \- ... op.1.0.0.HashBuild: Current used bytes: 900.0 MiB, ...
     \- ShuffleWriter.0: Current used bytes: 56.0 MiB, ...
```

How to act:
1. **(1)** A small request that failed means the task was already at its limit. Look at (3) rather than the request size.
2. **(2)** Per-task budget = off-heap size ÷ task slots. Here it's 4 GiB ÷ 4 = 1 GiB.
3. **(3)** Find the biggest leaf. In this example the hash build side uses 900 MiB.
   - Check that the operator can spill (`...velox.joinSpillEnabled`, `aggregationSpillEnabled`, … default true).
   - Check the spill metrics on that node in the UI. If they are 0, spill was disabled or couldn't free memory, e.g. one huge key.
4. **Fix**, in order:
   - more off-heap memory, or fewer cores per executor (a bigger per-task budget);
   - fix skew;
   - for joins, prefer broadcast for small sides or set `forceShuffledHashJoin=false` (SMJ needs less memory);
   - see [06](#doc-06) §6 for sizing and [07](#doc-07) for spill tuning.
5. If instead the **container** is killed (exit 137) with no Spark OOM, the memory was untracked. Raise `spark.executor.memoryOverhead` (decision tree in §5).

---

## 7. Pitfalls & FAQ

- **The fallback summary says "Gluten does not touch it or does not support it".** No reason was recorded, often because AQE re-created the node. Check the driver log for `Validation failed for plan` lines.
- **UI metrics are 0 on a native node.** That operator was fused into another native operator, or the stage was cancelled before metrics were fetched. Metrics are fetched once, when the iterator finishes.
- **Config set with `spark.conf.set` but native code ignores it.** Static and backend configs (`buildStaticConf`, backend init keys) only apply at startup. Session configs reach native code per task, and only for forwarded keys (§3.2).
- **Debug mode is noisy and slow.** Use `spark.gluten.sql.debug=true` only to reproduce a problem, never in production.

---

## 8. Code map

| File | Role |
|---|---|
| [MetricsUtil.scala](../../backends-velox/src/main/scala/org/apache/gluten/metrics/MetricsUtil.scala) | Native → SQL metric mapping |
| [VeloxMetricsApi.scala](../../backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxMetricsApi.scala) | Metric definitions per operator |
| `backends-velox/src/main/scala/org/apache/gluten/metrics/*MetricsUpdater.scala` | Per-operator updaters |
| [JniWrapper.cc](../../cpp/core/jni/JniWrapper.cc) | `nativeFetchMetrics` |
| [WholeStageResultIterator.cc](../../cpp/velox/compute/WholeStageResultIterator.cc) | `collectMetrics`, query config |
| [GlutenFallbackReporter.scala](../../gluten-substrait/src/main/scala/org/apache/spark/sql/execution/GlutenFallbackReporter.scala) | Fallback logs/events |
| `gluten-ui/src/main/scala/org/apache/spark/sql/execution/ui/*` | UI |
| [GlutenConfig.scala](../../gluten-substrait/src/main/scala/org/apache/gluten/config/GlutenConfig.scala), [VeloxConfig.scala](../../backends-velox/src/main/scala/org/apache/gluten/config/VeloxConfig.scala) | Config definitions & propagation |

## 9. Related docs

- [02 · Planning](#doc-02) · [03 · Substrait](#doc-03) · [04 · Runtime](#doc-04) · [06 · Memory](#doc-06) · [14 · Build & test](#doc-14)

---

<a id="doc-14"></a>

# 14 · Build, Test & Development Workflow

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [13 · Troubleshooting](#doc-13) · Next: [15 · Other backends](#doc-15)

---

## TL;DR

- Gluten is **C++ libraries + Scala/Java jars**, packaged into one bundle jar.
- **Build native first, then Maven**, and always use `./build/mvn`.
- Tests come in four layers: Scala suites, reused Spark unit tests (`gluten-ut`), C++ gtests and benchmarks. Format code and check license headers before committing.

**Technical summary**

- Gluten = **C++ native libraries** (`libgluten.so`, `libvelox.so`, built with CMake/Ninja against a Velox checkout) + **Scala/Java jars** (Maven), packaged into one **bundle jar** (`package/target/gluten-velox-bundle-spark<ver>_<scala>-<os>_<arch>-<gluten-ver>.jar`) that embeds the native libs.
- **Always build native first, then Maven**, and always use `./build/mvn` (pins Maven version/JVM flags). See [AGENTS.md](../../AGENTS.md).
- One-shot: `./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON ...`. Incremental: `./dev/builddeps-veloxbe.sh build_gluten_cpp` then `./build/mvn ... -DskipTests`.
- Velox comes from `ep/build-velox` (`get-velox.sh` pins `VELOX_REPO`/`VELOX_BRANCH`; v1.7.0 uses IBM's `gluten-1.7.0-dft` branch).
- Tests: **Scala suites** in `backends-velox/src/test` (e.g. `*ValidateSuite`, TPC-H approved plans), **Spark UT reuse** in `gluten-ut` (`VeloxTestSettings` per Spark version), **C++ gtests** in `cpp/velox/tests`, **micro benchmarks** (`GenericBenchmark`), **gluten-it** TPC-H/DS integration harness.
- Before committing: `./dev/format-scala-code.sh`, `./dev/format-cpp-code.sh` (clang-format **15**), `./dev/check.py header main --fix`.

---

## 1. Why the build is structured this way

| Constraint | Consequence |
|---|---|
| Velox is a large C++ project with many deps (folly, arrow, protobuf, ...) | Separate native build (`ep/`, `dev/builddeps-veloxbe.sh`), optional vcpkg for a hermetic static toolchain |
| JVM code calls native through JNI and must load matching libs | Bundle native libs inside the jar; native & Java must come from the same commit |
| Support many Spark/Scala/Java versions | Maven profiles (`spark-3.3 … spark-4.1`, `scala-2.12/2.13`, `java-8 … java-25`) + shims |
| Optional integrations | Maven profiles (`iceberg`, `delta`, `hudi`, `paimon`, `celeborn`, `uniffle`, `kafka`) + CMake flags (`ENABLE_S3`, `ENABLE_HDFS`, `ENABLE_GCS`, `ENABLE_ABFS`, `ENABLE_QAT`, `ENABLE_GPU`) |

---

## 2. Repository build components

| Path | Role |
|---|---|
| [build/mvn](../../build/mvn) | Maven wrapper (use it, not system `mvn`) |
| [dev/buildbundle-veloxbe.sh](../../dev/buildbundle-veloxbe.sh) | Native build + `mvn clean install` for one or more Spark versions |
| [dev/builddeps-veloxbe.sh](../../dev/builddeps-veloxbe.sh) | Native build driver; subcommands `get_velox`, `build_arrow`, `build_velox`, `build_gluten_cpp`, `build_velox_backend` |
| [ep/build-velox/src/get-velox.sh](../../ep/build-velox/src/get-velox.sh), [build-velox.sh](../../ep/build-velox/src/build-velox.sh) | Fetch & build Velox (+ patches) |
| `dev/vcpkg/` | vcpkg manifests for static builds |
| `cpp/CMakeLists.txt`, `cpp/core`, `cpp/velox` | Gluten native code |
| `dev/docker/`, [docs/developers/velox-backend-build-in-docker.md](../../docs/developers/velox-backend-build-in-docker.md) | Containerized builds |
| `dev/format-*.sh`, `dev/check.py` | Formatting & license checks |
| `dev/gen-all-config-docs.sh` | Regenerate configuration docs |
| `package/` | Bundle jar assembly |

---

## 3. Building

The build pipeline, in order.

1. `ep/build-velox/get-velox.sh` fetches the pinned Velox fork and branch.
2. `builddeps-veloxbe.sh` builds Velox and its dependencies (vcpkg optional).
3. CMake under `cpp/` builds `libgluten.so` and `libvelox.so`.
4. `./build/mvn -Pbackends-velox -Pspark-X.Y` builds the Scala/Java modules.
5. `package/` produces the `gluten-velox-bundle` jar, containing the jars and the native libraries.
6. You run it with `spark-shell` / `spark-submit --jars <bundle>`.

For incremental work after a C++ change, run `builddeps-veloxbe.sh build_gluten_cpp` (step 3 only), then step 4 again.

The order matters. Maven packages the native libraries built in the step before it. If you change C++ and skip the native rebuild, the jar builds fine but fails at runtime (see [AGENTS.md](../../AGENTS.md)).

### 3.1 First-time full build (recommended)

```bash
# auto-confirm system package / vcpkg prompts
export PROMPT_ALWAYS_RESPOND=y
./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON \
  --enable_s3=OFF --enable_gcs=OFF --enable_hdfs=OFF --enable_abfs=OFF
# -> package/target/gluten-velox-bundle-*.jar
```

`buildbundle-veloxbe.sh` sources `builddeps-veloxbe.sh` (native build), then for each Spark version runs:

```bash
./build/mvn clean install -Pbackends-velox -Pspark-3.5 -DskipTests
# Spark 4.x:
./build/mvn clean install -Pbackends-velox -Pspark-4.0 -Pjava-17 -Pscala-2.13 \
  -DskipTests -Dmaven.compiler.release=17
```

### 3.2 Native build options (`builddeps-veloxbe.sh`)

| Option | Purpose |
|---|---|
| `--build_type=Release/Debug/RelWithDebInfo` | CMake build type |
| `--build_tests=ON` | Build Gluten C++ gtests |
| `--build_benchmarks=ON` | Build micro benchmarks (`GenericBenchmark`) |
| `--build_examples=ON` | Build examples (e.g. UDF example lib) |
| `--enable_vcpkg=ON` | Static build with vcpkg deps |
| `--enable_s3/--enable_gcs/--enable_hdfs/--enable_abfs=ON` | Filesystem connectors |
| `--enable_qat=ON` | Intel QAT codec |
| `--enable_gpu=ON` | cuDF GPU support |
| `--enable_jemalloc_stats=ON` | jemalloc stats/profiling |
| `--enable_enhanced_features=ON` | Enhanced features (e.g. some DSv2 write paths) |
| `--velox_repo=… --velox_branch=… --velox_home=…` | Use a custom Velox |
| `--build_velox_tests/--build_velox_benchmarks=ON` | Build Velox's own tests/benchmarks |
| `--num_threads=N` | Parallelism |

`build_gluten_cpp` runs CMake with `-DBUILD_VELOX_BACKEND=ON -DVELOX_HOME=... -DBUILD_TESTS=... -DENABLE_S3=...` and `ninja` in `cpp/build`; libraries land in `cpp/build/releases/` and are picked up by Maven when packaging.

### 3.3 Incremental loops

| You changed | Rebuild |
|---|---|
| Gluten C++ (`cpp/`) | `./dev/builddeps-veloxbe.sh build_gluten_cpp` (or `ninja -C cpp/build`) → `./build/mvn package -Pbackends-velox -Pspark-3.5 -DskipTests -pl package -am` (re-bundle) |
| Velox | `./dev/builddeps-veloxbe.sh build_velox build_gluten_cpp` → Maven package |
| Scala/Java only | `./build/mvn package -Pbackends-velox -Pspark-3.5 -DskipTests` (native libs reused) |
| `gluten-core`/`gluten-substrait` with JNI/Substrait changes | Rebuild **native too** — Maven succeeds but JNI/Substrait mismatches fail at runtime (AGENTS.md pitfall) |

> **Tip:** After a clean checkout run `./build/mvn install ... -DskipTests` once, because `gluten-ut` and `backends-velox` resolve sibling modules from the local Maven repo.

### 3.4 Maven profiles

| Category | Profiles |
|---|---|
| Backend | `backends-velox`, `backends-clickhouse` |
| Spark | `spark-3.3`, `spark-3.4`, `spark-3.5`, `spark-4.0`, `spark-4.1` |
| Scala / Java | `scala-2.12`, `scala-2.13`; `java-8`, `java-11`, `java-17`, `java-21`, `java-25` |
| Hadoop | `hadoop-2.7.4`, `hadoop-3.2`, `hadoop-3.3`, `hadoop-3.4` |
| Lakes | `iceberg`, `iceberg-test`, `delta`, `hudi`, `paimon` |
| Shuffle | `celeborn`, `uniffle` |
| Other | `kafka`, `spark-ut` (enables `gluten-ut`), `fast-build`, platform profiles |

Spark 4.x requires `-Pjava-17 -Pscala-2.13` (and `-Dmaven.compiler.release=17`).

---

## 4. Running locally

```bash
GLUTEN_JAR=$(ls package/target/gluten-velox-bundle-*.jar)
$SPARK_HOME/bin/spark-shell \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.driver.extraClassPath=$GLUTEN_JAR \
  --conf spark.executor.extraClassPath=$GLUTEN_JAR \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=4g \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager
```

TPC-H in a shell: [tools/workload/tpch/run_tpch/tpch_parquet.scala](../../tools/workload/tpch/run_tpch/tpch_parquet.scala) (see [docs/get-started/Velox.md](../../docs/get-started/Velox.md#L490-L530)).

IDE setup & debugging (IntelliJ for Scala, VSCode/CLion for C++, attaching gdb to executors, debug Velox builds): [docs/developers/NewToGluten.md](../../docs/developers/NewToGluten.md). Bloop integration: [docs/developers/bloop-integration.md](../../docs/developers/bloop-integration.md).

---

## 5. Testing

### 5.1 Test layers

- **Scala backend suites**
  - Where: `backends-velox/src/test/scala` (e.g. `functions/*ValidateSuite`, execution suites, TPC-H plan checks with `tpch-approved-plan/`)
  - Checks: Results vs vanilla Spark + that plans are offloaded (`checkGlutenPlan[...]`)
  - Run: `./build/mvn test -Pbackends-velox -Pspark-3.5 -pl backends-velox -Dsuites=org.apache.gluten.functions.ScalarFunctionsValidateSuite`
- **Spark UT reuse**
  - Where: `gluten-ut/spark3x/...`, enabled list in `VeloxTestSettings.scala`
  - Checks: Spark's own test suites run with Gluten (`enableSuite[...]`, `.exclude("test name")`)
  - Run: `./build/mvn test -pl gluten-ut -Pspark-ut -Pbackends-velox -Pspark-3.5 -Pscala-2.12`
- **C++ gtests**
  - Where: `cpp/velox/tests` (Substrait conversion/validation, shuffle writers, C2R/R2C, memory manager, functions, ...)
  - Checks: Native components in isolation
  - Run: Build with `--build_tests=ON`, run binaries from `cpp/build/velox/tests` (or `ctest`)
- **Micro benchmarks**
  - Where: `cpp/velox/benchmarks` (`GenericBenchmark`, `ParquetWriteBenchmark`, ...)
  - Checks: Replay dumped tasks, profile
  - Run: [MicroBenchmarks.md](../../docs/developers/MicroBenchmarks.md)
- **Integration**
  - Where: `tools/gluten-it`
  - Checks: TPC-H/TPC-DS data gen, query runs, result comparison vs baseline, parameterized configs
  - Run: `cd tools/gluten-it && mvn clean package -Pspark-3.5 && sbin/gluten-it.sh queries-compare ...`

Example Scala test pattern:

```scala
test("bit_count") {
  runQueryAndCompare("SELECT bit_count(l_partkey) from lineitem limit 1") {
    checkGlutenPlan[ProjectExecTransformer]
  }
}
```

### 5.2 Approved plans

`backends-velox/src/test/resources/tpch-approved-plan/{v1,v1-bhj,...}/sparkXX/*.txt` store expected formatted plans for TPC-H; plan-changing PRs must update them. These files are also great **learning material** for reading Gluten plans.

### 5.3 CI

GitHub Actions in [.github/workflows](../../.github/workflows): `velox_backend_x86.yml`, `velox_backend_arm.yml`, `velox_backend_ansi.yml`, `velox_backend_cache.yml`, `velox_backend_enhanced.yml`, nightly/weekly jobs, `code_format.yml`, `scala_code_format.yml`, `check_license.yml`, `cpp_clang_tidy.yml`, `flink.yml`, ClickHouse triggers. Details: [docs/developers/velox-backend-CI.md](../../docs/developers/velox-backend-CI.md).

---

## 6. End-to-end worked example: make a change and test it

Scenario: fix a bug in Substrait→Velox conversion of a function argument and add a test.

1. **Branch & build once**: `./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON --build_tests=ON --spark_version=3.5`.
2. **Reproduce**: write a failing Scala test in `backends-velox/src/test/.../ScalarFunctionsValidateSuite.scala` (`runQueryAndCompare`), or dump the failing task (`spark.gluten.sql.benchmark_task.*`, `spark.gluten.saveDir`) and replay with `GenericBenchmark`.
3. **Fix C++** in `cpp/velox/substrait/SubstraitToVeloxExpr.cc`; add a gtest in `cpp/velox/tests/SubstraitVeloxExprConverterTest.cc`.
4. **Rebuild native**: `./dev/builddeps-veloxbe.sh build_gluten_cpp`; run the gtest binary.
5. **Re-bundle & run Scala test**: `./build/mvn install -Pbackends-velox -Pspark-3.5 -DskipTests`, then `./build/mvn test -Pbackends-velox -Pspark-3.5 -pl backends-velox -Dsuites=...`.
6. **Spark UTs** (if behavior is covered by Spark tests): enable/un-exclude in `gluten-ut/spark35/.../VeloxTestSettings.scala`, run `gluten-ut`.
7. **Format & license**: `./dev/format-cpp-code.sh` (clang-format 15), `./dev/format-scala-code.sh`, `./dev/check.py header main --fix`.
8. **PR**: title `[GLUTEN-<issue>][VL] description` (or `[CORE]`, `[CH]`, `[DOC]`, `[MINOR]`), fill the template including the generative-AI disclosure ([CONTRIBUTING.md](../../CONTRIBUTING.md)).

---

## 7. Pitfalls & FAQ

| Pitfall | Avoid by |
|---|---|
| Using system `mvn` | Always `./build/mvn` |
| Changing JNI signatures/Substrait protos but only rebuilding Java | Rebuild native too |
| Wrong clang-format version | Use clang-format-15 (`clang-format --version`) |
| Spark 4.x with JDK 8 | Use `-Pjava-17 -Pscala-2.13 -Dmaven.compiler.release=17` |
| Running per-module tests before `install` | Run `./build/mvn install -DskipTests` once |
| `libunwind-dev` installed (Ubuntu 20.04) | Remove it (native exceptions otherwise abort) |
| Mixing jars from different builds on driver/executors | Use one bundle jar everywhere |

---

## 8. Code map

| File | Role |
|---|---|
| [AGENTS.md](../../AGENTS.md) | Rules that are easy to get wrong |
| [docs/get-started/Velox.md](../../docs/get-started/Velox.md) | Build/run guide |
| [docs/get-started/build-guide.md](../../docs/get-started/build-guide.md) | Build parameters |
| [dev/builddeps-veloxbe.sh](../../dev/builddeps-veloxbe.sh) | Native build |
| [dev/buildbundle-veloxbe.sh](../../dev/buildbundle-veloxbe.sh) | Bundle build |
| [ep/build-velox/src/get-velox.sh](../../ep/build-velox/src/get-velox.sh) | Velox version pin |
| [gluten-ut/spark35/.../VeloxTestSettings.scala](../../gluten-ut/spark35/src/test/scala/org/apache/gluten/utils/velox/VeloxTestSettings.scala) | Spark UT enable/exclude |
| `cpp/velox/tests/`, `cpp/velox/benchmarks/` | Native tests/benchmarks |
| [tools/gluten-it/README.md](../../tools/gluten-it/README.md) | Integration testing |

## 9. Related docs

- [13 · Troubleshooting](#doc-13) (dumps & replay) · [11 · Expressions](#doc-11) (adding functions) · [15 · Other backends](#doc-15)
- Repo: [CONTRIBUTING.md](../../CONTRIBUTING.md), [CppCodingStyle.md](../../docs/developers/CppCodingStyle.md), [HowTo.md](../../docs/developers/HowTo.md)

---

<a id="doc-15"></a>

# 15 · Other Backends & Frontends: ClickHouse, GPU (cuDF), Flink

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [14 · Build & test](#doc-14) · Next: [16 · Delta & Iceberg](#doc-16)

---

## TL;DR

- Gluten's core is backend-neutral. **Velox** is the default engine; **ClickHouse** is a second engine that consumes the same plans.
- **GPU** acceleration (Velox + cuDF) and a **Flink** frontend exist but are experimental.
- Pick one backend per build; everything in docs 01–08 (planning, fallback, memory, shuffle framework) is shared.

**Technical summary**

- Gluten's core (plugin, components, planner, transitions, memory targets, Substrait) is **backend-neutral**. A backend implements `SubstraitBackend` + the 8 API facets and ships a native library. Exactly one Substrait backend is loaded at a time.
- **ClickHouse backend** (`backends-clickhouse` + `cpp-ch/local-engine`, `libch.so`): consumes the same Substrait plans but executes with ClickHouse operators; it has its own transformers (`CH*`), MergeTree table support (via Delta metadata), its own shuffle, and keeps scans in separate RDDs (`excludeScanExecFromCollapsedStage`).
- **GPU via Velox cuDF** (experimental): built with `--enable_gpu=ON`; enabled with `spark.gluten.sql.columnar.cudf=true`.
  - Velox plan nodes are swapped for cuDF GPU operators where supported (filter, order by, partial project/agg/join), with CPU fallback; GPU memory is managed by RMM outside Spark's memory system; a GPU-aware shuffle path exists.
- **Flink frontend** (experimental, `gluten-flink`): offloads Flink SQL streaming operators to Velox through **Velox4j**, reusing the Velox engine but not Spark's planner.

---

## 1. Why multiple backends/frontends

| Goal | How the design enables it |
|---|---|
| Use the best native engine per environment | Backend abstraction behind `BackendsApiManager`; Substrait as neutral IR |
| Share planning/fallback/memory logic | `gluten-core` + `gluten-substrait` are engine-agnostic |
| Hardware acceleration | Velox's cuDF integration plugs in below the Velox plan, so Gluten mostly passes config and handles batch/shuffle specifics |
| Other compute frameworks | Reuse the native engine (Velox) via a different frontend (Flink planner extensions) |

---

## 2. ClickHouse backend

### 2.1 Architecture

From [docs/get-started/ClickHouse.md](../../docs/get-started/ClickHouse.md):

1. On the driver, Gluten converts the physical plan to Substrait (same framework as Velox).
2. Executors load `libch.so` through JNI; the ClickHouse "local engine" parses Substrait (`cpp-ch/local-engine/Parser`) and builds ClickHouse query pipelines.
3. A ClickHouse catalog (DSv2) supports MergeTree tables whose part metadata is stored with Delta; reads assign parts to Spark partitions; writes produce MergeTree parts and commit metadata via Delta.
4. MergeTree reads currently come from local/shared storage.

### 2.2 Differences vs. Velox backend

| Aspect | Velox backend | ClickHouse backend |
|---|---|---|
| Native library | `libgluten.so` + `libvelox.so` | `libch.so` |
| Native source | `cpp/core`, `cpp/velox` | `cpp-ch/local-engine` |
| Transformers | Common + Velox-specific | `CH*` transformers (e.g. `CHHashJoinExecTransformer`, `CHSortMergeJoinExecTransformer`, `CHWindowGroupLimitExecTransformer`) |
| Scan in whole stage | Yes (scan inside `WholeStageTransformer`) | No — `excludeScanExecFromCollapsedStage = true`, scans run in their own RDD and stages use `genFinalStageIterator` |
| Table formats | Parquet/ORC + Iceberg/Delta/Hudi/Paimon components | MergeTree (+ Delta, Iceberg, Kafka components: `CHDeltaComponent`, `CHIcebergComponent`, `CHKafkaComponent`) |
| Build | `dev/buildbundle-veloxbe.sh` | `ep/build-clickhouse/src/build-clickhouse.sh` + `./build/mvn ... -Pbackends-clickhouse` |
| Toolchain | GCC/Clang per platform (vcpkg optional) | Clang 19, CMake ≥ 3.20 (doc) |

### 2.3 Build & run (summary)

```bash
bash ./ep/build-clickhouse/src/build-clickhouse.sh
./build/mvn clean package -Pbackends-clickhouse -Pspark-3.3 -DskipTests
# then set spark.plugins=org.apache.gluten.GlutenPlugin and point Gluten to
# libch.so (see ClickHouse.md)
```

> **Note:** The rest of this series focuses on Velox; many concepts (components, rules, validation, fallback, transitions, memory targets, Substrait) apply equally to ClickHouse, but operator details, configs and native internals differ.

---

## 3. GPU acceleration with Velox cuDF (experimental)

From [docs/get-started/VeloxGPU.md](../../docs/get-started/VeloxGPU.md) and code:

### 3.1 How it works

- **Build**: `./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF --enable_gpu=ON` (CUDA 12.8 toolkit and drivers; prebuilt image `apache/gluten:centos-9-jdk8-cudf`; host setup with `dev/start-cudf.sh`).
- **Native init** (`VeloxBackend::init`, under `GLUTEN_ENABLE_GPU`): when cuDF is enabled, initializes `CudfConfig` (memory resource, memory percent, debug), calls `velox::cudf_velox::registerCudf()`, registers a `CudfVectorStreamOperatorTranslator`, and cuDF Spark scalar/aggregate functions.
- **Operator replacement**: with `spark.gluten.sql.columnar.cudf=true`, Velox replaces supported plan nodes with GPU operators; unsupported ones stay on CPU (with conversions). `CudfPlanValidator` (native) and `CudfNodeValidationRule` (JVM post rule) decide what runs on GPU and tag whole stages (`isCudf`, shown as `Cudf...` node names).
- **Inputs**: stream inputs can use a `CudfValueStreamNode`; a cuDF Hive connector (`cudfHive`) can be used for scans when `...velox.cudf.enableTableScan` is set (scan support is listed as in development in the doc).
- **Shuffle**: `GpuHashShuffleWriterType` / `VeloxGpuShuffleWriter` / `VeloxGpuShuffleReader`, plus `GpuBufferBatchResizeForShuffleInputOutput` and `GpuResizeBufferColumnarBatchExec` for batch handling around exchanges.
- **Concurrency**: a process-wide reentrant GPU lock (`cpp/velox/cudf/GpuLock.h`) serializes GPU access across task threads.
- **Memory**: RMM manages GPU memory globally; it **cannot be aligned with Spark's memory system** (doc). GPU spill is not supported yet.

### 3.2 Support status (from the doc)

| Operator | Status |
|---|---|
| Scan | Not supported (in development) |
| Project | Partial (TPC-H-compatible functions) |
| Filter | Implemented |
| OrderBy | Implemented |
| Aggregation | Partial |
| Join | Partial |
| Spill | Not supported |

The doc also describes **dynamic execution**: schedule IO-bound scan stages on CPU nodes and compute-heavy join stages on GPU nodes (see [VeloxStageResourceAdj.md](../../docs/get-started/VeloxStageResourceAdj.md) for stage resource profile adjustment, applied by `GlutenAutoAdjustStageResourceProfile`).

### 3.3 Configs

| Key | Default | Effect |
|---|---|---|
| `spark.gluten.sql.columnar.cudf` | `false` | Enable cuDF execution |
| `spark.gluten.sql.debug.cudf` | `false` (static, internal) | cuDF debug logging |
| `spark.gluten.sql.columnar.backend.velox.cudf.memoryResource` / `.memoryPercent` | see `VeloxConfig` | RMM configuration |
| `spark.gluten.sql.columnar.backend.velox.cudf.enableTableScan` | see `VeloxConfig` | GPU table scan |
| `spark.gluten.sql.columnar.backend.velox.cudf.enableValidation` | see `VeloxConfig` | Validate plans for GPU |
| `spark.gluten.sql.columnar.backend.velox.cudf.batchSize`, `.shuffleMaxPrefetchBytes` | see `VeloxConfig` | Batch/shuffle tuning |

---

## 4. Flink frontend (experimental)

From [gluten-flink/README.md](../../gluten-flink/README.md) and [gluten-flink/docs/Flink.md](../../gluten-flink/docs/Flink.md):

- **Goal**: accelerate Flink SQL streaming jobs with Velox.
- **Versions**: Flink 1.19.2, JDK 11/17, Scala 2.12.
- **Dependency**: [Velox4j](https://github.com/velox4j/velox4j) (Java bindings for Velox); the doc pins a fork/commit.
- **Structure**:
  - `gluten-flink/planner` — overrides Flink planner exec nodes (e.g. `StreamExecGroupAggregate`, `StreamExecJoin`, `StreamExecWindowAggregate`, `StreamExecRank`, `StreamExecDeduplicate`, `StreamExecWatermarkAssigner`, `CommonExecSink`) to produce native (Velox) operators where possible; `rexnode` converts Flink expressions.
  - `gluten-flink/runtime` — runtime operators wrapping Velox4j execution.
  - `gluten-flink/loader`, `patches`, `ut`, `docs`.
- **Build**: `cd gluten-flink && mvn clean package -Dmaven.test.skip=true`; tests use Nexmark.
- **CI**: `.github/workflows/flink.yml`.

This shows the layering: the **engine** (Velox) is reused, while the **frontend integration** (planner hooks, data exchange, state) is framework-specific.

---

## 5. What's shared vs. backend-specific

| Shared (`gluten-core`, `gluten-substrait`, `gluten-arrow`, `cpp/core`) | Backend-specific |
|---|---|
| Plugin/components, rule injection, heuristic planner | Rule list (`RuleApi`) and extra rules |
| Validation framework, fallback tags/policies, reporter, UI | Native validator, capabilities (`BackendSettingsApi`) |
| Conventions & transitions framework | Batch types & transition implementations |
| Base transformers & Substrait builders | Backend transformers & expression mappings |
| Memory targets, task resources, JNI runtime scaffolding | Native memory manager & arbitration, operator spill |
| Shuffle exchange/dependency/manager framework, partition writer core | Shuffle writers/readers |

---

## 6. End-to-end worked examples

### 6.1 ClickHouse backend: first query

From the `spark-shell` example in [ClickHouse.md](../../docs/get-started/ClickHouse.md). The settings that matter are the plugin, off-heap memory and **where `libch.so` lives**:

```bash
spark-shell \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=6442450944 \
  --conf spark.executorEnv.LD_PRELOAD=/path_to_clickhouse_library/libch.so \
  --conf spark.gluten.sql.columnar.libpath=/path_to_clickhouse_library/libch.so
# ClickHouse.md copies gluten-XXXXX-spark-3.3-jar-with-dependencies.jar into the
# Spark jars dir first
```

```scala
spark.read.parquet("/data/lineitem").groupBy("l_returnflag").count().explain()
```

What to expect: the same `^ ...Transformer` nodes as with Velox, but backend-specific ones carry a `CH` prefix (e.g. `CHHashAggregateExecTransformer`). Scans sit in their **own** stage RDD, because `excludeScanExecFromCollapsedStage = true`. The "Gluten build info" block in the driver log (and in the Gluten UI tab) lists the ClickHouse backend's component info instead of Velox's.

### 6.2 GPU (cuDF): run a filter + sort on the GPU

```bash
# Build (inside apache/gluten:centos-9-jdk8-cudf, after dev/start-cudf.sh on the
# host)
./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF \
  --enable_gpu=ON

spark-shell \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=8g \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --conf spark.gluten.sql.columnar.cudf=true \
  --jars package/target/gluten-velox-bundle-*.jar
```

```scala
spark.read.parquet("/data/orders")
  .where("o_totalprice > 1000")
  .orderBy("o_orderdate")
  .explain()
```

What to expect:
- Stages that `CudfNodeValidationRule` accepts get the **`Cudf` node-name prefix** in EXPLAIN.
- Filter and OrderBy are "Implemented" (§3.2), so they run on the GPU. The scan stays on the CPU (GPU scan is still in development).
- Set `spark.gluten.sql.debug.cudf=true` (static) to log which operators were replaced.
- GPU memory comes from RMM and is **not** counted in Spark's off-heap memory (§3.1). Leave GPU memory headroom yourself.

### 6.3 Flink: run the example job natively

From [Flink.md](../../gluten-flink/docs/Flink.md) (Flink 1.19.2, JDK 11/17):

```bash
# 1. Build velox4j (pinned fork/commit in Flink.md), then gluten-flink
cd gluten-flink && mvn clean package -Dmaven.test.skip=true

# 2. Put the jars in front of Flink's classpath
mkdir -p $FLINK_HOME/gluten_lib
ln -s $VELOX4J_HOME/target/velox4j-0.1.0-SNAPSHOT.jar \
  $FLINK_HOME/gluten_lib/
ln -s $GLUTEN_FLINK_HOME/runtime/target/gluten-flink-runtime-1.7.0.jar \
  $FLINK_HOME/gluten_lib/
ln -s $GLUTEN_FLINK_HOME/loader/target/gluten-flink-loader-1.7.0.jar \
  $FLINK_HOME/gluten_lib/
# ...and prepend them in constructFlinkClassPath() in $FLINK_HOME/bin/config.sh

# 3. Run
cd $FLINK_HOME && bin/start-cluster.sh && bin/flink run \
  examples/table/StreamSQLExample.jar
```

What to expect: an operator named **`gluten-cal`** in the Flink web UI, and results in `log/flink-*-taskexecutor-*.out`. Flink.md notes that this example currently hits an NPE until issue #10315 is fixed.

---

## 7. Configuration reference

- `spark.gluten.sql.columnar.libpath` (ClickHouse): Path to `libch.so`
- `spark.executorEnv.LD_PRELOAD` (ClickHouse): Preload `libch.so` on executors (from ClickHouse.md)
- `spark.gluten.sql.columnar.cudf` (GPU, default `false`): Run supported stages on GPU.
- `spark.gluten.sql.debug.cudf` (GPU, default `false` (static)): Log cuDF operator replacement.
- `spark.gluten.sql.columnar.backend.velox.cudf.*` (GPU, default see `VeloxConfig`): RMM memory, table scan, validation, batch size (§3.3)

## 8. Pitfalls & FAQ

- **Can I load Velox and ClickHouse at the same time?** No. Exactly one Substrait backend is loaded per build/bundle.
- **ClickHouse docs mention `JavaSerializer` and `adaptive.enabled=false`.** Those come from the example launch command in ClickHouse.md. Treat them as the documented starting point, not as requirements of Gluten's core.
- **GPU OOM kills tasks that Spark thinks are fine.** RMM memory isn't tracked by Spark. Lower `...cudf.memoryPercent` or the number of concurrent tasks per GPU.
- **A GPU stage is slower than CPU.** Small batches or many CPU↔GPU conversions (unsupported operators in the middle) hurt. Check for `Cudf` prefixes on *every* node in the stage.
- **Flink jobs don't show `gluten-cal`.** The Gluten jars weren't loaded first. Check the classpath order in `config.sh`.

---

## 9. Code map

| Path | Role |
|---|---|
| `backends-clickhouse/`, [CHBackend.scala](../../backends-clickhouse/src/main/scala/org/apache/gluten/backendsapi/clickhouse/CHBackend.scala) | ClickHouse JVM backend |
| `cpp-ch/local-engine/` | ClickHouse native engine (Parser, Operator, Shuffle, Storages, jni) |
| `cpp/velox/cudf/` ([CudfPlanValidator.cc](../../cpp/velox/cudf/CudfPlanValidator.cc), [GpuLock.h](../../cpp/velox/cudf/GpuLock.h)) | GPU support |
| [VeloxGpuShuffleWriter.cc](../../cpp/velox/shuffle/VeloxGpuShuffleWriter.cc) | GPU shuffle |
| `backends-velox/.../CudfNodeValidationRule.scala` | GPU stage tagging |
| `gluten-flink/` | Flink frontend |

## 10. Related docs

- [00 · Overview](#doc-00) · [01 · Bootstrap](#doc-01) (backend abstraction) · [08 · Shuffle](#doc-08) (GPU writer type)
- Repo: [ClickHouse.md](../../docs/get-started/ClickHouse.md), [VeloxGPU.md](../../docs/get-started/VeloxGPU.md), [Flink.md](../../gluten-flink/docs/Flink.md)

---

<a id="doc-16"></a>

# 16 · Delta Lake & Iceberg Integration

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [15 · Other backends & frontends](#doc-15) · Back to [00 · Overview](#doc-00)

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

Component wiring (both are legacy-transform rules running in the heuristic planner, see [02 · Planning](#doc-02)):

- `VeloxDeltaComponent`, name `velox-delta`, depends on `VeloxBackend`. Active when `io.delta.sql.DeltaSparkSessionExtension` is on the classpath. Rules: transform: `OffloadDeltaScan`, `OffloadDeltaProject`, `OffloadDeltaFilter`; post-transform: `DeltaPostTransformRules.rules`
- `VeloxDelta33WriteComponent` / `VeloxDelta40WriteComponent`, name `velox-delta33-write` / `velox-delta40-write`, depends on `VeloxDeltaComponent`. Active when (inherits) is on the classpath. Rules: transform: `OffloadDeltaCommand`
- `VeloxIcebergComponent`, name `velox-iceberg`, depends on `VeloxBackend`. Active when `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` is on the classpath. Rules: transform: `OffloadIcebergScan`; `OffloadIcebergWrite.inject` (5 write rules)

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

`DeltaScanTransformer` extends `FileSourceScanExecTransformerBase`, the same base as a normal Parquet scan (see [09 · Scan](#doc-09)). It changes three things:

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
- Batches reach the file writer as `BatchCarrierRow`s, so Spark's row-based `FileFormatWriter` API can carry columnar batches. See [05 · Transitions](#doc-05) and [10 · Native write](#doc-10).
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
- Upstream docs list partitioned tables as "Fallback mostly". In practice the scan itself often offloads. Fallback usually comes from expressions around it, e.g. `StaticInvoke` transform functions or unsupported metadata columns. Check the fallback reason in the UI (see [13 · Troubleshooting](#doc-13)).

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
spark.range(0, 1000000).selectExpr("id",
    "cast(id as string) v").write.mode("append").saveAsTable("t")
// writes DVs instead of rewriting files
spark.sql("DELETE FROM t WHERE id % 10 = 0")
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
  TBLPROPERTIES ('delta.columnMapping.mode'='name',
      'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5');
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
-- produces a position delete file
DELETE FROM local.db.t WHERE id < 100;
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
- **How do I see why it fell back?** Use the Gluten UI tab or `spark.gluten.sql.debug`. Fallback reasons such as `fallback Delta _delta_log scan` or `Contains equality delete files` show up there (see [13 · Troubleshooting](#doc-13)).

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

- [09 · Scan & data sources](#doc-09) (generic scan, splits) · [10 · Native write](#doc-10) (write framework, carrier rows) · [01 · Bootstrap](#doc-01) (components) · [02 · Planning](#doc-02) (validators, fallback) · [11 · Expressions](#doc-11) (StaticInvoke mapping)
- Repo: [VeloxDelta.md](../../docs/get-started/VeloxDelta.md), [VeloxIceberg.md](../../docs/get-started/VeloxIceberg.md), [Velox.md](../../docs/get-started/Velox.md)

---
