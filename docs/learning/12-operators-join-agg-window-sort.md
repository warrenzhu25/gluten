# 12 · Operators: Joins, Aggregation, Broadcast, Window, Sort & Others

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [11 · Expressions](11-expressions-functions-udf.md) · Next: [13 · Metrics & troubleshooting](13-metrics-ui-config-troubleshooting.md)

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

| Spark node | Gluten node (EXPLAIN) | Velox node | Spill |
|---|---|---|---|
| `FileSourceScanExec` / `BatchScanExec` | `FileSourceScanExecTransformer` / `BatchScanExecTransformer` | `TableScanNode` | — |
| `FilterExec` | `FilterExecTransformer` | `FilterNode` (or pushed into scan) | — |
| `ProjectExec` | `ProjectExecTransformer` / `ColumnarPartialProjectExec` | `ProjectNode` | — |
| `HashAggregateExec` (partial → exchange) | `FlushableHashAggregateExecTransformer` | `AggregationNode` (partial, flushable) | flush/abandon |
| `HashAggregateExec` (final/complete) | `RegularHashAggregateExecTransformer` | `AggregationNode` | ✅ |
| `SortAggregateExec` | `SortHashAggregateExecTransformer` (EXPLAIN prefixes it with `SortToHash`) | `AggregationNode` | ✅ |
| `ShuffledHashJoinExec` | `ShuffledHashJoinExecTransformer` | `HashJoinNode` | ✅ |
| `SortMergeJoinExec` | `ShuffledHashJoinExecTransformer` (rewritten) or `SortMergeJoinExecTransformer` | `HashJoinNode` / `MergeJoinNode` | ✅ / — |
| `BroadcastHashJoinExec` | `BroadcastHashJoinExecTransformer` | `HashJoinNode` with prebuilt table | — |
| `BroadcastNestedLoopJoinExec` | `VeloxBroadcastNestedLoopJoinExecTransformer` | `NestedLoopJoinNode` | — |
| `CartesianProductExec` | `CartesianProductExecTransformer` | `NestedLoopJoinNode` | — |
| `SortExec` | `SortExecTransformer` | `OrderByNode` | ✅ |
| `TakeOrderedAndProjectExec` | `TakeOrderedAndProjectExecTransformer` | `TopNNode` (+ project) | — |
| `GlobalLimitExec` / `LocalLimitExec` | `LimitExecTransformer` | `LimitNode` | — |
| `WindowExec` | `WindowExecTransformer` | `WindowNode` | ✅ |
| `WindowGroupLimitExec` | `WindowGroupLimitExecTransformer` | `TopNRowNumberNode` / `RowNumberNode` | ✅ |
| `ExpandExec` | `ExpandExecTransformer` | `ExpandNode` | — |
| `GenerateExec` (explode, posexplode, inline, ...) | `GenerateExecTransformer` | `UnnestNode` | — |
| `UnionExec` | `ColumnarUnionExec` (or native union) | `LocalPartitionNode` gather | — |
| `ShuffleExchangeExec` | `ColumnarExchange` | — (native shuffle writer) | ✅ |
| `BroadcastExchangeExec` | `ColumnarBroadcastExchange` | — | — |
| `WriteFilesExec` | `WriteFilesExecTransformer` | `TableWriteNode` | — |
| `ArrowEvalPythonExec` | `ColumnarArrowEvalPythonExec` | — (Arrow IPC to Python) | — |
| `InMemoryTableScanExec` | columnar cache scan | — | — |

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
  SELECT *, row_number() OVER (PARTITION BY store ORDER BY amount DESC) AS rn FROM sales
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
SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
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
               :              +- ^ ShuffledHashJoinExecTransformer Inner BuildLeft (19)
               :                 :- ... ColumnarExchange (6) <- Filter/Scan customer (1)
               :                 +- ... ColumnarExchange (15) <- Filter/Scan orders (10)
               +- ^ InputIteratorTransformer (35)
                  +- ... ColumnarExchange (32) <- Filter/Scan lineitem (27)
```

What changed, and which rule did it:

| Change | Why | Where explained |
|---|---|---|
| `SortMergeJoin` → `ShuffledHashJoinExecTransformer` and all four `Sort`s are gone | `forceShuffledHashJoin=true` (default) lets `RewriteJoin` turn eligible SMJs into hash joins; hash joins don't need sorted input | §3.1 |
| `BuildLeft`: the smaller, filtered side builds the hash table | Build side picked from Spark's plan or from stats (`shuffledHashJoin.optimizeBuildSide`) | §3.1 |
| Two `HashAggregate`s → one `RegularHashAggregateExecTransformer` | The group-by keys include the join key, so there is no shuffle between partial and final aggregation. `MergeTwoPhasesHashBaseAggregate` merges them into one native aggregation | §2.4 |
| `Exchange` → `ColumnarExchange` + `VeloxResizeBatches` | Native columnar shuffle; small batches are merged before shuffle | [08](08-shuffle.md) |
| The top-level join, aggregation and projects form **one native stage** | Shuffle outputs come in through `InputIteratorTransformer` | [02](02-planning-offload-validation-fallback.md) |
| `VeloxColumnarToRow` only at the very top | `collect()` needs rows; there are no other transitions, i.e. **no fallbacks** | [05](05-columnar-batches-conventions-transitions.md) |

**Try it / verify.** Run Q3 on TPC-H data with `spark.sql.autoBroadcastJoinThreshold=-1`, then:

```scala
spark.sql(q3).explain("formatted")    // expect ShuffledHashJoinExecTransformer, no Sort/SortMergeJoin
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

- [02 · Planning](02-planning-offload-validation-fallback.md) · [07 · Spill](07-spill.md) · [08 · Shuffle](08-shuffle.md) · [11 · Expressions](11-expressions-functions-udf.md)
- Repo: [velox-backend-support-progress.md](../../docs/velox-backend-support-progress.md), [velox-backend-aggregate-function-support.md](../../docs/velox-backend-aggregate-function-support.md)
