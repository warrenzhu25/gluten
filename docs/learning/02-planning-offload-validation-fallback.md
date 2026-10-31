# 02 · Query Planning: Offload, Validation & Fallback

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [01 · Bootstrap](01-plugin-bootstrap-components-backends.md) · Next: [03 · Substrait](03-substrait-and-native-plan-conversion.md)

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
| `InsertTransitions.create(outputsColumnar, VeloxBatchType)` | Insert C2R/R2C (and other) transitions where conventions mismatch ([05](05-columnar-batches-conventions-transitions.md)) |

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

Details of each operator: [12 · Operators](12-operators-join-agg-window-sort.md).

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

A `WholeStageTransformer` with leaf scans creates a `GlutenWholeStageColumnarRDD` (first-stage iterator with splits); one without leaf scans creates a `WholeStageZippedPartitionsRDD` that zips its input RDDs (final-stage iterator). See [04](04-native-execution-runtime-jni.md).

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
- Because `spark.gluten.sql.columnar.partial.project=true`, `PartialFallbackRules` can still keep `id + 1` native and evaluate only the UDF in the JVM (a partial project node). See [docs/developers/PartialProject.md](../../docs/developers/PartialProject.md) and [11](11-expressions-functions-udf.md).

**Try it / verify**

- Run either example, then `import org.apache.spark.sql.execution.GlutenImplicits._; df.fallbackSummary()`. `numFallbackNodes` and `fallbackNodeToReason` should match what the example predicts.
- Set `spark.gluten.sql.validation.logLevel=INFO` to see every `Validation failed for plan` line in the driver log.
- Set `spark.gluten.sql.columnar.failOnFallback=true` to turn any fallback into an exception; useful in tests.
- A worked fallback investigation is in [13 §6.1](13-metrics-ui-config-troubleshooting.md).

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

- [03 · Substrait & native plan conversion](03-substrait-and-native-plan-conversion.md) — what `doTransform` produces
- [05 · Transitions](05-columnar-batches-conventions-transitions.md) — `InsertTransitions` in detail
- [11 · Expressions](11-expressions-functions-udf.md) — why expressions fail validation
- [13 · Troubleshooting](13-metrics-ui-config-troubleshooting.md) — fallback debugging workflow
- Repo: [docs/velox-backend-limitations.md](../../docs/velox-backend-limitations.md), [docs/developers/PartialProject.md](../../docs/developers/PartialProject.md)
