# Learning Apache Gluten (Velox backend) — Internal Doc Series

**Code base:** public Apache Gluten **v1.7.0** (tag `v1.7.0`).
All class names, file paths, config keys and defaults were checked against that code. Code links point into the repository source tree.

## Reading on Kindle or other e-readers

There are no rendered diagrams. Every diagram is written out as plain text, usually as numbered steps, so the docs read the same on any screen. To make an e-book from the merged file:

```bash
pandoc gluten-learning-complete.md -o gluten-learning.epub --toc --metadata title="Learning Apache Gluten"
```

Then send the `.epub` to your Kindle with Send to Kindle.

## How each doc is organized

TL;DR → why the component exists → key concepts → architecture (explained as numbered steps) → detailed walkthrough with code → end-to-end worked example → config reference (verified defaults) → pitfalls and FAQ → code map → related docs.

## Docs

| # | Doc | Covers |
|---|---|---|
| 00 | [Overview & end-to-end query lifecycle](00-overview-and-query-lifecycle.md) | What Gluten is, architecture, repo map, TPC-H Q1 from SQL to results, glossary, first-week learning path |
| 01 | [Plugin bootstrap, components & backends](01-plugin-bootstrap-components-backends.md) | `GlutenPlugin`, component discovery and DAG, `SubstraitBackend` and its 8 API facets, native init, `Runtime`, `TaskResources`, shims |
| 02 | [Planning: offload, validation & fallback](02-planning-offload-validation-fallback.md) | `GlutenColumnarRule`, 4 rule phases, full Velox rule list, validator chain, native validation, fallback tags and policy, whole-stage grouping |
| 03 | [Substrait & native plan conversion](03-substrait-and-native-plan-conversion.md) | `SubstraitContext`, Rel builders, function signatures, input iterators, splits, Substrait→Velox conversion, debugging the plan |
| 04 | [Native execution runtime & JNI](04-native-execution-runtime-jni.md) | Whole-stage RDDs, `NativePlanEvaluator`, JNI entry points, `WholeStageResultIterator` (serial Velox task), handles, errors, threads, metrics |
| 05 | [Columnar batches, conventions & transitions](05-columnar-batches-conventions-transitions.md) | Light vs. heavy batches, convention types, transition graph, C2R/R2C internals, carrier rows, `VeloxResizeBatches` |
| 06 | [Memory management](06-memory-management.md) | Memory-target tree, reservation listener, `ListenableArbitrator`, one allocation end to end, isolation/dynamic/global modes, sizing |
| 07 | [Spill](07-spill.md) | Spiller phases, `spillTree`, Velox reclaim, shuffle spill, re-entrant spill, spill configs, two worked scenarios |
| 08 | [Columnar shuffle](08-shuffle.md) | Exchange offload, hash/sort/RSS-sort/GPU writers, partition writer, payloads, reader, writer choice, metrics, tuning |
| 09 | [Scan & data sources](09-scan-and-data-sources.md) | Scan transformers, validation, filter pushdown, splits, native readers and IO, cache, soft affinity, lake formats |
| 10 | [Native write path](10-native-write.md) | `WriteFilesExecTransformer`, commit protocol bridge, Parquet writer, support matrix, `INSERT OVERWRITE` example |
| 11 | [Expressions, functions & UDFs](11-expressions-functions-udf.md) | `ExpressionConverter`, mappings, native function registry, ANSI, UDF/UDAF, partial project, adding a function |
| 12 | [Operators](12-operators-join-agg-window-sort.md) | Aggregation (flushable), joins (SHJ/BHJ/SMJ/BNLJ), broadcast hash table reuse, window, sort/TopN, misc operators, table cache |
| 13 | [Metrics, UI, config & troubleshooting](13-metrics-ui-config-troubleshooting.md) | How native metrics reach the UI, Gluten UI, how configs reach native code, logging, troubleshooting decision tree |
| 14 | [Build, test & dev workflow](14-build-test-dev-workflow.md) | Native + Maven builds, profiles, incremental loops, test layers, CI, change-and-test walkthrough |
| 15 | [Other backends & frontends](15-other-backends-and-frontends.md) | ClickHouse backend, cuDF GPU acceleration, Flink frontend, what is shared vs. backend-specific |
| 16 | [Delta Lake & Iceberg integration](16-delta-and-iceberg-integration.md) | Delta/Iceberg components, deletion vectors, column mapping, Iceberg delete files, native write paths, support matrix |

## Suggested reading order

- **New to Gluten:** 00 → 01 → 02 → 04 → 05.
- **Performance work:** 06 → 07 → 08 → 12 → 13.
- **Adding features:** 03 → 11 → 14.
- **Lakehouse tables:** 09 → 10 → 16.

A single-file version of the whole series: [gluten-learning-complete.md](gluten-learning-complete.md).
