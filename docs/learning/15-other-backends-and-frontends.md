# 15 · Other Backends & Frontends: ClickHouse, GPU (cuDF), Flink

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [14 · Build & test](14-build-test-dev-workflow.md) · Next: [16 · Delta & Iceberg](16-delta-and-iceberg-integration.md)

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
# then set spark.plugins=org.apache.gluten.GlutenPlugin and point Gluten to libch.so (see ClickHouse.md)
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
# ClickHouse.md copies gluten-XXXXX-spark-3.3-jar-with-dependencies.jar into the Spark jars dir first
```

```scala
spark.read.parquet("/data/lineitem").groupBy("l_returnflag").count().explain()
```

What to expect: the same `^ ...Transformer` nodes as with Velox, but backend-specific ones carry a `CH` prefix (e.g. `CHHashAggregateExecTransformer`). Scans sit in their **own** stage RDD, because `excludeScanExecFromCollapsedStage = true`. The "Gluten build info" block in the driver log (and in the Gluten UI tab) lists the ClickHouse backend's component info instead of Velox's.

### 6.2 GPU (cuDF): run a filter + sort on the GPU

```bash
# Build (inside apache/gluten:centos-9-jdk8-cudf, after dev/start-cudf.sh on the host)
./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF --enable_gpu=ON

spark-shell \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=8g \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --conf spark.gluten.sql.columnar.cudf=true \
  --jars package/target/gluten-velox-bundle-*.jar
```

```scala
spark.read.parquet("/data/orders").where("o_totalprice > 1000").orderBy("o_orderdate").explain()
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
ln -s $VELOX4J_HOME/target/velox4j-0.1.0-SNAPSHOT.jar           $FLINK_HOME/gluten_lib/
ln -s $GLUTEN_FLINK_HOME/runtime/target/gluten-flink-runtime-1.7.0.jar $FLINK_HOME/gluten_lib/
ln -s $GLUTEN_FLINK_HOME/loader/target/gluten-flink-loader-1.7.0.jar   $FLINK_HOME/gluten_lib/
#    ...and prepend them in constructFlinkClassPath() in $FLINK_HOME/bin/config.sh

# 3. Run
cd $FLINK_HOME && bin/start-cluster.sh && bin/flink run examples/table/StreamSQLExample.jar
```

What to expect: an operator named **`gluten-cal`** in the Flink web UI, and results in `log/flink-*-taskexecutor-*.out`. Flink.md notes that this example currently hits an NPE until issue #10315 is fixed.

---

## 7. Configuration reference

| Key | Default | Applies to | Effect |
|---|---|---|---|
| `spark.gluten.sql.columnar.libpath` | — | ClickHouse | Path to `libch.so` |
| `spark.executorEnv.LD_PRELOAD` | — | ClickHouse | Preload `libch.so` on executors (from ClickHouse.md) |
| `spark.gluten.sql.columnar.cudf` | `false` | GPU | Run supported stages on GPU |
| `spark.gluten.sql.debug.cudf` | `false` (static) | GPU | Log cuDF operator replacement |
| `spark.gluten.sql.columnar.backend.velox.cudf.*` | see `VeloxConfig` | GPU | RMM memory, table scan, validation, batch size (§3.3) |

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

- [00 · Overview](00-overview-and-query-lifecycle.md) · [01 · Bootstrap](01-plugin-bootstrap-components-backends.md) (backend abstraction) · [08 · Shuffle](08-shuffle.md) (GPU writer type)
- Repo: [ClickHouse.md](../../docs/get-started/ClickHouse.md), [VeloxGPU.md](../../docs/get-started/VeloxGPU.md), [Flink.md](../../gluten-flink/docs/Flink.md)
