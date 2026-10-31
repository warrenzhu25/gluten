# 14 · Build, Test & Development Workflow

> **Series:** Learning Apache Gluten (Velox backend) · **Code base:** Apache Gluten v1.7.0 · Previous: [13 · Troubleshooting](13-metrics-ui-config-troubleshooting.md) · Next: [15 · Other backends](15-other-backends-and-frontends.md)

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

- [13 · Troubleshooting](13-metrics-ui-config-troubleshooting.md) (dumps & replay) · [11 · Expressions](11-expressions-functions-udf.md) (adding functions) · [15 · Other backends](15-other-backends-and-frontends.md)
- Repo: [CONTRIBUTING.md](../../CONTRIBUTING.md), [CppCodingStyle.md](../../docs/developers/CppCodingStyle.md), [HowTo.md](../../docs/developers/HowTo.md)
