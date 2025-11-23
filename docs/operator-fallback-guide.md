# Operator Fallback Guide: Understanding Unsupported Operations in Gluten

## Table of Contents

- [Overview](#overview)
- [Fallback Architecture](#fallback-architecture)
  - [Validation Pipeline](#validation-pipeline)
  - [How Fallback Works](#how-fallback-works)
- [Operator Support Matrix](#operator-support-matrix)
  - [Core Operators](#core-operators)
  - [Join Operations](#join-operations)
  - [Window Functions](#window-functions)
  - [Set Operations](#set-operations)
- [Backend-Specific Limitations](#backend-specific-limitations)
  - [Velox Backend](#velox-backend)
  - [ClickHouse Backend](#clickhouse-backend)
- [Data Type Limitations](#data-type-limitations)
- [Configuration-Based Fallback Control](#configuration-based-fallback-control)
- [Common Fallback Scenarios](#common-fallback-scenarios)
- [Debugging Fallback Issues](#debugging-fallback-issues)
- [Best Practices](#best-practices)
- [Code References](#code-references)

---

## Overview

Gluten uses a sophisticated validation system to determine which Spark SQL operators can be executed natively (offloaded to Velox or ClickHouse) versus those that must fall back to vanilla Spark execution. Understanding this system is crucial for:

- **Query Performance Optimization**: Maximizing native execution reduces overhead
- **Debugging**: Identifying why operators fall back
- **Configuration**: Tuning fallback behavior for your workload
- **Development**: Understanding support boundaries when writing queries

**Key Concept**: Fallback is **automatic and transparent**. When an operator cannot be executed natively, Gluten seamlessly transitions to Spark's standard execution engine without query failure.

---

## Fallback Architecture

### Validation Pipeline

**Location**: `gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/validator/Validators.scala`

Gluten applies a **chain of validators** to each operator in the query plan. Each validator can veto native execution:

```scala
// Validation chain (in order)
1. FallbackByHint          // Explicit fallback tags
2. FallbackIfScanOnly      // Scan-only mode check
3. FallbackComplexExpressions  // Expression complexity check
4. FallbackByBackendSettings   // Backend capability check
5. FallbackByUserOptions   // User configuration check
6. FallbackByTestInjects   // Test framework overrides
7. FallbackByNativeValidation  // Native library validation
```

**Decision Flow**:

```
Operator in Query Plan
    ↓
[FallbackByHint] → Tagged for fallback? → YES → FALLBACK
    ↓ NO
[FallbackIfScanOnly] → Not scan/filter? → YES → FALLBACK
    ↓ NO
[FallbackComplexExpressions] → Depth > threshold? → YES → FALLBACK
    ↓ NO
[FallbackByBackendSettings] → Backend unsupported? → YES → FALLBACK
    ↓ NO
[FallbackByUserOptions] → User disabled? → YES → FALLBACK
    ↓ NO
[FallbackByNativeValidation] → Native fails? → YES → FALLBACK
    ↓ NO
NATIVE EXECUTION ✓
```

### How Fallback Works

**Physical Plan Transformation**:

1. **Planning Phase**: Gluten's columnar rules attempt to transform Spark operators into native equivalents (e.g., `HashAggregateExec` → `HashAggregateExecTransformer`)

2. **Validation Phase**: Each transformer validates whether it can execute natively

3. **Tagging Phase**: Failed validation adds a `FallbackTag` to the operator

4. **Final Transformation**: Tagged operators remain as vanilla Spark operators

5. **Execution Phase**:
   - Native operators execute in Velox/ClickHouse
   - Fallback operators execute in Spark JVM
   - Transitions use `ColumnarToRow` / `RowToColumnar` converters

**Example Physical Plan**:

```
Native Scan (Parquet)
  ↓ ColumnarBatch
Native Filter
  ↓ ColumnarBatch
Native Project
  ↓ ColumnarBatch
ColumnarToRow ← Transition boundary
  ↓ InternalRow
Fallback UDF (Python)
  ↓ InternalRow
RowToColumnar ← Transition boundary
  ↓ ColumnarBatch
Native Aggregate
```

---

## Operator Support Matrix

### Core Operators

| Operator | Velox | ClickHouse | Config Key | Notes |
|----------|-------|------------|------------|-------|
| **Scan** | ✅ | ✅ | `spark.gluten.sql.columnar.filescan` | See data type limitations |
| **BatchScan** | ✅ | ✅ | `spark.gluten.sql.columnar.batchscan` | Data source V2 |
| **HiveTableScan** | ✅ | ✅ | `spark.gluten.sql.columnar.hivetablescan` | Hive catalog tables |
| **Project** | ✅ | ✅ | `spark.gluten.sql.columnar.project` | Column projections |
| **Filter** | ✅ | ✅ | `spark.gluten.sql.columnar.filter` | Predicate pushdown |
| **HashAggregate** | ✅ | ✅ | `spark.gluten.sql.columnar.hashagg` | Group by aggregations |
| **Sort** | ✅ | ✅ | `spark.gluten.sql.columnar.sort` | Order by operations |
| **Limit** | ✅ | ✅ | `spark.gluten.sql.columnar.limit` | Limit/offset |
| **TakeOrderedAndProject** | ✅ | ✅ | `spark.gluten.sql.columnar.takeOrderedAndProject` | Top-N queries |
| **CollectLimit** | ✅ | ✅ | `spark.gluten.sql.columnar.collectLimit` | Driver collection limit |
| **CollectTail** | ✅ | ✅ | `spark.gluten.sql.columnar.collectTail` | Tail operation |
| **Coalesce** | ✅ | ✅ | `spark.gluten.sql.columnar.coalesce` | Partition coalescing |
| **Range** | ✅ | ✅ | `spark.gluten.sql.columnar.range` | Range generation |
| **Sample** | ✅ | ❌ | `spark.gluten.sql.columnar.sample` | Random sampling |

**Default**: All core operators enabled by default except `Sample` (enabled for Velox only)

### Join Operations

| Join Type | Velox | ClickHouse | Config Key | Notes |
|-----------|-------|------------|------------|-------|
| **ShuffledHashJoin** | ✅ | ✅ | `spark.gluten.sql.columnar.shuffledHashJoin` | Hash partitioned join |
| **BroadcastHashJoin** | ✅ | ✅ | `spark.gluten.sql.columnar.broadcastJoin` | Broadcast join |
| **SortMergeJoin** | ✅ | ✅ | `spark.gluten.sql.columnar.sortMergeJoin` | Sort-merge join |
| **CartesianProduct** | ✅ | ❌ | `spark.gluten.sql.cartesianProductTransformerEnabled` | Cross join |
| **BroadcastNestedLoopJoin** | ✅ | ✅ | `spark.gluten.sql.broadcastNestedLoopJoinTransformerEnabled` | BNLJ with predicates |

**Join Type Support by Backend**:

#### Velox Join Types

**Location**: `backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBackend.scala:248-290`

**Hash Build on Left** (build side = left):
- ✅ Inner
- ✅ LeftOuter
- ✅ RightOuter
- ✅ FullOuter

**Hash Build on Right** (build side = right):
- ✅ Inner
- ✅ LeftOuter
- ✅ RightOuter
- ✅ FullOuter
- ✅ LeftSemi
- ✅ LeftAnti
- ✅ ExistenceJoin

**Known Limitation**:
- ⚠️ **LeftSemi with build on left** currently unsupported (pending Velox issue #9980)

**BroadcastNestedLoopJoin**:
- ✅ Supports FullOuter join type (Velox extension)

#### ClickHouse Join Types

- Uses default `BackendSettingsApi` join type support
- No explicit CartesianProduct support

### Window Functions

| Operation | Velox | ClickHouse | Config Key |
|-----------|-------|------------|------------|
| **Window** | ✅ Limited | ✅ Limited | `spark.gluten.sql.columnar.window` |
| **WindowGroupLimit** | ✅ RowNumber only | ✅ | `spark.gluten.sql.columnar.window.group.limit` |

#### Velox Window Function Support

**Location**: `backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBackend.scala:159-220`

**Fully Supported Window Functions**:
- ✅ **RowNumber**: Row numbering within partition
- ✅ **Rank**: Rank with gaps
- ✅ **DenseRank**: Rank without gaps
- ✅ **PercentRank**: Relative rank (0 to 1)
- ✅ **CumeDist**: Cumulative distribution
- ✅ **NTile**: Bucket assignment

**Conditionally Supported**:
- ✅ **NthValue**: Only with **non-foldable** (non-literal) input
- ✅ **Lag**: Only with **non-foldable** input
- ✅ **Lead**: Only with **non-foldable** input
- ✅ **AggregateExpression**: Most aggregates supported (see exclusions)

**Unsupported in Window Context**:
- ❌ **ApproximatePercentile**: Approximate quantiles
- ❌ **Percentile**: Exact quantiles
- ❌ **HyperLogLogPlusPlus**: Cardinality estimation

**Window Frame Limitations**:

```sql
-- ❌ UNSUPPORTED: DESC order with literal bounds
SELECT SUM(value) OVER (
  ORDER BY id DESC
  ROWS BETWEEN 5 PRECEDING AND CURRENT ROW  -- Literal bound
)

-- ✅ SUPPORTED: ASC order with literal bounds
SELECT SUM(value) OVER (
  ORDER BY id ASC
  ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
)

-- ❌ UNSUPPORTED: Non-integral type with literal bounds
SELECT SUM(value) OVER (
  ORDER BY price DESC  -- price is DOUBLE
  RANGE BETWEEN 10.5 PRECEDING AND CURRENT ROW
)

-- ✅ SUPPORTED: Integral types with literal bounds
SELECT SUM(value) OVER (
  ORDER BY id DESC  -- id is INT/LONG
  RANGE BETWEEN 10 PRECEDING AND CURRENT ROW
)
```

**Supported Sort Key Types with Literal Bounds**:
- ✅ ByteType, ShortType, IntegerType, LongType, DateType
- ❌ FloatType, DoubleType, StringType, TimestampType

**WindowGroupLimit**:
```sql
-- ✅ SUPPORTED: RowNumber
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as rn
  FROM products
) WHERE rn <= 10

-- ❌ UNSUPPORTED: Rank or DenseRank
SELECT * FROM (
  SELECT *, RANK() OVER (PARTITION BY category ORDER BY sales DESC) as rnk
  FROM products
) WHERE rnk <= 10  -- Falls back
```

#### ClickHouse Window Function Support

**Location**: `backends-clickhouse/src/main/scala/org/apache/gluten/backendsapi/clickhouse/CHBackend.scala`

**Supported**:
- ✅ RowNumber, Rank, DenseRank, PercentRank, NTile
- ✅ AggregateExpression

**Conditionally Supported**:
- ⚠️ **Lag/Lead**: Only with **literal default values**

```sql
-- ✅ SUPPORTED: Literal default
SELECT LAG(value, 1, 0) OVER (ORDER BY id)  -- default = 0 (literal)

-- ❌ UNSUPPORTED: Non-literal default
SELECT LAG(value, 1, some_column) OVER (ORDER BY id)  -- Falls back
```

### Set Operations

| Operation | Velox | ClickHouse | Config Key |
|-----------|-------|------------|------------|
| **Union** | ✅ | ✅ | `spark.gluten.sql.columnar.union` |
| **Expand** | ✅ | ✅ | `spark.gluten.sql.columnar.expand` |
| **Generate** | ✅ | ✅ | `spark.gluten.sql.columnar.generate` |

**Generate (explode, posexplode, etc.)**:
- Controlled via `spark.gluten.sql.columnar.generate`
- Supports lateral view operations

---

## Backend-Specific Limitations

### Velox Backend

**Location**: `backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBackend.scala`

#### Scan Format Limitations

##### Parquet (`backends-velox/.../VeloxBackend.scala:352-366`)

**Unsupported Features**:
- ❌ **Merge Schema**: `spark.sql.parquet.mergeSchema = true`
  ```scala
  // Falls back when:
  parquetOptions.mergeSchema == true
  ```

- ❌ **Encrypted Parquet Files**: When validation enabled
  ```scala
  // Falls back when:
  // - File is encrypted (Parquet modular encryption)
  // - spark.gluten.sql.enable.native.validation = true
  ```

**Workaround**: Disable native validation or use unencrypted files

##### ORC (`backends-velox/.../VeloxBackend.scala:304-348`)

**Must Enable**: `spark.gluten.sql.columnar.backend.velox.orc.scan.enabled = true`

**Unsupported Nested Types**:
```scala
// ❌ StructType as element in ArrayType
ARRAY<STRUCT<a: INT, b: STRING>>

// ❌ ArrayType as element in ArrayType
ARRAY<ARRAY<INT>>

// ❌ StructType as Key in MapType
MAP<STRUCT<id: INT>, STRING>

// ❌ ArrayType as Value in MapType
MAP<STRING, ARRAY<INT>>

// ❌ TimestampType
STRUCT<ts: TIMESTAMP>  -- in any nested context

// ⚠️ CHAR type (configurable fallback)
// Falls back when: spark.gluten.sql.orc.charType.scan.fallback.enabled = true
```

**Example Fallback**:
```sql
-- ❌ Falls back (array of structs)
SELECT nested_column FROM orc_table
WHERE nested_column is ARRAY<STRUCT<...>>

-- ✅ Native execution (simple array)
SELECT simple_array FROM orc_table
WHERE simple_array is ARRAY<INT>
```

##### CSV/Text
- ✅ Fully supported for basic types
- See expression limitations for complex parsing

#### Write Limitations (`backends-velox/.../VeloxBackend.scala:397-500`)

**Unsupported Data Types**:

```scala
// For ParquetFileFormat:
// ❌ StructType fields

// For all other formats:
// ❌ StructType
// ❌ ArrayType
// ❌ MapType
// ❌ YearMonthIntervalType (all formats)

// ❌ Fields with metadata (except CHAR_VARCHAR_TYPE_STRING)
```

**Example**:
```sql
-- ❌ Falls back: writing StructType to Parquet
CREATE TABLE output USING parquet AS
SELECT struct(id, name) as info FROM source

-- ✅ Native: writing primitive types
CREATE TABLE output USING parquet AS
SELECT id, name, price FROM source
```

**Compression Codec Support**:

```properties
# ✅ Supported codecs
spark.sql.parquet.compression.codec=snappy  # Supported
spark.sql.parquet.compression.codec=gzip    # Supported
spark.sql.parquet.compression.codec=lz4     # Supported
spark.sql.parquet.compression.codec=zstd    # Supported

# ❌ Unsupported codecs (causes fallback)
spark.sql.parquet.compression.codec=brotli  # Falls back
spark.sql.parquet.compression.codec=lzo     # Falls back
spark.sql.parquet.compression.codec=lz4raw  # Falls back
```

**Other Write Constraints**:

```sql
-- ❌ Falls back: maxRecordsPerFile set
spark.sql.files.maxRecordsPerFile=100000

-- ❌ Falls back: Non-Hive-compatible bucketing
CREATE TABLE bucketed (id INT, data STRING)
USING parquet
CLUSTERED BY (id) INTO 10 BUCKETS  -- Hive format required

-- ❌ Falls back: HiveFileFormat with non-Parquet
CREATE TABLE hive_table (id INT)
STORED AS ORC  -- Falls back (HiveFileFormat only supports Parquet)

-- ✅ Native: HiveFileFormat with Parquet
CREATE TABLE hive_table (id INT)
STORED AS PARQUET
```

#### File System Validation

**Location**: `backends-velox/.../VeloxBackend.scala:370-381`

Velox validates file system schemes via JNI:
```scala
VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(paths)
```

**Common Schemes**:
- ✅ `file://` - Local filesystem (always supported)
- ✅ `hdfs://` - HDFS (if registered)
- ✅ `s3://`, `s3a://` - S3 (if registered)
- ✅ `gs://` - Google Cloud Storage (if registered)
- ⚠️ Custom schemes require registration in Velox

**Fallback Behavior**: If any file path uses an unsupported scheme, entire scan falls back.

#### Special Features

**Arrow Columnar UDF**:
- ✅ `supportColumnarArrowUdf(): Boolean = true`
- Velox can execute Arrow-based UDFs natively
- ClickHouse does not support this

---

### ClickHouse Backend

**Location**: `backends-clickhouse/src/main/scala/org/apache/gluten/backendsapi/clickhouse/CHBackend.scala`

#### Unsupported Operators

```scala
// ❌ CartesianProduct not supported
supportCartesianProductExec(): Boolean = false

// ❌ Sample not supported by default
supportSampleExec(): Boolean = false

// ❌ Arrow UDF not supported
supportColumnarArrowUdf(): Boolean = false
```

#### Sort Configuration

```scala
// Sort support is configurable
supportSortExec(): Boolean = GlutenConfig.get.enableColumnarSort
```

**Enable**: `spark.gluten.sql.columnar.sort = true` (default)

#### Scan Type Limitations

**For Text-Based Formats** (CSV, Text):

```scala
// ❌ MapType not supported
// ❌ StructType not supported
// ❌ ArrayType not supported
```

**Example**:
```sql
-- ❌ Falls back: complex type in CSV
SELECT array_column FROM VALUES ('1,2,3') AS t(array_column)

-- ✅ Native: primitive types in CSV
SELECT id, name, price FROM csv_table
```

**Supported Formats**:
- ✅ Parquet
- ✅ ORC
- ✅ MergeTree (ClickHouse native format)
- ✅ JSON
- ✅ Kafka

#### Write Limitations

```scala
// ❌ maxRecordsPerFile > 0
// ❌ Fields with metadata (except CHAR_VARCHAR_TYPE_STRING)
```

**Supported Write Formats**:
- ✅ ParquetFileFormat
- ✅ OrcFileFormat

---

## Data Type Limitations

### Overview

Data type support varies by:
1. **Backend** (Velox vs ClickHouse)
2. **Operation** (Scan vs Write vs Compute)
3. **Format** (Parquet vs ORC vs CSV)

### Velox Data Type Matrix

| Type | Scan (Parquet) | Scan (ORC) | Write | Compute |
|------|---------------|-----------|-------|---------|
| **Primitives** |
| INT, LONG, DOUBLE, etc. | ✅ | ✅ | ✅ | ✅ |
| STRING, BINARY | ✅ | ✅ | ✅ | ✅ |
| DECIMAL | ✅ | ✅ | ✅ | ✅ |
| DATE | ✅ | ✅ | ✅ | ✅ |
| TIMESTAMP | ✅ | ❌ (nested) | ✅ | ✅ |
| **Complex Types** |
| ARRAY<primitive> | ✅ | ✅ | ❌ (non-Parquet) | ✅ |
| ARRAY<STRUCT> | ✅ | ❌ | ❌ | ✅ |
| ARRAY<ARRAY> | ✅ | ❌ | ❌ | ✅ |
| MAP<primitive,primitive> | ✅ | ✅ | ❌ (non-Parquet) | ✅ |
| MAP<STRUCT,...> | ✅ | ❌ | ❌ | ✅ |
| MAP<...,ARRAY> | ✅ | ❌ | ❌ | ✅ |
| STRUCT<primitives> | ✅ | ✅ | ❌ (write) | ✅ |
| **Interval Types** |
| YearMonthInterval | ✅ | ✅ | ❌ | ✅ |
| DayTimeInterval | ✅ | ✅ | ✅ | ✅ |
| **Special** |
| CHAR | ✅ | ⚠️ (config) | ⚠️ (metadata) | ✅ |
| VARCHAR | ✅ | ✅ | ⚠️ (metadata) | ✅ |

### ClickHouse Data Type Matrix

| Type | Scan (Parquet) | Scan (Text) | Write | Compute |
|------|---------------|------------|-------|---------|
| **Primitives** | ✅ | ✅ | ✅ | ✅ |
| **Complex** | ✅ | ❌ | ⚠️ | ✅ |

**Note**: ClickHouse has broader compute support but text format limitations.

---

## Configuration-Based Fallback Control

### Individual Operator Control

**Location**: `gluten-substrait/src/main/scala/org/apache/gluten/config/GlutenConfig.scala`

Every operator can be individually enabled/disabled:

```properties
# Scan Operations (default: true)
spark.gluten.sql.columnar.filescan=true
spark.gluten.sql.columnar.batchscan=true
spark.gluten.sql.columnar.hivetablescan=true

# Core Operations (default: true)
spark.gluten.sql.columnar.project=true
spark.gluten.sql.columnar.filter=true
spark.gluten.sql.columnar.hashagg=true
spark.gluten.sql.columnar.sort=true
spark.gluten.sql.columnar.window=true
spark.gluten.sql.columnar.limit=true

# Join Operations (default: true)
spark.gluten.sql.columnar.shuffledHashJoin=true
spark.gluten.sql.columnar.sortMergeJoin=true
spark.gluten.sql.columnar.broadcastJoin=true
spark.gluten.sql.cartesianProductTransformerEnabled=true
spark.gluten.sql.broadcastNestedLoopJoinTransformerEnabled=true

# Force Hash Join (default: true)
spark.gluten.sql.columnar.forceShuffledHashJoin=true
# When true, prefers hash join over sort-merge join

# Data Movement (default: true)
spark.gluten.sql.columnar.shuffle=true
spark.gluten.sql.columnar.broadcastExchange=true

# Set Operations (default: true)
spark.gluten.sql.columnar.union=true
spark.gluten.sql.columnar.expand=true

# Other Operations
spark.gluten.sql.columnar.generate=true        # explode, etc. (default: true)
spark.gluten.sql.columnar.sample=false         # Velox: true, CH: false
spark.gluten.sql.columnar.coalesce=true
spark.gluten.sql.columnar.range=true

# Write Operations (default: true)
spark.gluten.sql.columnar.appendData=true
spark.gluten.sql.columnar.replaceData=true
spark.gluten.sql.columnar.overwriteByExpression=true
spark.gluten.sql.columnar.overwritePartitionsDynamic=true

# UDFs (default: true)
spark.gluten.sql.columnar.arrowUdf=true        # Velox only
```

### Expression Blacklisting

```properties
# Blacklist specific expressions by class name
spark.gluten.expression.blacklist=Cast,Add,Substring

# Fallback regexp expressions
spark.gluten.sql.fallbackRegexpExpressions=false  # default: false
# When true, falls back: rlike, regexp_replace, regexp_extract,
#                        regexp_extract_all, split
```

**Example**:
```sql
-- With spark.gluten.expression.blacklist=Cast
SELECT CAST(id AS STRING) FROM table  -- Falls back due to Cast

-- With spark.gluten.sql.fallbackRegexpExpressions=true
SELECT * FROM table WHERE name RLIKE '^[A-Z].*'  -- Falls back
```

### Fallback Thresholds

```properties
# Expression Complexity Threshold
spark.gluten.sql.columnar.fallback.expressions.threshold=50  # default: 50
# Falls back if expression tree depth exceeds threshold

# Query-Level Fallback Threshold
spark.gluten.sql.columnar.query.fallback.threshold=-1  # default: -1 (disabled)
# Falls back entire query if fallback node count exceeds threshold
# Example: Set to 10 to fallback entire query if >10 operators can't go native

# Whole-Stage Fallback Threshold
spark.gluten.sql.columnar.wholeStage.fallback.threshold=-1  # default: -1 (disabled)
# Falls back whole code-gen stage if fallback node count exceeds threshold
```

**Example Usage**:
```properties
# Fall back entire query if >5 operators must fall back
spark.gluten.sql.columnar.query.fallback.threshold=5

# Reduces overhead from frequent native ↔ Spark transitions
```

### Special Modes

#### Scan-Only Mode

```properties
spark.gluten.sql.columnar.scanOnly=false  # default: false
```

**When enabled**: Only scan operations and filters directly after scans are offloaded.

**Use Case**: Debugging, or when only I/O acceleration is desired.

**Example**:
```sql
-- With scanOnly=true:
SELECT *                  -- ✅ Native Scan
FROM parquet_table
WHERE id > 100            -- ✅ Native Filter (directly after scan)
GROUP BY category         -- ❌ Falls back (aggregation)
```

#### ANSI Mode Fallback

```properties
spark.sql.ansi.enabled=false              # Spark ANSI SQL mode
spark.gluten.sql.ansiFallback.enabled=true  # default: true
```

**Behavior**:
- When `spark.sql.ansi.enabled=true` AND `ansiFallback.enabled=true`:
  - **Entire query falls back** to ensure ANSI compliance

**Reason**: ANSI mode changes semantics (e.g., overflow handling, null behavior) that may differ between Spark and native backends.

**Disable Fallback** (use at your own risk):
```properties
spark.sql.ansi.enabled=true
spark.gluten.sql.ansiFallback.enabled=false  # Allow native execution
```

#### Native Validation

```properties
spark.gluten.sql.enable.native.validation=true  # default: true
```

**When enabled**: Each operator's Substrait plan is validated by the native library before execution.

**When disabled**: Only Scala-side validation occurs (faster planning, but may fail at execution).

**Use Case**: Disable for performance in well-tested workloads.

---

## Common Fallback Scenarios

### Scenario 1: Complex Nested Expressions

**Trigger**: Expression nesting depth exceeds threshold

**Example**:
```sql
SELECT
  CASE
    WHEN CASE
      WHEN CASE
        WHEN ... (50+ nested levels)
        THEN 1
      END = 1
    THEN 2
  END as deeply_nested
FROM table
```

**Diagnosis**:
```
FallbackTag: Expression tree depth (52) exceeds threshold (50)
```

**Solutions**:

**Option A**: Increase threshold
```properties
spark.gluten.sql.columnar.fallback.expressions.threshold=100
```

**Option B**: Simplify query (recommended)
```sql
-- Break into CTEs
WITH step1 AS (
  SELECT ..., intermediate_calc1 FROM table
),
step2 AS (
  SELECT ..., intermediate_calc2 FROM step1
)
SELECT * FROM step2
```

### Scenario 2: Unsupported Data Types in ORC

**Trigger**: Array of structs in ORC file

**Example**:
```sql
-- Schema: events ARRAY<STRUCT<timestamp: TIMESTAMP, value: DOUBLE>>
SELECT events FROM orc_table WHERE date = '2024-01-01'
```

**Diagnosis**:
```
FallbackTag: ORC scan with ARRAY<STRUCT> not supported by Velox backend
```

**Solutions**:

**Option A**: Use Parquet format
```sql
-- Convert to Parquet (supported)
CREATE TABLE parquet_table USING parquet AS SELECT * FROM orc_table
```

**Option B**: Flatten nested structure
```sql
-- Explode and flatten
SELECT
  date,
  event.timestamp,
  event.value
FROM orc_table
LATERAL VIEW explode(events) AS event  -- Explode handled by Spark
```

**Option C**: Accept fallback (ORC scan falls back, rest may be native)

### Scenario 3: Window Function with DESC + Literal Bounds

**Trigger**: Velox limitation with DESC order and literal frame bounds

**Example**:
```sql
SELECT
  id,
  SUM(sales) OVER (
    ORDER BY date DESC
    ROWS BETWEEN 7 PRECEDING AND CURRENT ROW  -- Literal bound
  ) as rolling_7day
FROM sales
```

**Diagnosis**:
```
FallbackTag: Window with DESC order and literal bound not supported
```

**Solutions**:

**Option A**: Use ASC order (if semantics allow)
```sql
SELECT
  id,
  SUM(sales) OVER (
    ORDER BY date ASC  -- Changed to ASC
    ROWS BETWEEN CURRENT ROW AND 7 FOLLOWING
  ) as rolling_7day
FROM sales
```

**Option B**: Use non-literal bounds (unbounded)
```sql
SELECT
  id,
  SUM(sales) OVER (
    ORDER BY date DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  -- Non-literal
  ) as cumulative
FROM sales
```

**Option C**: Accept fallback for this operation

### Scenario 4: Encrypted Parquet Files

**Trigger**: Reading encrypted Parquet files with native validation enabled

**Example**:
```sql
SELECT * FROM encrypted_parquet_table
```

**Diagnosis**:
```
FallbackTag: Encrypted Parquet not supported with native validation
```

**Solutions**:

**Option A**: Disable native validation (if safe)
```properties
spark.gluten.sql.enable.native.validation=false
```

**Option B**: Decrypt files before reading
```bash
# Use Parquet tools to decrypt
parquet-tools decrypt --input encrypted.parquet --output decrypted.parquet
```

**Option C**: Accept fallback

### Scenario 5: Python UDF

**Trigger**: Python UDF in query

**Example**:
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Define Python UDF
@udf(returnType=IntegerType())
def complex_calc(x):
    return x * 2 + 1

df = spark.table("source")
df.withColumn("result", complex_calc(df.value)).show()
```

**Diagnosis**:
```
FallbackTag: Python UDF cannot be offloaded to native execution
```

**Solutions**:

**Option A**: Use Arrow-based UDF (Velox only)
```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(IntegerType())
def complex_calc_arrow(x: pd.Series) -> pd.Series:
    return x * 2 + 1

# Enable Arrow UDF
spark.conf.set("spark.gluten.sql.columnar.arrowUdf", "true")
df.withColumn("result", complex_calc_arrow(df.value)).show()
```

**Option B**: Implement as expression
```sql
-- Replace UDF with native expression
SELECT value * 2 + 1 as result FROM source
```

**Option C**: Accept fallback for UDF (rest of query may be native)

### Scenario 6: ANSI Mode Enabled

**Trigger**: ANSI SQL mode enabled with default fallback settings

**Example**:
```sql
-- Enable ANSI mode
SET spark.sql.ansi.enabled=true;

SELECT value / 0 FROM table  -- ANSI: throws error instead of NULL
```

**Diagnosis**:
```
FallbackTag: ANSI mode enabled, falling back to ensure compliance
```

**Solutions**:

**Option A**: Disable ANSI mode
```sql
SET spark.sql.ansi.enabled=false;
```

**Option B**: Disable ANSI fallback (use with caution)
```properties
spark.gluten.sql.ansiFallback.enabled=false
```
⚠️ **Warning**: Native execution may have different ANSI compliance behavior

**Option C**: Accept fallback for ANSI-sensitive queries

### Scenario 7: Write with maxRecordsPerFile

**Trigger**: Setting maxRecordsPerFile causes write fallback

**Example**:
```sql
-- Configure max records per file
SET spark.sql.files.maxRecordsPerFile=1000000;

-- Write query
CREATE TABLE output USING parquet AS
SELECT * FROM large_table
```

**Diagnosis**:
```
FallbackTag: maxRecordsPerFile not supported for native write
```

**Solutions**:

**Option A**: Remove maxRecordsPerFile constraint
```sql
SET spark.sql.files.maxRecordsPerFile=0;  -- Disable
```

**Option B**: Control file size via partitions instead
```sql
CREATE TABLE output USING parquet
PARTITIONED BY (year, month)
AS SELECT * FROM large_table
```

**Option C**: Accept fallback for write operation

---

## Debugging Fallback Issues

### Enable Fallback Logging

**Add to `log4j.properties`**:
```properties
# Gluten validation logging
log4j.logger.org.apache.gluten.extension.columnar.validator=DEBUG
log4j.logger.org.apache.gluten.extension.columnar.FallbackTag=DEBUG

# Plan transformation logging
log4j.logger.org.apache.gluten.extension.columnar.TransformHints=DEBUG
```

### Check Physical Plan

**Scala/PySpark**:
```python
df = spark.sql("SELECT ...")

# Show physical plan with fallback tags
df.explain(mode="extended")

# Look for:
# - "*Exec" (vanilla Spark operators - fell back)
# - "*ExecTransformer" or "*Transformer" (native operators)
# - "ColumnarToRow" / "RowToColumnar" (transition points)
```

**Example Output**:
```
== Physical Plan ==
*(1) Project [id#10, name#11]
+- NativeFileScan parquet [id#10,name#11]  ← Native
   ↓
ColumnarToRow  ← Transition boundary
   ↓
*(2) Project [python_udf(name#11)]  ← Fell back (Python UDF)
   ↓
RowToColumnar  ← Transition boundary
   ↓
HashAggregateTransformer [...]  ← Native
```

### Check Fallback Tags

**Enable validation result tracking**:
```scala
// In application code
import org.apache.gluten.extension.columnar.FallbackTags

// After validation
val plan = df.queryExecution.executedPlan
val fallbackInfo = FallbackTags.get(plan)
println(s"Fallback reason: ${fallbackInfo.map(_.reason).getOrElse("N/A")}")
```

### Common Log Messages

**Expression complexity**:
```
DEBUG FallbackComplexExpressions: Expression tree depth 52 exceeds threshold 50
```

**Backend settings**:
```
DEBUG FallbackByBackendSettings: Operator CartesianProductExec not supported by ClickHouse backend
```

**Data type unsupported**:
```
DEBUG FallbackByNativeValidation: ORC scan validation failed: ARRAY<STRUCT> not supported
```

**User configuration**:
```
DEBUG FallbackByUserOptions: Window operator disabled by spark.gluten.sql.columnar.window=false
```

### Spark UI Analysis

**Navigate to**: Spark UI → SQL Tab → Query Details

**Look for**:
1. **Fallback Indicators**:
   - Operator names without "Transformer" suffix
   - "ColumnarToRow" / "RowToColumnar" operators

2. **Stage Metrics**:
   - Tasks with high GC time (may indicate fallback to Spark)
   - Stage duration variance

3. **Columnar Support**:
   - Check "Code Generation" field
   - Native operators show "Columnar: true"

---

## Best Practices

### 1. Minimize Fallback Boundaries

**Bad** (multiple transitions):
```sql
SELECT
  native_agg(...),         -- Native
  python_udf(col1),        -- Falls back
  native_expr(col2),       -- Native (requires transition back)
  another_udf(col3)        -- Falls back again
FROM table
```

**Good** (group fallback operations):
```sql
-- Stage 1: All native
SELECT native_agg(...), native_expr(col2), col1, col3
FROM table

-- Stage 2: All fallback
SELECT python_udf(col1), another_udf(col3), ...
FROM stage1
```

**Benefit**: Reduces overhead from ColumnarToRow/RowToColumnar conversions.

### 2. Use Supported Data Types

**Recommendation**: Prefer Parquet over ORC for complex nested types with Velox.

```sql
-- Prefer this schema (flatter structure)
CREATE TABLE events (
  event_time TIMESTAMP,
  event_type STRING,
  user_id BIGINT,
  value DOUBLE
) USING parquet

-- Over this (complex nesting)
CREATE TABLE events (
  events ARRAY<STRUCT<
    event_time: TIMESTAMP,
    event_type: STRING,
    user_id: BIGINT,
    value: DOUBLE
  >>
) USING orc
```

### 3. Configure Fallback Thresholds Appropriately

**For production workloads**:
```properties
# Allow moderate expression complexity
spark.gluten.sql.columnar.fallback.expressions.threshold=75

# Fall back entire query if >20% operators can't go native
spark.gluten.sql.columnar.query.fallback.threshold=20
```

**Rationale**: Avoid thrashing between native and fallback execution.

### 4. Use Native-Friendly Expressions

**Prefer**:
```sql
-- Native expressions
SELECT
  col1 + col2,
  SUBSTRING(name, 1, 10),
  DATE_ADD(date_col, 7)
FROM table
```

**Avoid**:
```sql
-- UDFs (unless Arrow-based)
SELECT
  my_custom_udf(col1),
  python_udf(col2)
FROM table
```

### 5. Test Queries After Configuration Changes

**Workflow**:
1. Change configuration (e.g., disable an operator)
2. Run representative queries
3. Check `EXPLAIN EXTENDED` for unexpected fallbacks
4. Review Spark UI for performance changes
5. Adjust configuration if needed

### 6. Monitor Fallback Ratio

**Metric to track**:
```
Fallback Ratio = (# Fallback Operators) / (# Total Operators)
```

**Goal**: Keep fallback ratio < 20% for optimal performance.

**Tool**: Parse physical plans or use Spark metrics to track.

### 7. Document Known Fallbacks

**For your workload**, maintain a document:
```markdown
# Known Fallbacks in Our Workload

1. **Python UDFs** in ETL pipeline
   - Reason: Legacy business logic
   - Impact: ~15% of query time
   - Plan: Migrate to Arrow UDFs (Q2 2024)

2. **Window functions with DESC**
   - Queries: dashboard_aggregates.sql
   - Workaround: Rewritten to use ASC + reorder results
```

---

## Code References

### Validation Framework
- **Validator Interface**: `gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/validator/Validator.scala`
- **Validators Collection**: `gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/validator/Validators.scala:37-296`
- **ValidationResult**: `gluten-substrait/src/main/scala/org/apache/gluten/execution/ValidationResult.scala`
- **FallbackTag**: `gluten-core/src/main/scala/org/apache/gluten/extension/columnar/FallbackTag.scala`

### Backend Settings
- **Backend API**: `gluten-substrait/src/main/scala/org/apache/gluten/backendsapi/BackendSettingsApi.scala:35-166`
- **Velox Backend**: `backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxBackend.scala:90-549`
  - Window functions: Lines 159-220
  - Join types: Lines 248-290
  - ORC limitations: Lines 304-348
  - Parquet limitations: Lines 352-366
  - Write limitations: Lines 397-500
- **ClickHouse Backend**: `backends-clickhouse/src/main/scala/org/apache/gluten/backendsapi/clickhouse/CHBackend.scala:79-399`

### Configuration
- **GlutenConfig**: `gluten-substrait/src/main/scala/org/apache/gluten/config/GlutenConfig.scala:52-1566`
- **Expression Mappings**: `gluten-substrait/src/main/scala/org/apache/gluten/expression/ExpressionMappings.scala`

### Test Settings (Documented Limitations)
- **Velox Test Settings**: `backends-velox/src/test/scala/org/apache/gluten/utils/velox/VeloxTestSettings.scala`
  - Contains extensive documentation of known issues and workarounds

---

## Summary

Gluten's fallback system provides **transparent and automatic** handling of unsupported operations, allowing queries to execute even when parts cannot be offloaded. Key takeaways:

1. **Validation is Multi-Layered**: Hint tags, backend capabilities, user config, and native validation all contribute

2. **Backend Differences Matter**: Velox and ClickHouse have different support matrices (e.g., CartesianProduct, Sample, Arrow UDF)

3. **Data Types Are Critical**: Complex nested types, especially in ORC with Velox, frequently trigger fallback

4. **Window Functions Have Limitations**: Frame specs with DESC order and literal bounds are problematic in Velox

5. **Configuration Provides Control**: Every operator can be individually disabled, and thresholds can be tuned

6. **Minimize Transitions**: Group fallback operations together to reduce ColumnarToRow/RowToColumnar overhead

7. **Monitor and Iterate**: Track fallback ratio, review physical plans, and adjust queries/configuration over time

For specific function support details, refer to:
- `velox-backend-scalar-function-support.md`
- `velox-backend-aggregate-function-support.md`
- `velox-backend-window-function-support.md`
- `velox-backend-limitations.md`
