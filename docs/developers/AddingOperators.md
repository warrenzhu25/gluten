---
layout: page
title: Adding Operators to Gluten
nav_order: 3
parent: Developer Overview
---

# Guide for Adding Operator Support to Gluten

This comprehensive guide walks you through the complete process of adding a new operator to Apache Gluten, from initial research to PR submission. It includes multiple end-to-end examples progressing from simple to complex implementations.

## Table of Contents
- [1. Introduction](#1-introduction)
- [2. Prerequisites](#2-prerequisites)
- [3. Architecture Overview](#3-architecture-overview)
- [4. Development Process](#4-development-process)
- [5. Example 1: Simple Unary Operator (Limit)](#5-example-1-simple-unary-operator-limit)
- [6. Example 2: Expression-Based Operator (Project)](#6-example-2-expression-based-operator-project)
- [7. Example 3: Complex Operator (HashAggregate)](#7-example-3-complex-operator-hashaggregate)
- [8. Backend Implementation Details](#8-backend-implementation-details)
- [9. Testing Guide](#9-testing-guide)
- [10. Build and Troubleshooting](#10-build-and-troubleshooting)
- [11. PR Submission Checklist](#11-pr-submission-checklist)
- [12. Quick Reference](#12-quick-reference)

---

## 1. Introduction

### 1.1 What is an Operator in Gluten?

In Gluten, an **operator** represents a physical execution unit in Spark's query plan that has been offloaded to a native execution engine (Velox or ClickHouse). Each operator:

- **Transforms**: Converts Spark's physical plan nodes into Substrait representation
- **Validates**: Checks if the native backend supports the operation
- **Executes**: Runs natively using columnar data format
- **Reports**: Provides metrics back to Spark UI

### 1.2 The Transformation Pipeline

The operator transformation follows this flow:

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Spark Physical  │────▶│ Gluten          │────▶│ Substrait       │────▶│ Native Engine   │
│ Plan (Row)      │     │ Transformer     │     │ Plan (Proto)    │     │ (Velox/CH)      │
└─────────────────┘     └──────────────────┘     └─────────────────┘     └─────────────────┘
                              ▲                                                   │
                              │                                                   │
                              └───────────────── Validation ─────────────────────┘
```

**Key Stages:**
1. **Planning**: Spark creates a physical plan with row-based operators
2. **Transformation**: Gluten rules replace operators with "Transformer" versions
3. **Validation**: Each transformer checks if native backend supports the operation
4. **Substrait Generation**: Transformers generate Substrait plan (cross-engine IR)
5. **Native Execution**: Backend converts Substrait to native execution
6. **Result Return**: Columnar batches are returned to Spark

### 1.3 Why Add Operator Support?

You might need to add operator support when:
- A Spark operator currently falls back to vanilla Spark execution
- A new Spark version introduces a new operator
- Backend engines add new capabilities
- Performance optimizations require new execution patterns

---

## 2. Prerequisites

### 2.1 Required Knowledge

Before implementing operators, you should understand:

**Scala & Spark Internals:**
- Spark physical plan and execution model
- Catalyst expressions and their lifecycle
- Case classes and pattern matching
- Implicit conversions

**Substrait Specification:**
- Substrait plan structure and RelNodes
- Expression representation
- Type system mapping

**Backend Engines (for full implementation):**
- Velox: C++ execution engine, operator API
- ClickHouse: C++ backend architecture

**Build Tools:**
- Maven for building Java/Scala code
- CMake for C++ code
- Git for version control

### 2.2 Development Environment Setup

Ensure you have:
- **JDK**: 8, 11, or 17 (17 recommended for future Spark 4.0)
- **Maven**: 3.6.3 or above
- **GCC**: 11 or above
- **IDE**: IntelliJ IDEA or VS Code
- **Clang-format**: Version 15 (for C++ code formatting)

Follow [NewToGluten.md](NewToGluten.md) for detailed environment setup.

### 2.3 Build Gluten

```bash
# Clone repository
git clone https://github.com/apache/incubator-gluten.git
cd incubator-gluten

# Build Velox backend with tests (Debug mode for development)
./dev/builddeps-veloxbe.sh --build_tests=ON --build_benchmarks=ON --build_type=Debug

# Build Gluten Java/Scala code
./dev/build.sh --backends-velox
```

---

## 3. Architecture Overview

### 3.1 Component Hierarchy

```
┌────────────────────────────────────────────────────────────────┐
│                     Spark Physical Plan                        │
└───────────────────────────┬────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────┐
│              Gluten Transformation Rules                       │
│  (ColumnarOverrides, InsertTransitions, etc.)                  │
└───────────────────────────┬────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────┐
│                    Transformer Classes                         │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │              TransformSupport (Trait)                     │ │
│  │  - doValidateInternal(): ValidationResult                 │ │
│  │  - doTransform(SubstraitContext): TransformContext        │ │
│  │  - metricsUpdater(): MetricsUpdater                       │ │
│  └────────────────┬───────────────────┬─────────────────────┘ │
│                   │                   │                        │
│    ┌──────────────▼─────┐  ┌─────────▼───────────┐           │
│    │ UnaryTransform     │  │ BinaryTransform      │           │
│    │ Support            │  │ Support              │           │
│    │ (1 child)          │  │ (2 children)         │           │
│    └──────────┬─────────┘  └────────┬─────────────┘           │
│               │                     │                          │
│  ┌────────────▼──────────────────────▼─────────────────────┐  │
│  │  Concrete Transformers:                                  │  │
│  │  - LimitExecTransformer                                  │  │
│  │  - FilterExecTransformer                                 │  │
│  │  - ProjectExecTransformer                                │  │
│  │  - SortExecTransformer                                   │  │
│  │  - HashAggregateExecTransformer                          │  │
│  │  - HashJoinExecTransformer                               │  │
│  └──────────────────────────────────────────────────────────┘  │
└───────────────────────────┬────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────┐
│                   Substrait Generation                         │
│  RelBuilder.makeXXXRel() → Substrait Proto                     │
└───────────────────────────┬────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────┐
│                      JNI Boundary                              │
└───────────────────────────┬────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────┐
│                   Backend Native Code                          │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  Velox: SubstraitToVeloxPlan::toVeloxPlan()              │ │
│  │  ClickHouse: SubstraitPlanParser::parse()                │ │
│  └──────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────┘
```

### 3.2 Key Base Traits and Classes

#### TransformSupport
The root trait for all operators that can be transformed to native execution.

**Location:** `gluten-substrait/src/main/scala/org/apache/gluten/execution/WholeStageTransformer.scala:59`

**Key Methods:**
```scala
trait TransformSupport extends ValidatablePlan {
  // Validation phase - check if native backend supports this operation
  protected def doValidateInternal(): ValidationResult

  // Transformation phase - convert to Substrait plan
  protected def doTransform(context: SubstraitContext): TransformContext

  // Metrics for Spark UI
  def metricsUpdater(): MetricsUpdater

  // Final validation result (cached)
  final def doValidate(): ValidationResult

  // Final transformation (cached)
  final def transform(context: SubstraitContext): TransformContext

  // Batch type for columnar execution
  override def batchType(): Convention.BatchType

  // Node name for debugging and metrics
  override def nodeName: String
}
```

**Important Notes:**
- `doValidateInternal()` is called during planning - must be fast and lightweight
- `doTransform()` is called during execution - can be more expensive
- Both methods are cached, so they're only called once per operator instance
- Actual implementations are in concrete transformer classes (see `LimitExecTransformer.scala:29`)

#### UnaryTransformSupport
For operators with a single child (Filter, Project, Sort, Limit, etc.)

```scala
abstract class UnaryTransformSupport extends UnaryExecNode with TransformSupport {
  def child: SparkPlan  // Single input
}
```

#### BinaryTransformSupport
For operators with two children (Joins, Set operations, etc.)

```scala
abstract class BinaryTransformSupport extends BinaryExecNode with TransformSupport {
  def left: SparkPlan
  def right: SparkPlan
}
```

#### LeafTransformSupport
For source operators with no children (Scans, etc.)

```scala
abstract class LeafTransformSupport extends LeafExecNode with TransformSupport
```

### 3.3 Backend API Abstraction

Gluten uses a backend abstraction layer to support multiple native engines:

```scala
// Trait defining all operator creation methods
trait SparkPlanExecApi {
  def genProjectExecTransformer(
    projectList: Seq[NamedExpression],
    child: SparkPlan): ProjectExecTransformer

  def genFilterExecTransformer(
    condition: Expression,
    child: SparkPlan): FilterExecTransformerBase

  // ... other operators
}

// Backend-specific implementations
// backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxSparkPlanExecApi.scala
class VeloxSparkPlanExecApi extends SparkPlanExecApi { ... }

// backends-clickhouse/src/main/scala/org/apache/gluten/backendsapi/clickhouse/CHSparkPlanExecApi.scala
class CHSparkPlanExecApi extends SparkPlanExecApi { ... }

// Usage via BackendsApiManager
val transformer = BackendsApiManager.getSparkPlanExecApiInstance
  .genProjectExecTransformer(projectList, child)
```

### 3.4 Validation vs Transformation

Operators go through **two distinct phases**:

**Phase 1: Validation (Planning Time)**
- Happens during Spark's query planning
- Checks if the operator CAN be offloaded to native
- No actual child transformations
- Uses dummy/mock input nodes
- Fast - must not be expensive
- Returns `ValidationResult.succeeded` or `ValidationResult.failed`

**Phase 2: Transformation (Execution Time)**
- Happens when Spark executes the query
- Recursively transforms children first
- Builds actual Substrait plan
- Connects to child nodes
- Returns `TransformContext` with output schema and RelNode

```scala
// Validation - lightweight check
override protected def doValidateInternal(): ValidationResult = {
  val context = new SubstraitContext
  val operatorId = context.nextOperatorId(this.nodeName)
  // Use null for input (not validating children yet) or mock input
  val relNode = getRelNode(context, operatorId, ..., input = null, validation = true)
  doNativeValidation(context, relNode)
}

// Transformation - actual execution
override protected def doTransform(context: SubstraitContext): TransformContext = {
  // Transform child first (recursive)
  val childCtx = child.asInstanceOf[TransformSupport].transform(context)
  val operatorId = context.nextOperatorId(this.nodeName)
  // Use actual child node
  val relNode = getRelNode(context, operatorId, ..., input = childCtx.root, validation = false)
  TransformContext(output, relNode)
}
```

### 3.5 SubstraitContext Deep Dive

SubstraitContext is a critical object that tracks state during plan generation.

**Location:** `gluten-substrait/src/main/scala/org/apache/gluten/substrait/SubstraitContext.scala:48`

**What SubstraitContext Tracks:**

```scala
class SubstraitContext {
  // Function registry: maps function names to IDs
  private val functionMap: JHashMap[String, JLong]

  // Operator to Substrait Rel ID mapping
  private val operatorToRelsMap: JMap[JLong, JList[JLong]]

  // Join-specific parameters
  private val joinParamsMap: JHashMap[JLong, JoinParams]

  // Aggregation-specific parameters
  private val aggregationParamsMap: JHashMap[JLong, AggregationParams]

  // Auto-incrementing IDs
  private var iteratorIndex: JLong = 0L
  private var operatorId: JLong = 0L
  private var relId: JLong = 0L
}
```

**Key Methods:**
```scala
// Register a function (e.g., "add", "substring") and get its ID
def registerFunction(funcName: String): JLong

// Get next operator ID for your transformer
def nextOperatorId(nodeName: String): JLong

// Register this rel to an operator
def registerRelToOperator(operatorId: JLong): Unit
```

**Usage Example (from LimitExecTransformer.scala:44-50):**
```scala
override protected def doValidateInternal(): ValidationResult = {
  val context = new SubstraitContext  // Create fresh context
  val operatorId = context.nextOperatorId(this.nodeName)  // Get unique ID
  val relNode = getRelNode(context, operatorId, ...)  // Build Substrait
  doNativeValidation(context, relNode)  // Validate with backend
}
```

### 3.6 ValidationResult Patterns

ValidationResult is a sealed trait with two states: Succeeded or Failed.

**Location:** `gluten-substrait/src/main/scala/org/apache/gluten/execution/ValidationResult.scala:26`

**Structure:**
```scala
sealed trait ValidationResult {
  def ok(): Boolean
  def reason(): String
}

object ValidationResult {
  def succeeded: ValidationResult
  def failed(reason: String, prefix: String = "\n - "): ValidationResult
  def merge(left: ValidationResult, right: ValidationResult): ValidationResult
}
```

**Common Patterns:**

```scala
// Pattern 1: Simple success
ValidationResult.succeeded

// Pattern 2: Simple failure
ValidationResult.failed("Operator not supported")

// Pattern 3: Conditional validation
if (backendSupportsOperation) {
  ValidationResult.succeeded
} else {
  ValidationResult.failed(s"Backend doesn't support $operationType")
}

// Pattern 4: Early return on failure
val childValidation = validateChild()
if (!childValidation.ok()) {
  return childValidation  // Propagate failure
}

// Pattern 5: Merge multiple validations
val validation1 = validatePart1()
val validation2 = validatePart2()
ValidationResult.merge(validation1, validation2)

// Pattern 6: Wrap expression conversion (catches exceptions)
val validationResult = ValidationResult.wrap {
  val expressions = convertExpressions()
  doNativeValidation(context, relNode)
}
```

**Real Example from Codebase:**
Most operators follow this pattern (see `LimitExecTransformer.scala:44`):
```scala
override protected def doValidateInternal(): ValidationResult = {
  val context = new SubstraitContext
  val operatorId = context.nextOperatorId(this.nodeName)
  val relNode = getRelNode(context, operatorId, offset, count, child.output, null, true)
  doNativeValidation(context, relNode)  // Calls JNI to ask backend
}
```

### 3.7 BackendsApiManager Deep Dive

BackendsApiManager is a singleton that routes to backend-specific implementations.

**Location:** `gluten-substrait/src/main/scala/org/apache/gluten/backendsapi/BackendsApiManager.scala:21`

**Architecture:**
```
BackendsApiManager (singleton)
        │
        ├─> getSparkPlanExecApiInstance → Creates transformers
        ├─> getMetricsApiInstance → Defines metrics
        ├─> getValidatorApiInstance → Validates operators
        ├─> getTransformerApiInstance → Transformer utilities
        └─> getSettings → Backend configuration
```

**Usage Examples from Real Code:**

```scala
// Creating transformers (from any transformation rule):
val transformer = BackendsApiManager.getSparkPlanExecApiInstance
  .genFilterExecTransformer(condition, child)

// Getting metrics (from LimitExecTransformer.scala:33):
@transient override lazy val metrics =
  BackendsApiManager.getMetricsApiInstance.genLimitTransformerMetrics(sparkContext)

// Getting metrics updater (from LimitExecTransformer.scala:41):
override def metricsUpdater(): MetricsUpdater =
  BackendsApiManager.getMetricsApiInstance.genLimitTransformerMetricsUpdater(metrics)
```

**Backend Detection:**
BackendsApiManager automatically detects which backend is loaded (Velox or ClickHouse) at initialization and routes all API calls to the appropriate implementation.

### 3.8 How Transformation Rules Work

Operators get transformed to Gluten transformers via Spark's rule-based optimizer.

**Key Transformation Entry Points:**

1. **ColumnarOverrides Rule** - Main transformation rule
   - Matches Spark operators and replaces with transformers
   - Located in `gluten-core` module
   - Runs during Spark's physical planning phase

2. **Pattern Matching Example:**
```scala
// Simplified example of how rules work:
def apply(plan: SparkPlan): SparkPlan = plan transformDown {
  case limit @ LimitExec(count, child) =>
    // Create transformer
    LimitExecTransformer(child, offset = 0, count)

  case filter @ FilterExec(condition, child) =>
    BackendsApiManager.getSparkPlanExecApiInstance
      .genFilterExecTransformer(condition, child)

  case project @ ProjectExec(projectList, child) =>
    ProjectExecTransformer(projectList, child)
}
```

3. **Validation Before Transformation:**
Before a transformer replaces a Spark operator, Gluten validates it:
```scala
val transformer = createTransformer(sparkOperator)
val validationResult = transformer.doValidate()
if (validationResult.ok()) {
  // Use transformer
  transformer
} else {
  // Fallback to vanilla Spark
  logInfo(s"Fallback: ${validationResult.reason()}")
  sparkOperator
}
```

---

## 4. Development Process

Adding operator support follows a **5-phase workflow**:

### Quick Decision Tree

Before starting, use this decision tree to determine your approach:

```
Start: Want to add operator support
    │
    ├─> Is the Spark operator already transformed?
    │   └─> YES: Check gluten-substrait/src/main/scala/org/apache/gluten/execution/
    │       └─> Operator exists: Enhance existing transformer
    │       └─> Operator missing: Backend doesn't support it yet
    │
    ├─> NO: Proceed with new implementation
        │
        ├─> What type of operator?
        │   ├─> Unary (1 child): Filter, Project, Limit, Sort
        │   │   └─> Base: UnaryTransformSupport
        │   │   └─> Example: LimitExecTransformer.scala:29
        │   │
        │   ├─> Binary (2 children): Joins, Union, Set operations
        │   │   └─> Base: BinaryTransformSupport
        │   │   └─> Example: HashJoinExecTransformer
        │   │
        │   └─> Leaf (no children): Scans, Literals
        │       └─> Base: LeafTransformSupport
        │       └─> Example: FileSourceScanExecTransformer
        │
        ├─> Does it process expressions?
        │   ├─> YES: Need ExpressionTransformer support
        │   │   └─> Check: ExpressionMappings.scala for expression support
        │   │   └─> Example: ProjectExecTransformer (many expressions)
        │   │
        │   └─> NO: Simple parameter passing
        │       └─> Example: LimitExecTransformer (just offset/count)
        │
        ├─> Backend support?
        │   ├─> Velox: Check cpp/velox/substrait/SubstraitToVeloxPlan.cc
        │   │   └─> Has toVeloxPlan(MyOperatorRel)? → YES: Easy
        │   │   └─> Missing? → Need to add C++ support first
        │   │
        │   └─> ClickHouse: Check backends-clickhouse/ similarly
        │
        └─> Substrait support?
            ├─> Standard Substrait type exists?
            │   └─> YES: Use RelBuilder.makeXXXRel()
            │   └─> NO: May need custom extension
```

### Implementation Complexity Guide

| Operator Type | Complexity | Time Estimate | Key Challenges |
|---------------|------------|---------------|----------------|
| Simple Unary (Limit, Sample) | Low | 1-2 days | Minimal - parameter passing only |
| Filter | Medium | 2-3 days | Expression validation |
| Project | Medium | 2-4 days | Multiple expression types |
| Sort | Medium | 2-4 days | Sort order handling |
| Join | High | 5-10 days | Multiple join types, conditions, projections |
| Aggregate | High | 7-14 days | Multiple phases, aggregate functions, grouping |
| Window | Very High | 14+ days | Complex frame specs, multiple functions |

### Phase 1: Research and Planning

**1.1 Identify the Operator**
- Find the Spark operator class (e.g., `LimitExec`, `ProjectExec`)
- Understand its parameters, semantics, and output schema
- Check if it already has a transformer in Gluten

**1.2 Study Similar Implementations**
```bash
# Find existing transformer implementations
find gluten-substrait/src/main/scala/org/apache/gluten/execution/ -name "*Transformer.scala"

# Look for operators with similar characteristics:
# - Unary operators: LimitExecTransformer, FilterExecTransformer
# - Binary operators: HashJoinExecTransformer, UnionExecTransformer
# - Aggregate operators: HashAggregateExecTransformer
```

**1.3 Check Backend Support**
- **Velox**: Check Substrait support in `cpp/velox/substrait/SubstraitToVeloxPlan.cc`
- **ClickHouse**: Check support in backend-specific code
- Review feature support matrix: `docs/velox-backend-support-progress.md`

**1.4 Identify Substrait Mapping**
- Determine which Substrait RelNode type to use:
  - `FetchRel` for Limit
  - `FilterRel` for Filter
  - `ProjectRel` for Project
  - `SortRel` for Sort
  - `AggregateRel` for Aggregates
  - `JoinRel` for Joins

### Phase 2: Backend-Agnostic Scala Implementation

**2.1 Create Transformer Class**

Location: `gluten-substrait/src/main/scala/org/apache/gluten/execution/`

```scala
// File: MyOperatorExecTransformer.scala
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

case class MyOperatorExecTransformer(
    // Operator parameters
    param1: Type1,
    param2: Type2,
    child: SparkPlan  // For unary operators
  ) extends UnaryTransformSupport {

  // Step 1: Define metrics
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genMyOperatorTransformerMetrics(sparkContext)

  // Step 2: Define output schema
  override def output: Seq[Attribute] = child.output  // Or transform as needed

  // Step 3: Implement child copy (for Spark's tree transformations)
  override protected def withNewChildInternal(newChild: SparkPlan): MyOperatorExecTransformer =
    copy(child = newChild)

  // Step 4: Define metrics updater
  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genMyOperatorTransformerMetricsUpdater(metrics)

  // Step 5: Implement validation
  override protected def doValidateInternal(): ValidationResult = {
    val context = new SubstraitContext
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = getRelNode(
      context,
      operatorId,
      param1,
      param2,
      child.output,
      input = null,  // No real input during validation
      validation = true
    )
    doNativeValidation(context, relNode)
  }

  // Step 6: Implement transformation
  override protected def doTransform(context: SubstraitContext): TransformContext = {
    // Transform child first
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    // Build this operator's plan
    val relNode = getRelNode(
      context,
      operatorId,
      param1,
      param2,
      child.output,
      input = childCtx.root,  // Use actual child node
      validation = false
    )
    TransformContext(output, relNode)
  }

  // Step 7: Implement Substrait plan generation
  def getRelNode(
      context: SubstraitContext,
      operatorId: Long,
      param1: Type1,
      param2: Type2,
      inputAttributes: Seq[Attribute],
      input: RelNode,
      validation: Boolean): RelNode = {
    if (!validation) {
      // Production path
      RelBuilder.makeMyOperatorRel(input, param1, param2, context, operatorId)
    } else {
      // Validation path - may need mock input
      RelBuilder.makeMyOperatorRel(
        input,
        param1,
        param2,
        RelBuilder.createExtensionNode(inputAttributes.asJava),
        context,
        operatorId
      )
    }
  }
}
```

**2.2 Add to Backend API**

If this is a new operator type, add to `SparkPlanExecApi`:

```scala
// File: gluten-substrait/src/main/scala/org/apache/gluten/backendsapi/SparkPlanExecApi.scala
trait SparkPlanExecApi {
  // Add method to create your transformer
  def genMyOperatorExecTransformer(
      param1: Type1,
      param2: Type2,
      child: SparkPlan): MyOperatorExecTransformer =
    MyOperatorExecTransformer.create(param1, param2, child)
}
```

**2.3 Add Metrics Support**

Add metrics definitions to `MetricsApi`:

```scala
// File: gluten-substrait/src/main/scala/org/apache/gluten/backendsapi/MetricsApi.scala
trait MetricsApi {
  def genMyOperatorTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric]
  def genMyOperatorTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater
}
```

### Phase 3: Backend-Specific Implementation

#### For Velox Backend:

**3.1 Implement Backend API**

```scala
// File: backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxSparkPlanExecApi.scala
class VeloxSparkPlanExecApi extends SparkPlanExecApi {
  override def genMyOperatorExecTransformer(
      param1: Type1,
      param2: Type2,
      child: SparkPlan): MyOperatorExecTransformer = {
    // Velox-specific validation or customization if needed
    MyOperatorExecTransformer.createUnsafe(param1, param2, child)
  }
}
```

**3.2 Implement C++ Substrait to Velox Conversion**

Location: `cpp/velox/substrait/`

```cpp
// File: SubstraitToVeloxPlan.h
class SubstraitToVeloxPlan {
 public:
  // Add declaration
  core::PlanNodePtr toVeloxPlan(const ::substrait::MyOperatorRel& myOpRel);
};

// File: SubstraitToVeloxPlan.cc
core::PlanNodePtr SubstraitToVeloxPlan::toVeloxPlan(
    const ::substrait::MyOperatorRel& myOpRel) {

  // 1. Convert child plan
  core::PlanNodePtr childNode;
  if (myOpRel.has_input()) {
    childNode = toVeloxPlan(myOpRel.input());
  }

  // 2. Extract parameters from Substrait
  auto param1Value = myOpRel.param1();
  auto param2Value = myOpRel.param2();

  // 3. Create Velox operator node
  return std::make_shared<velox::core::MyOperatorNode>(
    nextPlanNodeId(),      // Unique node ID
    childNode,             // Input node
    param1Value,           // Parameters
    param2Value
  );
}
```

**3.3 Register in Substrait Parser**

```cpp
// File: SubstraitToVeloxPlan.cc - In main conversion function
core::PlanNodePtr SubstraitToVeloxPlan::toVeloxPlan(
    const ::substrait::Rel& srel) {

  if (srel.has_myoperator()) {
    return toVeloxPlan(srel.myoperator());
  }
  // ... other cases
}
```

#### For ClickHouse Backend:

Implementation follows similar patterns in `backends-clickhouse/` directory.

### Phase 4: Testing

**4.1 Create Test Class**

Location: `backends-velox/src/test/scala/org/apache/gluten/execution/`

```scala
// File: MyOperatorSuite.scala
package org.apache.gluten.execution

import org.apache.spark.SparkConf

class MyOperatorSuite extends VeloxWholeStageTransformerSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
  }

  test("basic_my_operator") {
    // Test basic functionality
    val df = runQueryAndCompare("SELECT ... FROM ... LIMIT 10") {
      df => {
        // Assertions on dataframe
        assert(df.count() == 10)
      }
    }
    checkLengthAndPlan(df, 10)
  }

  test("my_operator_with_complex_input") {
    // Test with more complex scenarios
    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey < 100
      """.stripMargin
    ) {
      checkGlutenOperatorMatch[MyOperatorExecTransformer]
    }
  }

  test("my_operator_fallback") {
    // Test that unsupported cases fallback correctly
    // This helps ensure validation works
  }
}
```

**4.2 Run Tests**

```bash
# Run specific test suite
./build/sbt "backends-velox/testOnly org.apache.gluten.execution.MyOperatorSuite"

# Run specific test
./build/sbt "backends-velox/testOnly org.apache.gluten.execution.MyOperatorSuite -- -z basic_my_operator"

# Run all tests
./build/sbt "backends-velox/test"
```

### Phase 5: Documentation and PR

**5.1 Update Documentation**

- Update operator support matrix if applicable
- Add to release notes if significant feature
- Document any new configurations

**5.2 Code Quality**

```bash
# Format Scala code
./dev/format-scala-code.sh

# Format C++ code (for Velox)
./dev/formatcppcode.sh

# Check scalastyle
./build/sbt scalastyle

# Run license header check
dev/check.py header main --fix
```

**5.3 Create PR**

Follow [CONTRIBUTING.md](../../CONTRIBUTING.md) guidelines:

- PR Title: `[GLUTEN-<issue ID>][VL] Add support for MyOperator`
- Include description of:
  - What operator is added
  - Why it's needed
  - How it's implemented
  - Test coverage
- Link to JIRA ticket

---

## 5. Example 1: Simple Unary Operator (Limit)

This example demonstrates the simplest operator pattern - a unary operator with no expression handling.

### 5.1 Understanding Limit Operator

**Spark Operator:** `LimitExec(limit: Int, child: SparkPlan)`

**Semantics:** Returns at most `limit` rows from child, with optional offset support.

**Substrait Mapping:** `FetchRel` (combines LIMIT and OFFSET)

### 5.2 Complete Implementation

**File:** `gluten-substrait/src/main/scala/org/apache/gluten/execution/LimitExecTransformer.scala`

```scala
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.JavaConverters._

/**
 * Transformer for Spark's LimitExec operator.
 *
 * This operator limits the number of rows returned from the child operator.
 * In Substrait, this is represented as a FetchRel which supports both offset and limit.
 *
 * @param child The child SparkPlan to limit
 * @param offset The number of rows to skip (typically 0)
 * @param count The maximum number of rows to return
 */
case class LimitExecTransformer(child: SparkPlan, offset: Long, count: Long)
  extends UnaryTransformSupport {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genLimitTransformerMetrics(sparkContext)

  // Output schema is the same as child
  override def output: Seq[Attribute] = child.output

  // Required for Spark's tree transformation infrastructure
  override protected def withNewChildInternal(newChild: SparkPlan): LimitExecTransformer =
    copy(child = newChild)

  // Metrics updater for Spark UI
  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genLimitTransformerMetricsUpdater(metrics)

  // Validation: Check if native backend can execute this limit operation
  override protected def doValidateInternal(): ValidationResult = {
    val context = new SubstraitContext
    val operatorId = context.nextOperatorId(this.nodeName)
    // Create Substrait plan for validation (with mock input)
    val relNode = getRelNode(context, operatorId, offset, count, child.output, null, true)
    // Ask backend if it can execute this plan
    doNativeValidation(context, relNode)
  }

  // Transformation: Convert to Substrait and link to actual child
  override protected def doTransform(context: SubstraitContext): TransformContext = {
    // First, transform child to get its Substrait representation
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    // Create Substrait plan with actual child node
    val relNode = getRelNode(context, operatorId, offset, count, child.output, childCtx.root, false)
    TransformContext(child.output, relNode)
  }

  /**
   * Creates Substrait FetchRel node.
   *
   * @param validation If true, creates plan for validation only (may use mock input)
   */
  def getRelNode(
      context: SubstraitContext,
      operatorId: Long,
      offset: Long,
      count: Long,
      inputAttributes: Seq[Attribute],
      input: RelNode,
      validation: Boolean): RelNode = {
    if (!validation) {
      // Production execution: use actual input node
      RelBuilder.makeFetchRel(input, offset, count, context, operatorId)
    } else {
      // Validation: provide schema information via extension node
      RelBuilder.makeFetchRel(
        input,
        offset,
        count,
        RelBuilder.createExtensionNode(inputAttributes.asJava),
        context,
        operatorId)
    }
  }
}
```

### 5.3 Key Takeaways from Limit Example

1. **Simple Structure**: No expression transformation, just pass-through schema
2. **Dual-Mode RelNode Creation**: Different paths for validation vs execution
3. **Metrics Integration**: Uses backend API for metric definitions
4. **Validation Pattern**: Creates Substrait plan, asks backend to validate

### 5.4 Testing Limit Operator

```scala
test("limit_basic") {
  val df = runQueryAndCompare("SELECT * FROM lineitem LIMIT 10") { df =>
    assert(df.count() == 10)
  }
  checkLengthAndPlan(df, 10)
}

test("limit_with_filter") {
  val df = runQueryAndCompare(
    "SELECT * FROM lineitem WHERE l_orderkey < 100 LIMIT 5") { df =>
    assert(df.count() == 5)
  }
  checkLengthAndPlan(df, 5)
}

test("limit_zero") {
  val df = runQueryAndCompare("SELECT * FROM lineitem LIMIT 0") { df =>
    assert(df.isEmpty)
  }
  checkLengthAndPlan(df, 0)
}
```

---

## 6. Example 2: Expression-Based Operator (Project)

This example shows a more complex operator that handles expressions.

### 6.1 Understanding Project Operator

**Spark Operator:** `ProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)`

**Semantics:** Evaluates a list of expressions to produce output columns. This is SQL's SELECT clause.

**Substrait Mapping:** `ProjectRel` with expression list

**Complexity:** Must transform Spark expressions to Substrait expressions.

### 6.2 Implementation Architecture

```
ProjectExecTransformer (thin wrapper)
        │
        └──▶ ProjectExecTransformerBase (main logic)
                    │
                    ├──▶ ExpressionConverter (Spark → ExpressionTransformer)
                    │
                    └──▶ ExpressionTransformer.doTransform() (→ Substrait Expression)
```

### 6.3 Complete Implementation

**File 1:** `gluten-substrait/src/main/scala/org/apache/gluten/execution/ProjectExecTransformer.scala`

```scala
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.SparkPlan

/**
 * Thin wrapper for ProjectExecTransformerBase.
 * Allows backends to intercept creation via BackendsApiManager.
 */
case class ProjectExecTransformer(projectList: Seq[NamedExpression], child: SparkPlan)
  extends ProjectExecTransformerBase(projectList, child) {

  override protected def withNewChildInternal(newChild: SparkPlan): ProjectExecTransformer =
    copy(child = newChild)
}

object ProjectExecTransformer {
  /**
   * Factory method that goes through backend API.
   * Use this in transformation rules.
   */
  def apply(projectList: Seq[NamedExpression], child: SparkPlan): ProjectExecTransformer = {
    BackendsApiManager.getSparkPlanExecApiInstance.genProjectExecTransformer(projectList, child)
  }

  /**
   * Direct constructor - only for backend implementations.
   * Most code should use apply() instead.
   */
  def createUnsafe(projectList: Seq[NamedExpression], child: SparkPlan): ProjectExecTransformer =
    new ProjectExecTransformer(projectList, child)
}
```

**File 2:** `gluten-substrait/src/main/scala/org/apache/gluten/execution/BasicPhysicalOperatorTransformer.scala` (excerpt)

```scala
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.{ExpressionConverter, ExpressionTransformer}
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.JavaConverters._

/**
 * Base class for Project operator transformation.
 * Contains the core logic for converting Spark projection expressions to Substrait.
 */
abstract class ProjectExecTransformerBase(
    val projectList: Seq[NamedExpression],
    val child: SparkPlan)
  extends UnaryTransformSupport {

  // Metrics for Spark UI
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetrics(sparkContext)

  // Output schema comes from project list
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetricsUpdater(metrics)

  // Validation phase
  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    // Wrap in try-catch to handle expression conversion failures
    val validationResult = ValidationResult.wrap {
      val relNode = getRelNode(
        substraitContext,
        projectList,
        child.output,
        operatorId,
        input = null,
        validation = true
      )
      doNativeValidation(substraitContext, relNode)
    }

    validationResult
  }

  // Transformation phase
  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = getRelNode(
      context,
      projectList,
      child.output,
      operatorId,
      childCtx.root,
      validation = false
    )
    TransformContext(output, relNode)
  }

  /**
   * Creates Substrait ProjectRel.
   *
   * Key steps:
   * 1. Convert Spark expressions to ExpressionTransformers
   * 2. Transform each to Substrait expression nodes
   * 3. Build ProjectRel with expression list
   */
  def getRelNode(
      context: SubstraitContext,
      projectList: Seq[NamedExpression],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {

    // Step 1: Convert Spark expressions to Gluten's ExpressionTransformer
    val columnarProjExprs: Seq[ExpressionTransformer] = ExpressionConverter
      .replaceWithExpressionTransformer(projectList, originalInputAttributes)

    // Step 2: Transform each expression to Substrait
    val projExprNodeList = columnarProjExprs.map(_.doTransform(context)).asJava

    // Step 3: Build ProjectRel
    RelBuilder.makeProjectRel(
      originalInputAttributes.asJava,
      input,
      projExprNodeList,
      context,
      operatorId,
      validation
    )
  }
}
```

### 6.4 Expression Handling Deep Dive

**Expression Conversion Flow:**

```
Spark Expression (e.g., Add(a, b))
        │
        ▼
ExpressionConverter.replaceWithExpressionTransformer()
        │
        ▼
ExpressionTransformer (e.g., AddExpressionTransformer)
        │
        ▼
.doTransform(SubstraitContext)
        │
        ▼
Substrait Expression Node (ExpressionNode)
```

**Example Expression Transformations:**

```scala
// Simple column reference
// Spark: AttributeReference("col1", IntegerType)
// Substrait: FieldReference(0)

// Arithmetic
// Spark: Add(AttributeReference("a"), Literal(1))
// Substrait: ScalarFunction("add", [FieldReference(0), Literal(1)])

// Complex expression
// Spark: Substring(col("name"), lit(1), lit(10))
// Substrait: ScalarFunction("substring", [FieldReference(x), Literal(1), Literal(10)])
```

### 6.5 Testing Project Operator

```scala
test("project_simple_columns") {
  val df = runQueryAndCompare(
    "SELECT l_orderkey, l_partkey FROM lineitem LIMIT 10") { df =>
    assert(df.schema.fields.length == 2)
    assert(df.schema.fieldNames.contains("l_orderkey"))
    assert(df.schema.fieldNames.contains("l_partkey"))
  }
  checkLengthAndPlan(df, 10)
}

test("project_with_expressions") {
  runQueryAndCompare(
    """
      |SELECT
      |  l_orderkey + 1 as key_plus_one,
      |  l_quantity * l_extendedprice as total,
      |  UPPER(l_shipmode) as mode
      |FROM lineitem
      |LIMIT 10
    """.stripMargin
  ) {
    checkGlutenOperatorMatch[ProjectExecTransformer]
  }
}

test("project_with_complex_types") {
  runQueryAndCompare(
    """
      |SELECT
      |  STRUCT(l_orderkey, l_partkey) as order_part,
      |  ARRAY(l_quantity, l_discount) as metrics
      |FROM lineitem
      |LIMIT 10
    """.stripMargin
  ) {
    checkGlutenOperatorMatch[ProjectExecTransformer]
  }
}
```

### 6.6 Key Takeaways from Project Example

1. **Expression Translation**: Core complexity is converting Spark expressions
2. **Two-Level Structure**: Thin wrapper + base class for backend customization
3. **Factory Pattern**: Use `apply()` for creation, which goes through backend API
4. **Validation Wrapping**: Expression conversion wrapped in try-catch for validation

### 6.7 Expression Transformer Deep Dive

Understanding expression transformation is crucial for operators that process expressions.

**Location:** `gluten-substrait/src/main/scala/org/apache/gluten/expression/ExpressionConverter.scala:42`

#### How Expression Conversion Works

**Entry Point:**
```scala
// From ExpressionConverter.scala:44-49
def replaceWithExpressionTransformer(
    exprs: Seq[Expression],
    attributeSeq: Seq[Attribute]): Seq[ExpressionTransformer] = {
  val expressionsMap = ExpressionMappings.expressionsMap
  exprs.map(expr => replaceWithExpressionTransformer0(expr, attributeSeq, expressionsMap))
}
```

#### Common Expression Transformers

**File Locations:**
- `gluten-substrait/src/main/scala/org/apache/gluten/expression/` - Base transformers
- Expression mapping registry in `ExpressionMappings.scala`

**Expression Transformer Examples:**

1. **Literal Values:**
```scala
// Spark: Literal(42, IntegerType)
// Transformer: LiteralTransformer
// Substrait: Literal { i32: 42 }
```

2. **Attribute References (Column Access):**
```scala
// Spark: AttributeReference("col1", IntegerType, ordinal=0)
// Transformer: AttributeReferenceTransformer
// Substrait: FieldReference { direct_reference { struct_field { field: 0 } } }
```

3. **Binary Arithmetic:**
```scala
// Spark: Add(col("a"), Literal(1))
// Transformer: AddExpressionTransformer
// Substrait: ScalarFunction {
//   function_reference: 0,  // "add" function
//   arguments: [FieldReference(0), Literal(1)]
// }
```

4. **String Functions:**
```scala
// Spark: Substring(col("name"), Literal(1), Literal(10))
// Transformer: SubstringExpressionTransformer
// Substrait: ScalarFunction {
//   function_reference: N,  // "substring" function
//   arguments: [FieldReference(x), Literal(1), Literal(10)]
// }
```

5. **Conditional Expressions:**
```scala
// Spark: If(condition, thenExpr, elseExpr)
// Transformer: IfExpressionTransformer
// Substrait: IfThen {
//   ifs: [{if: condition_expr, then: then_expr}],
//   else: else_expr
// }
```

#### Expression Mapping Pattern

Expressions are mapped via a registry system:

```scala
// Simplified from ExpressionMappings.scala
val expressionsMap: Map[Class[_], String] = Map(
  classOf[Add] -> "add",
  classOf[Subtract] -> "subtract",
  classOf[Multiply] -> "multiply",
  classOf[Substring] -> "substring",
  classOf[Upper] -> "upper",
  classOf[Lower] -> "lower"
  // ... hundreds more
)
```

#### Handling Unsupported Expressions

```scala
// Pattern for handling unsupported expressions:
def validateExpressions(exprs: Seq[Expression]): ValidationResult = {
  try {
    val transformers = ExpressionConverter.replaceWithExpressionTransformer(
      exprs,
      inputAttributes
    )
    ValidationResult.succeeded
  } catch {
    case e: GlutenNotSupportException =>
      ValidationResult.failed(s"Expression not supported: ${e.getMessage}")
    case e: Exception =>
      ValidationResult.failed(s"Expression conversion failed: ${e.getMessage}")
  }
}
```

#### Testing Expression Support

```scala
// Check if expression is supported before using:
val canTransform = ExpressionConverter.canReplaceWithExpressionTransformer(
  expr,
  inputAttributes
)
```

---

## 7. Example 3: Complex Operator (HashAggregate)

This example provides an overview of complex operator patterns without full implementation (as it's quite extensive).

### 7.1 Understanding HashAggregate

**Spark Operator:** `HashAggregateExec` with grouping and aggregate expressions

**Semantics:** Group-by aggregation using hash table

**Substrait Mapping:** `AggregateRel` with grouping keys and aggregate functions

**Complexity Factors:**
- Multiple phases (partial, final aggregation)
- Aggregate function handling (sum, count, avg, etc.)
- Grouping expressions
- Result expressions
- Memory management for hash table

### 7.2 High-Level Structure

```scala
case class HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryTransformSupport {

  // Complex validation logic
  override protected def doValidateInternal(): ValidationResult = {
    // 1. Validate grouping expressions
    // 2. Validate each aggregate function
    // 3. Check if backend supports this aggregation pattern
    // 4. Validate result expressions
  }

  // Complex transformation logic
  override protected def doTransform(context: SubstraitContext): TransformContext = {
    // 1. Transform child
    // 2. Convert grouping expressions
    // 3. Convert aggregate functions
    // 4. Build AggregateRel
    // 5. Add result projection if needed
  }
}
```

### 7.3 Key Challenges in Complex Operators

**1. Multi-Phase Execution**
```scala
// Partial aggregation (mapper side)
HashAggregateExecTransformer(
  mode = Partial,
  groupingExpressions = Seq(col("category")),
  aggregateExpressions = Seq(Sum(col("amount"))),
  ...
)

// Final aggregation (reducer side)
HashAggregateExecTransformer(
  mode = Final,
  groupingExpressions = Seq(col("category")),
  aggregateExpressions = Seq(Sum(col("partial_sum"))),
  ...
)
```

**2. Aggregate Function Mapping**
```scala
// Spark aggregate functions → Substrait functions
// Sum → substrait::sum
// Count → substrait::count
// Avg → substrait::avg (or decomposed to sum/count)
// CollectList → substrait::list_agg
```

**3. State Management**
- Hash table for grouping
- Accumulator state for each aggregate
- Memory pressure handling
- Spilling (if supported by backend)

### 7.4 Simplified Aggregate Example

**Testing Basic Aggregation:**

```scala
test("aggregate_simple_count") {
  runQueryAndCompare(
    """
      |SELECT COUNT(*)
      |FROM lineitem
    """.stripMargin
  ) {
    checkGlutenOperatorMatch[HashAggregateExecBaseTransformer]
  }
}

test("aggregate_group_by") {
  runQueryAndCompare(
    """
      |SELECT
      |  l_returnflag,
      |  l_linestatus,
      |  COUNT(*) as count,
      |  SUM(l_quantity) as sum_qty,
      |  AVG(l_extendedprice) as avg_price
      |FROM lineitem
      |GROUP BY l_returnflag, l_linestatus
    """.stripMargin
  ) {
    checkGlutenOperatorMatch[HashAggregateExecBaseTransformer]
  }
}

test("aggregate_with_having") {
  runQueryAndCompare(
    """
      |SELECT
      |  l_orderkey,
      |  SUM(l_quantity) as total_qty
      |FROM lineitem
      |GROUP BY l_orderkey
      |HAVING SUM(l_quantity) > 100
    """.stripMargin
  ) {
    checkGlutenOperatorMatch[HashAggregateExecBaseTransformer]
    checkGlutenOperatorMatch[FilterExecTransformerBase]
  }
}
```

### 7.5 Patterns for Complex Operators

When implementing complex operators like HashAggregate, Join, or Window:

1. **Break Down Validation**: Validate each sub-component separately
   ```scala
   // Validate grouping expressions
   val groupingValidation = validateGroupingExpressions()
   if (!groupingValidation.isValid) return groupingValidation

   // Validate aggregate functions
   val aggValidation = validateAggregateFunctions()
   if (!aggValidation.isValid) return aggValidation
   ```

2. **Modular Helper Methods**: Extract logic into focused methods
   ```scala
   private def buildGroupingExpressions(): Seq[ExpressionNode]
   private def buildAggregateFunctions(): Seq[AggregateFunctionNode]
   private def buildResultProjection(): RelNode
   ```

3. **Progressive Implementation**: Start with simple cases
   - First: Single aggregate, no grouping
   - Then: Single group by column
   - Then: Multiple group by columns
   - Finally: Complex aggregates, HAVING, etc.

4. **Extensive Testing**: Cover many scenarios
   - Different aggregate functions
   - Different data types
   - Edge cases (nulls, empty groups)
   - Multi-phase execution

---

## 8. Backend Implementation Details

### 8.1 Velox Backend C++ Implementation

#### 8.1.1 File Structure

```
cpp/velox/
├── substrait/
│   ├── SubstraitToVeloxPlan.h          # Main converter header
│   ├── SubstraitToVeloxPlan.cc         # Main converter implementation
│   ├── SubstraitToVeloxExpr.h          # Expression converter header
│   ├── SubstraitToVeloxExpr.cc         # Expression converter implementation
│   ├── SubstraitParser.h               # Utilities for parsing Substrait
│   └── SubstraitParser.cc
└── jni/
    ├── VeloxJniWrapper.cc               # JNI entry points
    └── VeloxPlanConverter.cc            # Plan conversion wrapper
```

#### 8.1.2 Adding a New Operator to Velox

**Step 1: Add Conversion Function Declaration**

```cpp
// File: cpp/velox/substrait/SubstraitToVeloxPlan.h

class SubstraitToVeloxPlan {
 public:
  // ... existing declarations

  /// Convert Substrait MyOperatorRel to Velox plan node
  /// @param myOpRel The Substrait MyOperator relation
  /// @return Velox plan node representing the operation
  core::PlanNodePtr toVeloxPlan(const ::substrait::MyOperatorRel& myOpRel);

 private:
  // Helper to validate MyOperator parameters
  void validateMyOperatorRel(const ::substrait::MyOperatorRel& myOpRel);
};
```

**Step 2: Implement Conversion Logic**

```cpp
// File: cpp/velox/substrait/SubstraitToVeloxPlan.cc

core::PlanNodePtr SubstraitToVeloxPlan::toVeloxPlan(
    const ::substrait::MyOperatorRel& myOpRel) {

  // 1. Validate the input
  validateMyOperatorRel(myOpRel);

  // 2. Convert child plan (if unary/binary operator)
  core::PlanNodePtr childNode;
  if (myOpRel.has_input()) {
    // Recursively convert child
    childNode = toVeloxPlan(myOpRel.input());
  }

  // 3. Extract parameters from Substrait
  auto param1 = myOpRel.param1();
  auto param2 = myOpRel.param2();

  // 4. Convert expressions if needed
  std::vector<core::TypedExprPtr> expressions;
  if (myOpRel.expressions_size() > 0) {
    for (const auto& substraitExpr : myOpRel.expressions()) {
      auto veloxExpr = exprConverter_->toVeloxExpr(substraitExpr, childNode->outputType());
      expressions.push_back(veloxExpr);
    }
  }

  // 5. Create Velox operator node
  // Option A: Use existing Velox operator
  return std::make_shared<core::MyVeloxOperatorNode>(
    nextPlanNodeId(),           // Unique node ID
    childNode,                  // Input source
    std::move(expressions),     // Expressions
    param1,                     // Parameters
    param2
  );

  // Option B: If Velox doesn't have this operator, you may need to:
  // - Implement a new Velox operator (in Velox repository)
  // - Or decompose into existing Velox operators
}

// Validation helper
void SubstraitToVeloxPlan::validateMyOperatorRel(
    const ::substrait::MyOperatorRel& myOpRel) {
  VELOX_CHECK(myOpRel.has_input(), "MyOperator requires input");
  VELOX_CHECK(myOpRel.param1() > 0, "MyOperator param1 must be positive");
  // ... other validation
}
```

**Step 3: Register in Main Conversion Switch**

```cpp
// File: cpp/velox/substrait/SubstraitToVeloxPlan.cc

core::PlanNodePtr SubstraitToVeloxPlan::toVeloxPlan(
    const ::substrait::Rel& rel) {

  if (rel.has_read()) {
    return toVeloxPlan(rel.read());
  } else if (rel.has_filter()) {
    return toVeloxPlan(rel.filter());
  } else if (rel.has_project()) {
    return toVeloxPlan(rel.project());
  } else if (rel.has_aggregate()) {
    return toVeloxPlan(rel.aggregate());
  } else if (rel.has_myoperator()) {  // ADD THIS
    return toVeloxPlan(rel.myoperator());
  }
  // ... other cases
  else {
    VELOX_FAIL("Unsupported Substrait relation type");
  }
}
```

**Step 4: Handle Expression Conversion (if needed)**

If your operator uses custom expressions:

```cpp
// File: cpp/velox/substrait/SubstraitToVeloxExpr.cc

core::TypedExprPtr SubstraitToVeloxExpr::toVeloxExpr(
    const ::substrait::Expression& substraitExpr,
    const RowTypePtr& inputType) {

  if (substraitExpr.has_my_custom_expr()) {
    return convertMyCustomExpression(substraitExpr.my_custom_expr(), inputType);
  }
  // ... existing cases
}

core::TypedExprPtr SubstraitToVeloxExpr::convertMyCustomExpression(
    const ::substrait::Expression::MyCustomExpr& expr,
    const RowTypePtr& inputType) {

  // Convert to Velox expression tree
  // This might be a function call, field reference, literal, etc.
}
```

#### 8.1.3 Example: Limit (Fetch) Conversion

```cpp
// Actual implementation in Gluten codebase
core::PlanNodePtr SubstraitToVeloxPlan::toVeloxPlan(
    const ::substrait::FetchRel& fetchRel) {

  // Convert child
  core::PlanNodePtr childNode;
  if (fetchRel.has_input()) {
    childNode = toVeloxPlan(fetchRel.input());
  }

  // Extract offset and count
  auto offset = fetchRel.offset();
  auto count = fetchRel.count();

  // Velox uses a LimitNode
  return std::make_shared<core::LimitNode>(
    nextPlanNodeId(),
    offset,
    count,
    false,  // isPartial
    childNode
  );
}
```

#### 8.1.4 Building and Testing C++ Changes

```bash
# Build Gluten C++ code with tests
cd cpp
mkdir -p build
cd build
cmake -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Debug ..
make -j$(nproc)

# Run Velox-specific tests
./velox/tests/velox_plan_conversion_test

# Or run all tests
ctest
```

### 8.2 ClickHouse Backend Implementation

ClickHouse backend follows a similar pattern but with different file locations:

**Key Files:**
- `cpp/clickhouse/substrait/SubstraitPlanParser.h`
- `cpp/clickhouse/substrait/SubstraitPlanParser.cc`
- Backend-specific Scala: `backends-clickhouse/src/main/scala/org/apache/gluten/backendsapi/clickhouse/`

**Pattern:**
```cpp
// Similar structure to Velox
DB::QueryPlanPtr SubstraitPlanParser::parseMyOperator(
    const substrait::MyOperatorRel& myOpRel) {
  // Convert to ClickHouse QueryPlan nodes
}
```

### 8.3 Backend API Implementation

Both backends must implement the `SparkPlanExecApi` trait:

```scala
// File: backends-velox/src/main/scala/.../VeloxSparkPlanExecApi.scala
class VeloxSparkPlanExecApi extends SparkPlanExecApi {

  override def genMyOperatorExecTransformer(
      params: ParamType,
      child: SparkPlan): MyOperatorExecTransformer = {

    // Velox-specific validation or customization
    if (needsSpecialHandling(params)) {
      // Custom logic
    }

    MyOperatorExecTransformer.createUnsafe(params, child)
  }
}

// File: backends-clickhouse/src/main/scala/.../CHSparkPlanExecApi.scala
class CHSparkPlanExecApi extends SparkPlanExecApi {

  override def genMyOperatorExecTransformer(
      params: ParamType,
      child: SparkPlan): MyOperatorExecTransformer = {

    // ClickHouse-specific logic
    CHMyOperatorExecTransformer(params, child)
  }
}
```

### 8.4 Substrait Proto Extensions

If adding entirely new operator types not in standard Substrait:

1. **Define Extension in Proto:**
   ```protobuf
   // substrait/proto/my_extension.proto
   message MyOperatorRel {
     RelCommon common = 1;
     Rel input = 2;
     int64 param1 = 3;
     string param2 = 4;
   }
   ```

2. **Register Extension:**
   Update Substrait plan builder to include extension type.

3. **Document Mapping:**
   Add to `docs/developers/SubstraitModifications.md`

### 8.5 Common Helper Utilities and Patterns

This section provides ready-to-use patterns and utilities found throughout the codebase.

#### Utility: RelBuilder Methods

**Location:** `gluten-substrait/src/main/scala/org/apache/gluten/substrait/rel/RelBuilder.scala`

Common RelBuilder methods you'll use:

```scala
// Create FetchRel (Limit)
RelBuilder.makeFetchRel(input, offset, count, context, operatorId)

// Create FilterRel
RelBuilder.makeFilterRel(input, condition, context, operatorId)

// Create ProjectRel
RelBuilder.makeProjectRel(
  originalInputAttributes.asJava,
  input,
  expressionNodes.asJava,
  context,
  operatorId,
  validation
)

// Create SortRel
RelBuilder.makeSortRel(input, sortExpressions, context, operatorId)

// Create AggregateRel
RelBuilder.makeAggregateRel(
  input,
  groupingExpressions,
  aggregateFunctions,
  context,
  operatorId
)

// Create JoinRel
RelBuilder.makeJoinRel(
  leftInput,
  rightInput,
  joinType,
  condition,
  context,
  operatorId
)

// Create extension node (for validation)
RelBuilder.createExtensionNode(attributes.asJava)
```

#### Pattern: Metrics Definition

**Common Metrics Pattern (from various transformers):**

```scala
// In your transformer:
@transient override lazy val metrics =
  BackendsApiManager.getMetricsApiInstance.genYourOperatorMetrics(sparkContext)

// Then in MetricsApi implementation (Velox/CH specific):
def genYourOperatorMetrics(sparkContext: SparkContext): Map[String, SQLMetric] = Map(
  "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
  "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
  "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
  "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time in operator"),
  "cpuNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "cpu time"),
  "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory usage")
)

// Metrics updater:
def genYourOperatorMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater = {
  new MetricsUpdater {
    override def updateNativeMetrics(opMetrics: OperatorMetrics): Unit = {
      if (opMetrics != null) {
        metrics("numOutputRows") += opMetrics.outputRows
        metrics("numOutputBatches") += opMetrics.outputVectors
        metrics("wallNanos") += opMetrics.wallNanos
        metrics("cpuNanos") += opMetrics.cpuNanos
        metrics("peakMemoryBytes") += opMetrics.peakMemoryBytes
      }
    }
  }
}
```

#### Pattern: Java Collections Conversion

Gluten uses Java collections for JNI interop. Common conversions:

```scala
import scala.collection.JavaConverters._

// Scala to Java
val scalaSeq: Seq[Attribute] = Seq(...)
val javaList: JList[Attribute] = scalaSeq.asJava

val scalaMap: Map[String, Int] = Map(...)
val javaMap: JMap[String, Int] = scalaMap.asJava

// Java to Scala
val javaList: JList[String] = ...
val scalaSeq: Seq[String] = javaList.asScala.toSeq
```

#### Pattern: Handling Optional Input Nodes

```scala
def getRelNode(
    context: SubstraitContext,
    operatorId: Long,
    input: RelNode,
    validation: Boolean): RelNode = {

  if (!validation) {
    // Production path - input is real child node
    RelBuilder.makeMyOperatorRel(input, params, context, operatorId)
  } else {
    // Validation path - create mock input if needed
    val validationInput = if (input == null) {
      // Create extension node with schema info
      RelBuilder.createExtensionNode(inputAttributes.asJava)
    } else {
      input
    }
    RelBuilder.makeMyOperatorRel(validationInput, params, context, operatorId)
  }
}
```

#### Pattern: Type Conversion

```scala
// Spark DataType to Substrait Type
import org.apache.gluten.substrait.`type`.TypeBuilder

val sparkType: DataType = IntegerType
val substraitType: TypeNode = TypeBuilder.makeI32(nullable = false)

// Common type conversions:
IntegerType    → TypeBuilder.makeI32()
LongType       → TypeBuilder.makeI64()
FloatType      → TypeBuilder.makeFP32()
DoubleType     → TypeBuilder.makeFP64()
StringType     → TypeBuilder.makeString()
BooleanType    → TypeBuilder.makeBoolean()
DecimalType(p,s) → TypeBuilder.makeDecimal(p, s)
```

#### Pattern: Debugging Substrait Plans

```scala
// Add this temporarily to see generated Substrait:
import org.apache.gluten.utils.SubstraitPlanPrinterUtil

override protected def doTransform(context: SubstraitContext): TransformContext = {
  val result = super.doTransform(context)

  // Print Substrait plan (human-readable)
  println(s"=== Substrait Plan for ${this.getClass.getSimpleName} ===")
  println(SubstraitPlanPrinterUtil.substraitRelToString(result.root))

  // Or print protobuf (detailed)
  println(result.root.toProtobuf.toString)

  result
}
```

#### Pattern: Child Transformation

```scala
// For UnaryTransformSupport:
override protected def doTransform(context: SubstraitContext): TransformContext = {
  // Always transform child first
  val childCtx = child.asInstanceOf[TransformSupport].transform(context)

  // Use childCtx.root as input to your operator
  val operatorId = context.nextOperatorId(this.nodeName)
  val relNode = buildMyOperator(childCtx.root, operatorId, context)

  // Return with your output schema
  TransformContext(output, relNode)
}

// For BinaryTransformSupport (e.g., joins):
override protected def doTransform(context: SubstraitContext): TransformContext = {
  // Transform both children
  val leftCtx = left.asInstanceOf[TransformSupport].transform(context)
  val rightCtx = right.asInstanceOf[TransformSupport].transform(context)

  val operatorId = context.nextOperatorId(this.nodeName)
  val relNode = buildJoin(leftCtx.root, rightCtx.root, operatorId, context)

  TransformContext(output, relNode)
}
```

#### Pattern: Configuration Access

```scala
import org.apache.gluten.config.GlutenConfig

// Access Gluten configs in your transformer:
val enableNativeValidation = GlutenConfig.getConf.enableNativeValidation
val columnarBatchSize = GlutenConfig.getConf.columnarBatchSize
val memoryLimit = GlutenConfig.getConf.memoryReservationBlockSize

// Check if feature is enabled:
if (GlutenConfig.getConf.enableMyFeature) {
  // Use feature
}
```

#### Pattern: Error Handling

```scala
// Validation errors - return failed result
override protected def doValidateInternal(): ValidationResult = {
  if (!isSupported) {
    return ValidationResult.failed(s"$nodeName: feature X not supported")
  }

  try {
    // Validation logic
    doNativeValidation(context, relNode)
  } catch {
    case e: GlutenNotSupportException =>
      ValidationResult.failed(s"$nodeName: ${e.getMessage}")
    case e: Exception =>
      ValidationResult.failed(s"$nodeName validation failed: ${e.getMessage}")
  }
}

// Transformation errors - throw exceptions
override protected def doTransform(context: SubstraitContext): TransformContext = {
  require(child != null, "Child cannot be null")
  require(output.nonEmpty, "Output schema cannot be empty")

  // Transformation logic - exceptions will be caught by Gluten framework
}
```

#### Pattern: Attribute Ordinal Tracking

```scala
// Attributes have ordinals that map to field positions
val inputAttributes: Seq[Attribute] = child.output

// Find attribute by name:
val attr = inputAttributes.find(_.name == "col1").getOrElse(
  throw new IllegalArgumentException(s"Column col1 not found")
)

// Get ordinal (position in tuple):
val ordinal = inputAttributes.indexOf(attr)

// Create field reference:
val fieldRef = ExpressionBuilder.makeSelection(ordinal)
```

---

## 9. Testing Guide

### 9.1 Test Organization

```
backends-velox/src/test/scala/org/apache/gluten/
├── execution/              # Operator execution tests
│   ├── MiscOperatorSuite.scala
│   ├── VeloxHashJoinSuite.scala
│   ├── VeloxAggregateFunctionsSuite.scala
│   ├── VeloxWindowExpressionSuite.scala
│   └── MyOperatorSuite.scala  # Your new test
├── expression/             # Expression transformation tests
│   └── VeloxUdfSuite.scala
└── utils/                  # Test utilities
```

### 9.2 Base Test Class: VeloxWholeStageTransformerSuite

All operator tests should extend this base class:

```scala
class MyOperatorSuite extends VeloxWholeStageTransformerSuite {

  // Override to set test data location
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  // Override to customize Spark configuration
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      // Add your custom configs
  }

  // Setup before all tests
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create test tables
    createTPCHNotNullTables()
  }
}
```

### 9.3 Writing Effective Tests

#### Pattern 1: Basic Functionality Test

```scala
test("my_operator_basic") {
  // runQueryAndCompare runs query in both Gluten and vanilla Spark, compares results
  val df = runQueryAndCompare(
    """
      |SELECT col1, col2
      |FROM my_table
      |WHERE col1 > 10
    """.stripMargin
  ) { df =>
    // Assertions on the DataFrame
    assert(df.columns.length == 2)
    assert(df.columns.contains("col1"))
  }

  // Check result count and verify plan contains transformer
  checkLengthAndPlan(df, expectedRowCount)
}
```

#### Pattern 2: Operator Presence Test

```scala
test("my_operator_in_plan") {
  runQueryAndCompare("SELECT ...") {
    // Verify that our transformer is in the plan
    checkGlutenOperatorMatch[MyOperatorExecTransformer]
  }
}
```

#### Pattern 3: Complex Query Test

```scala
test("my_operator_with_complex_query") {
  runQueryAndCompare(
    """
      |SELECT
      |  l_returnflag,
      |  l_linestatus,
      |  SUM(l_quantity) as sum_qty,
      |  SUM(l_extendedprice) as sum_base_price,
      |  COUNT(*) as count_order
      |FROM lineitem
      |WHERE l_shipdate <= date '1998-12-01'
      |GROUP BY l_returnflag, l_linestatus
      |ORDER BY l_returnflag, l_linestatus
    """.stripMargin
  ) { df =>
    // Can check multiple operators in plan
    checkGlutenOperatorMatch[MyOperatorExecTransformer]
    checkGlutenOperatorMatch[HashAggregateExecBaseTransformer]
  }
}
```

#### Pattern 4: Edge Case Tests

```scala
test("my_operator_empty_input") {
  runQueryAndCompare(
    "SELECT * FROM lineitem WHERE 1=0 LIMIT 10"
  ) { df =>
    assert(df.isEmpty)
  }
}

test("my_operator_null_handling") {
  val data = Seq(Row(null, 1), Row(2, null), Row(3, 4))
  val schema = StructType(Seq(
    StructField("col1", IntegerType, nullable = true),
    StructField("col2", IntegerType, nullable = true)
  ))

  spark.createDataFrame(data.asJava, schema)
    .createOrReplaceTempView("null_test")

  runQueryAndCompare("SELECT * FROM null_test WHERE col1 IS NOT NULL") {
    checkGlutenOperatorMatch[FilterExecTransformerBase]
  }
}

test("my_operator_large_input") {
  // Test with large dataset to verify memory handling
  runQueryAndCompare(
    "SELECT * FROM large_table LIMIT 1000000"
  ) { df =>
    assert(df.count() == 1000000)
  }
}
```

#### Pattern 5: Fallback Test

```scala
test("my_operator_fallback_on_unsupported_case") {
  // Test a case that should fallback to vanilla Spark
  // For example, if your operator doesn't support certain data types

  withSQLConf(
    "spark.gluten.sql.columnar.forceShuffledHashJoin" -> "false"
  ) {
    val df = spark.sql(
      """
        |SELECT * FROM table_with_unsupported_type
      """.stripMargin
    )

    // Verify it falls back (doesn't contain transformer)
    val plan = df.queryExecution.executedPlan
    assert(!plan.toString.contains("MyOperatorExecTransformer"))
  }
}
```

### 9.4 Test Data Setup

#### Using TPC-H Data

```scala
override def beforeAll(): Unit = {
  super.beforeAll()
  // Creates standard TPC-H tables: lineitem, orders, customer, etc.
  createTPCHNotNullTables()
}

test("using_tpch_data") {
  runQueryAndCompare("SELECT * FROM lineitem LIMIT 10") { _ => }
}
```

#### Creating Custom Test Data

```scala
test("custom_test_data") {
  val data = Seq(
    Row(1, "alice", 100.0),
    Row(2, "bob", 200.0),
    Row(3, "charlie", 300.0)
  )

  val schema = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("amount", DoubleType, nullable = false)
  ))

  spark.createDataFrame(data.asJava, schema)
    .createOrReplaceTempView("my_test_table")

  runQueryAndCompare("SELECT * FROM my_test_table WHERE amount > 150") { df =>
    assert(df.count() == 2)
  }
}
```

#### Using Temporary Files

```scala
test("test_with_parquet_file") {
  withTempPath { path =>
    // Write data to temp parquet file
    val testData = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
    testData.write.parquet(path.getCanonicalPath)

    // Read and test
    spark.read.parquet(path.getCanonicalPath)
      .createOrReplaceTempView("temp_view")

    runQueryAndCompare("SELECT * FROM temp_view") { df =>
      assert(df.count() == 3)
    }
  }
}
```

### 9.5 Running Tests

#### Run All Tests

```bash
# Run all Velox backend tests
./build/sbt "backends-velox/test"

# Run all tests with specific profile
./build/sbt -Pspark-3.3 "backends-velox/test"
```

#### Run Specific Test Suite

```bash
# Run single test suite
./build/sbt "backends-velox/testOnly org.apache.gluten.execution.MyOperatorSuite"

# Run with wildcard
./build/sbt "backends-velox/testOnly org.apache.gluten.execution.*OperatorSuite"
```

#### Run Specific Test

```bash
# Run single test by name
./build/sbt "backends-velox/testOnly org.apache.gluten.execution.MyOperatorSuite -- -z my_operator_basic"

# Run tests matching pattern
./build/sbt "backends-velox/testOnly org.apache.gluten.execution.MyOperatorSuite -- -z \"my_operator\""
```

#### Run Tests with Logging

```bash
# Enable debug logging
./build/sbt -Dlog4j.configuration=file:conf/log4j-debug.properties "backends-velox/testOnly ..."

# Run with specific log level
./build/sbt -Dspark.gluten.sql.debug=true "backends-velox/testOnly ..."
```

### 9.6 Debugging Failed Tests

#### View Test Output

```bash
# Test results are in
# backends-velox/target/test-reports/

# Surefire reports (detailed XML)
# backends-velox/target/surefire-reports/
```

#### Common Test Failures

**1. Native Crash**
```
Error: JVM crashed or UnsatisfiedLinkError
```
Solution: Check C++ code, rebuild with debug symbols, use GDB

**2. Result Mismatch**
```
Error: Gluten result differs from vanilla Spark
```
Solution: Check expression conversion, validate Substrait plan

**3. Validation Failure**
```
Error: Native validation failed
```
Solution: Check backend support, review operator parameters

**4. Fallback Happened**
```
Error: Expected transformer not found in plan
```
Solution: Check validation logic, ensure operator is transformable

---

## 10. Build and Troubleshooting

### 10.1 Build Commands

#### Full Build

```bash
# Build everything (Velox + Gluten)
./dev/builddeps-veloxbe.sh --build_tests=ON --build_benchmarks=ON
./dev/build.sh --backends-velox

# Build for specific Spark version
./dev/build.sh --backends-velox --spark-version=3.3

# Build with Java 17
./dev/build.sh --backends-velox -Pjava-17
```

#### Incremental Builds

```bash
# Only rebuild Scala/Java code (fast)
./build/sbt clean compile

# Only specific module
./build/sbt "backends-velox/compile"

# Only tests
./build/sbt "backends-velox/test:compile"
```

#### C++ Only Build

```bash
# Rebuild C++ code only
cd cpp
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j$(nproc)
```

### 10.2 Code Formatting

#### Scala/Java Code Style

```bash
# Format all Scala/Java code
./dev/format-scala-code.sh

# Check scalastyle violations
./build/sbt scalastyle

# Check specific module
./build/sbt "backends-velox/scalastyle"

# Common scalastyle fixes
# - Remove trailing whitespace
find . -name "*.scala" -exec sed -i '' 's/[[:space:]]*$//' {} \;

# - Add final newlines
find . -name "*.scala" -exec sh -c 'if [ ! -z "$(tail -c1 "$1")" ]; then echo >> "$1"; fi' _ {} \;

# - Break long lines (manual, max 100 chars)
```

#### C++ Code Style

```bash
# Format C++ code (requires clang-format-15)
./dev/formatcppcode.sh

# Format specific file
clang-format-15 -i cpp/velox/substrait/SubstraitToVeloxPlan.cc

# Install clang-format-15 (Ubuntu)
apt-get install clang-format-15
```

#### License Headers

```bash
# Check license headers
dev/check.py header main

# Fix license headers automatically
dev/check.py header main --fix
```

### 10.3 Common Build Issues

#### Issue 1: Velox Build Failure

```
Error: Velox dependencies not found
```

Solution:
```bash
# Clean Velox build
rm -rf ep/build-velox/build/velox_ep
./dev/builddeps-veloxbe.sh --build_tests=ON
```

#### Issue 2: JNI Link Errors

```
Error: UnsatisfiedLinkError: libgluten.so not found
```

Solution:
```bash
# Rebuild C++ with correct paths
cd cpp/build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# Check library is built
ls build/releases/libgluten.so
```

#### Issue 3: Scalastyle Violations

```
Error: File must end with newline character
Error: Line is longer than 100 characters
```

Solution: Follow scalastyle rules exactly (see section 10.2)

#### Issue 4: Maven Dependency Conflicts

```
Error: Could not resolve dependencies
```

Solution:
```bash
# Clean maven cache
rm -rf ~/.m2/repository/org/apache/gluten

# Rebuild with clean
./dev/build.sh --backends-velox --clean
```

### 10.4 Debugging Techniques

#### Debug Scala Code

**Using IntelliJ IDEA:**
1. Import project via `pom.xml`
2. Activate profile `backends-velox`
3. Reload Maven project
4. Set breakpoints in transformer code
5. Debug test via right-click → Debug

**Using SBT:**
```bash
# Run test in debug mode
./build/sbt -jvm-debug 5005 "backends-velox/testOnly MyOperatorSuite"

# Attach debugger to port 5005
```

#### Debug C++ Code

**Using GDB:**
```bash
# Get Java process PID
jps
# Output: 12345 ScalaTestRunner

# Attach GDB
gdb attach 12345

# Set breakpoints
(gdb) b SubstraitToVeloxPlan.cc:100
(gdb) c

# When hit breakpoint
(gdb) bt           # Backtrace
(gdb) p variable   # Print variable
(gdb) n           # Next line
(gdb) s           # Step into
```

**Using VS Code:**
See [NewToGluten.md](NewToGluten.md) for VS Code C++ debugging setup.

#### Debug Native Crashes

```bash
# Enable core dumps
mkdir -p /tmp/cores
echo "/tmp/cores/core-%e-%p-%t" | sudo tee /proc/sys/kernel/core_pattern
ulimit -c unlimited

# Run test
./build/sbt "backends-velox/testOnly MyOperatorSuite"

# If crash occurs, analyze core dump
gdb cpp/build/releases/libgluten.so /tmp/cores/core-java-12345-1234567890
(gdb) bt
(gdb) frame 5
(gdb) list
```

### 10.5 Common Error Scenarios and Solutions

This section catalogs real errors you might encounter and their solutions.

#### Error 1: Validation Failed with No Details

**Error:**
```
Query falls back to vanilla Spark
Plan shows original Spark operator, not transformer
```

**Cause:**
`doValidateInternal()` returned `ValidationResult.failed()` but reason isn't visible.

**Solution:**
```scala
// Add debug logging to see validation failures:
override protected def doValidateInternal(): ValidationResult = {
  val result = try {
    val context = new SubstraitContext
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = getRelNode(context, operatorId, ..., null, true)
    doNativeValidation(context, relNode)
  } catch {
    case e: Exception =>
      ValidationResult.failed(s"Validation exception: ${e.getMessage}")
  }

  // Debug logging
  if (!result.ok()) {
    logWarning(s"${this.getClass.getSimpleName} validation failed: ${result.reason()}")
  }

  result
}
```

#### Error 2: ClassCastException in doTransform

**Error:**
```
java.lang.ClassCastException: org.apache.spark.sql.execution.FilterExec
cannot be cast to org.apache.gluten.execution.TransformSupport
```

**Cause:**
Child operator isn't a transformer (fallback happened for child).

**Solution:**
```scala
// Check if child is actually transformable before casting:
override protected def doTransform(context: SubstraitContext): TransformContext = {
  child match {
    case transformChild: TransformSupport =>
      val childCtx = transformChild.transform(context)
      // Continue with transformation
    case _ =>
      throw new GlutenException(
        s"Child of $nodeName is not transformed. " +
        s"This indicates validation passed incorrectly."
      )
  }
}

// Better: Validate child during doValidateInternal:
override protected def doValidateInternal(): ValidationResult = {
  child match {
    case _: TransformSupport =>
      // Continue validation
    case _ =>
      return ValidationResult.failed(s"Child is not transformable")
  }
  // Rest of validation
}
```

#### Error 3: Substrait Function Not Found

**Error:**
```
Native validation failed: Function 'my_function' not registered
```

**Cause:**
Function name mismatch between Spark, Substrait, and Velox.

**Solution:**
```scala
// 1. Check ExpressionMappings.scala for correct Substrait function name
// 2. Verify Velox supports this function in cpp/velox/functions/

// Debug: Print registered functions
val context = new SubstraitContext
// Try registering the function
val funcId = context.registerFunction("my_function")
println(s"Function ID: $funcId, all functions: ${context.registeredFunction}")

// 3. Check Velox function registry:
// cpp/velox/functions/prestosql/registration/ArithmeticFunctionsRegistration.cpp
// Look for VELOX_REGISTER_VECTOR_FUNCTION(udf_my_function, "my_function")
```

#### Error 4: Schema Mismatch After Transformation

**Error:**
```
Schema mismatch: Expected [a:int, b:string], got [a:int]
```

**Cause:**
`output` attribute list doesn't match actual Substrait output.

**Solution:**
```scala
// Ensure output matches what Substrait produces:
override def output: Seq[Attribute] = {
  // For pass-through operators:
  child.output

  // For projection operators:
  projectList.map(_.toAttribute)

  // For joins:
  left.output ++ right.output

  // For aggregates:
  groupingExpressions.map(_.toAttribute) ++
    aggregateExpressions.map(_.resultAttribute)
}

// Debug: Print schemas
println(s"Expected output: ${output.map(a => s"${a.name}:${a.dataType}").mkString(", ")}")
println(s"Child output: ${child.output.map(a => s"${a.name}:${a.dataType}").mkString(", ")}")
```

#### Error 5: NullPointerException in Native Code

**Error:**
```
SIGSEGV (Segmentation fault)
JNI ERROR: app bug: accessed null object
```

**Cause:**
Null pointer passed to JNI or Substrait plan has null field.

**Solution:**
```scala
// Add null checks before JNI calls:
override protected def doTransform(context: SubstraitContext): TransformContext = {
  require(context != null, "SubstraitContext cannot be null")
  require(child != null, "Child cannot be null")

  val childCtx = child.asInstanceOf[TransformSupport].transform(context)
  require(childCtx != null, "Child transform context cannot be null")
  require(childCtx.root != null, "Child RelNode cannot be null")

  // Continue transformation
}

// In getRelNode, ensure all parameters are valid:
def getRelNode(..., input: RelNode, validation: Boolean): RelNode = {
  if (!validation && input == null) {
    throw new IllegalArgumentException("Input RelNode cannot be null in non-validation mode")
  }
  // Build RelNode
}
```

#### Error 6: Memory Allocation Failure

**Error:**
```
std::bad_alloc
OutOfMemoryError: Direct buffer memory
```

**Cause:**
Native memory exhausted during execution.

**Solution:**
```bash
# Increase off-heap memory in Spark config:
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=4g  # Increase this

# Or in test:
override protected def sparkConf: SparkConf = {
  super.sparkConf
    .set("spark.memory.offHeap.size", "4g")
}
```

#### Error 7: Expression Not Supported

**Error:**
```
GlutenNotSupportException: Expression CheckOverflow is not currently supported
```

**Cause:**
Expression type not mapped or backend doesn't support it.

**Solution:**
```scala
// 1. Check if expression is in ExpressionMappings.scala
// 2. If not, you need to add expression support first
// 3. Temporary workaround: Add validation to reject unsupported expressions

override protected def doValidateInternal(): ValidationResult = {
  // Check for unsupported expressions
  val unsupportedExprs = findUnsupportedExpressions(projectList)
  if (unsupportedExprs.nonEmpty) {
    return ValidationResult.failed(
      s"Unsupported expressions: ${unsupportedExprs.mkString(", ")}"
    )
  }
  // Continue validation
}

def findUnsupportedExpressions(exprs: Seq[Expression]): Seq[String] = {
  exprs.flatMap { expr =>
    if (!ExpressionConverter.canReplaceWithExpressionTransformer(expr, child.output)) {
      Some(expr.getClass.getSimpleName)
    } else {
      None
    }
  }
}
```

#### Error 8: Test Comparison Failed

**Error:**
```
Results differ:
Expected: [1, 2, 3]
Got: [1, 2, 3, 4]
```

**Cause:**
Gluten result differs from vanilla Spark result.

**Solution:**
```scala
// 1. Check if operator semantics are correct
// 2. Verify Substrait parameters match Spark parameters
// 3. Check for off-by-one errors (especially in Limit/Offset)

// Debug: Print both results
test("debug_comparison") {
  val query = "SELECT * FROM lineitem LIMIT 10"

  // Run with vanilla Spark
  withSQLConf("spark.gluten.enabled" -> "false") {
    val vanillaResult = spark.sql(query).collect()
    println(s"Vanilla result count: ${vanillaResult.length}")
    vanillaResult.take(5).foreach(println)
  }

  // Run with Gluten
  withSQLConf("spark.gluten.enabled" -> "true") {
    val glutenResult = spark.sql(query).collect()
    println(s"Gluten result count: ${glutenResult.length}")
    glutenResult.take(5).foreach(println)
  }
}
```

#### Error 9: Metrics Not Showing in Spark UI

**Error:**
Metrics show as 0 or don't appear in Spark UI.

**Solution:**
```scala
// 1. Ensure metrics are defined:
@transient override lazy val metrics =
  BackendsApiManager.getMetricsApiInstance.genMyOperatorMetrics(sparkContext)

// 2. Ensure metricsUpdater is implemented:
override def metricsUpdater(): MetricsUpdater =
  BackendsApiManager.getMetricsApiInstance.genMyOperatorMetricsUpdater(metrics)

// 3. Ensure backend is calling updateNativeMetrics with operator metrics
// Check in VeloxMetricsApi or CHMetricsApi implementation

// 4. Verify metrics are being collected in native code:
// For Velox: Check that operator returns stats in toVeloxPlan
```

#### Error 10: Build Fails with Protobuf Errors

**Error:**
```
[ERROR] cannot find symbol: class MyOperatorRel
```

**Cause:**
Substrait proto files not regenerated after modification.

**Solution:**
```bash
# Regenerate Substrait proto Java/Scala classes:
cd substrait
./gradlew clean generateProto
cd ..

# Rebuild Gluten:
./dev/build.sh --backends-velox --clean
```

#### Enable Verbose Logging

```scala
// In test
withSQLConf(
  "spark.gluten.sql.columnar.logicalPlan.debug" -> "true",
  "spark.gluten.sql.columnar.physicalPlan.debug" -> "true"
) {
  // Your test code
}
```

```bash
# Set log level via environment
export GLUTEN_LOG_LEVEL=DEBUG
./build/sbt "backends-velox/testOnly ..."
```

#### Inspect Substrait Plans

```scala
// Add to transformer code temporarily
override protected def doTransform(context: SubstraitContext): TransformContext = {
  val result = super.doTransform(context)

  // Print Substrait plan
  println(s"Substrait Plan for ${this.getClass.getSimpleName}:")
  println(result.root.toProtobuf.toString)

  result
}
```

---

## 11. PR Submission Checklist

### 11.1 Before Creating PR

- [ ] **Code is complete and tested**
  - All operator transformation logic implemented
  - Backend-specific code added (Velox/ClickHouse)
  - Comprehensive test coverage
  - All tests passing locally

- [ ] **Code quality checks pass**
  ```bash
  # Run all checks
  ./dev/format-scala-code.sh
  ./dev/formatcppcode.sh
  ./build/sbt scalastyle
  dev/check.py header main

  # Verify build succeeds
  ./dev/build.sh --backends-velox
  ```

- [ ] **No regressions**
  ```bash
  # Run full test suite
  ./build/sbt "backends-velox/test"

  # Or at minimum, run affected test suites
  ./build/sbt "backends-velox/testOnly org.apache.gluten.execution.*"
  ```

### 11.2 JIRA Issue

- [ ] **Create JIRA ticket** (if not exists)
  - Navigate to: https://issues.apache.org/jira/browse/GLUTEN
  - Title: `Add support for [OperatorName] operator`
  - Type: `New Feature` or `Improvement`
  - Component: `[VL] Velox` or `[CH] ClickHouse` or `[CORE] Core`
  - Description: Explain what operator is being added and why

### 11.3 PR Title and Description

**Title Format:**
```
[GLUTEN-<issue ID>][VL] Add support for MyOperator operator
```

Examples:
- `[GLUTEN-1234][VL] Add support for Window frame operator`
- `[GLUTEN-5678][CORE] Add RowNumber operator transformer`
- `[GLUTEN-9012][CH] Add TakeOrderedAndProject operator`

**Description Template:**
```markdown
## What changes were proposed in this pull request?

This PR adds support for the MyOperator operator in Gluten.

## Why are the changes needed?

The MyOperator operator is currently falling back to vanilla Spark execution,
causing performance degradation in queries that use [specific functionality].
Adding native support will improve performance by [X]%.

## How was this patch tested?

- Added MyOperatorSuite with [N] test cases covering:
  - Basic functionality
  - Edge cases (nulls, empty input, large data)
  - Integration with other operators
  - Fallback scenarios

- Ran TPC-H benchmark queries, showing [X]% improvement in queries Q1, Q5, Q8

## Related Issues

- Fixes GLUTEN-1234
- Related to GLUTEN-5678
```

### 11.4 Documentation Updates

- [ ] **Update support matrix** (if applicable)
  ```
  docs/velox-backend-support-progress.md
  ```

- [ ] **Add configuration docs** (if new configs)
  ```
  docs/Configuration.md
  ```

- [ ] **Update release notes** (if significant feature)
  ```
  docs/release-notes/[version].md
  ```

### 11.5 CI Requirements

- [ ] **All CI checks pass**
  - Velox Backend CI (GitHub Actions)
  - ClickHouse Backend CI (if applicable)
  - Scalastyle checks
  - License header checks

- [ ] **Benchmark results** (for performance-critical changes)
  ```
  # Trigger benchmark on PR
  Comment: /Benchmark Velox
  ```

### 11.6 Review Process

- [ ] **Self-review checklist**
  - Code follows existing patterns
  - No unnecessary changes or reformatting
  - Comments explain complex logic
  - No debug print statements left
  - Error handling is appropriate

- [ ] **Address review feedback**
  - Respond to all comments
  - Make requested changes
  - Re-request review when ready

- [ ] **Squash and merge**
  - When approved, maintainer will squash and merge
  - Commit message will be: `[GLUTEN-<ID>][TAG] Title (#PR_NUM)`

### 11.7 Post-Merge

- [ ] **Verify in main branch**
  - Check CI passes on main
  - Verify no regressions in subsequent runs

- [ ] **Update JIRA**
  - Mark issue as Resolved
  - Set Fix Version

- [ ] **Announce** (for major features)
  - Dev mailing list
  - Community channels

---

## 12. Quick Reference

### 12.1 Key File Locations

#### Scala/Java Code

```
gluten-substrait/src/main/scala/org/apache/gluten/
├── execution/                          # Transformer implementations
│   ├── BasicPhysicalOperatorTransformer.scala  # Filter, Project bases
│   ├── LimitExecTransformer.scala
│   ├── ProjectExecTransformer.scala
│   ├── FilterExecTransformer.scala
│   ├── SortExecTransformer.scala
│   ├── HashAggregateExecBaseTransformer.scala
│   └── HashJoinExecTransformer.scala
├── expression/                         # Expression transformers
│   ├── ExpressionConverter.scala
│   └── ExpressionTransformer.scala
├── substrait/                          # Substrait plan generation
│   ├── SubstraitContext.scala
│   └── rel/
│       ├── RelBuilder.scala            # Substrait RelNode builders
│       └── RelNode.scala
└── backendsapi/                        # Backend abstraction
    ├── BackendsApiManager.scala
    ├── SparkPlanExecApi.scala
    └── MetricsApi.scala

backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/
└── VeloxSparkPlanExecApi.scala         # Velox-specific implementations

backends-velox/src/test/scala/org/apache/gluten/execution/
└── [Various test suites]
```

#### C++ Code (Velox)

```
cpp/velox/
├── substrait/
│   ├── SubstraitToVeloxPlan.h          # Plan conversion header
│   ├── SubstraitToVeloxPlan.cc         # Plan conversion implementation
│   ├── SubstraitToVeloxExpr.h          # Expression conversion
│   └── SubstraitToVeloxExpr.cc
├── jni/
│   └── VeloxJniWrapper.cc              # JNI entry points
└── tests/
    └── [Test files]
```

### 12.2 Important Traits and Classes

| Trait/Class | Purpose | Key Methods |
|-------------|---------|-------------|
| `TransformSupport` | Base trait for all transformable operators | `doValidate()`, `doTransform()` |
| `UnaryTransformSupport` | Single-child operators | Extends `TransformSupport` |
| `BinaryTransformSupport` | Two-child operators | Extends `TransformSupport` |
| `LeafTransformSupport` | No-child operators (scans) | Extends `TransformSupport` |
| `ExpressionTransformer` | Expression conversion | `doTransform()` |
| `RelBuilder` | Substrait plan construction | `makeProjectRel()`, `makeFilterRel()`, etc. |
| `BackendsApiManager` | Backend routing | `getSparkPlanExecApiInstance()` |

### 12.3 Common Commands

#### Build

```bash
# Full build
./dev/builddeps-veloxbe.sh --build_tests=ON
./dev/build.sh --backends-velox

# Quick Scala rebuild
./build/sbt compile

# C++ rebuild
cd cpp/build && make -j$(nproc)
```

#### Test

```bash
# All tests
./build/sbt "backends-velox/test"

# Specific suite
./build/sbt "backends-velox/testOnly org.apache.gluten.execution.MyOperatorSuite"

# Specific test
./build/sbt "backends-velox/testOnly MyOperatorSuite -- -z test_name"
```

#### Format

```bash
# Scala
./dev/format-scala-code.sh

# C++
./dev/formatcppcode.sh

# License headers
dev/check.py header main --fix
```

#### Check

```bash
# Scalastyle
./build/sbt scalastyle

# Specific module
./build/sbt "backends-velox/scalastyle"
```

### 12.4 Substrait RelNode Types

| Spark Operator | Substrait RelNode | RelBuilder Method |
|----------------|-------------------|-------------------|
| LimitExec | FetchRel | `makeFetchRel()` |
| FilterExec | FilterRel | `makeFilterRel()` |
| ProjectExec | ProjectRel | `makeProjectRel()` |
| SortExec | SortRel | `makeSortRel()` |
| HashAggregateExec | AggregateRel | `makeAggregateRel()` |
| HashJoinExec | JoinRel | `makeJoinRel()` |
| UnionExec | SetRel | `makeSetRel()` |
| FileSourceScanExec | ReadRel | `makeReadRel()` |

### 12.5 Validation Result Patterns

```scala
// Success
ValidationResult.succeeded

// Failure
ValidationResult.failed("Reason for failure")

// Conditional
if (condition) {
  ValidationResult.succeeded
} else {
  ValidationResult.failed("Condition not met")
}

// Combining validations
val result1 = validatePart1()
if (!result1.isValid) return result1
val result2 = validatePart2()
if (!result2.isValid) return result2
ValidationResult.succeeded
```

### 12.6 Common Pitfalls

1. **Forgetting Validation vs Transform Distinction**
   - Validation should be fast, no expensive operations
   - Use `input = null` or mock input in validation

2. **Not Handling Nulls**
   - Always test with nullable columns
   - Check Velox's null handling semantics

3. **Schema Mismatch**
   - Ensure `output` attribute list matches actual output
   - Check attribute IDs and nullability

4. **Missing Metrics**
   - Every operator needs metrics
   - Use backend API for metric definitions

5. **Incomplete Expression Conversion**
   - Not all Spark expressions may be supported
   - Validate each expression type

6. **Forgetting withNewChildInternal**
   - Required for Spark's tree transformation
   - Just copy the case class with new child

### 12.7 Quick Find Commands

Essential commands to quickly locate code in the codebase:

```bash
# Find a specific transformer
find gluten-substrait/src/main/scala -name "*LimitExec*.scala"

# Find all transformers
find gluten-substrait/src/main/scala/org/apache/gluten/execution -name "*Transformer.scala"

# Find expression converters
find gluten-substrait/src/main/scala/org/apache/gluten/expression -name "*.scala"

# Search for a specific operator usage
grep -r "LimitExecTransformer" gluten-substrait/src/main/scala

# Find where an operator is created (in transformation rules)
grep -r "genLimitExecTransformer" --include="*.scala"

# Find Velox backend implementation
grep -r "toVeloxPlan.*FetchRel" cpp/velox/substrait/

# Find test cases for an operator
find backends-velox/src/test/scala -name "*.scala" -exec grep -l "LIMIT" {} \;

# Find all uses of a specific Substrait method
grep -r "makeFetchRel" gluten-substrait/

# Find expression mappings
grep -r "classOf\[Add\]" gluten-substrait/src/main/scala/org/apache/gluten/expression/

# Find metrics definitions
grep -r "genLimitTransformerMetrics" --include="*.scala"

# Find validation implementations
grep -r "def doValidateInternal" gluten-substrait/src/main/scala/org/apache/gluten/execution/

# Find C++ operator implementations
find cpp/velox -name "*.cc" -exec grep -l "LimitNode" {} \;
```

### 12.8 File Path Quick Reference by Task

**When you want to...**

| Task | File Location |
|------|---------------|
| Create a new transformer | `gluten-substrait/src/main/scala/org/apache/gluten/execution/MyOperatorExecTransformer.scala` |
| Add backend API method | `gluten-substrait/src/main/scala/org/apache/gluten/backendsapi/SparkPlanExecApi.scala` |
| Implement Velox backend | `backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxSparkPlanExecApi.scala` |
| Add Velox C++ conversion | `cpp/velox/substrait/SubstraitToVeloxPlan.cc` |
| Add expression support | `gluten-substrait/src/main/scala/org/apache/gluten/expression/MyExpressionTransformer.scala` |
| Map expression to Substrait | `gluten-substrait/src/main/scala/org/apache/gluten/expression/ExpressionMappings.scala` |
| Add metrics | `backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxMetricsApi.scala` |
| Write tests | `backends-velox/src/test/scala/org/apache/gluten/execution/MyOperatorSuite.scala` |
| Check operator support | `docs/velox-backend-support-progress.md` |
| Add configuration | `gluten-core/src/main/scala/org/apache/gluten/config/GlutenConfig.scala` |

### 12.9 Useful Resources

**Documentation:**
- [Gluten Architecture](../index.md)
- [Velox Backend Support Progress](../velox-backend-support-progress.md)
- [Contributing Guidelines](../../CONTRIBUTING.md)
- [New Developer Guide](NewToGluten.md)

**External Resources:**
- [Substrait Specification](https://substrait.io/)
- [Velox Documentation](https://facebookincubator.github.io/velox/)
- [Spark SQL Internals](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/)

**Community:**
- [GitHub Issues](https://github.com/apache/incubator-gluten/issues)
- [Dev Mailing List](dev@gluten.apache.org)
- [Slack/Discord](Check project README)

**Key Source Files to Study:**
1. `LimitExecTransformer.scala:29` - Simplest transformer example
2. `ProjectExecTransformer.scala` - Expression-based operator
3. `HashAggregateExecBaseTransformer.scala` - Complex operator example
4. `ExpressionConverter.scala:42` - Expression conversion logic
5. `SubstraitContext.scala:48` - Context and state tracking
6. `ValidationResult.scala:26` - Validation pattern
7. `BackendsApiManager.scala:21` - Backend routing
8. `cpp/velox/substrait/SubstraitToVeloxPlan.cc` - Velox C++ conversion

### 12.10 Common Code Snippets

**Copy-Paste Template for New Simple Transformer:**
```scala
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import scala.collection.JavaConverters._

case class MyOperatorExecTransformer(
    myParam: Int,
    child: SparkPlan
  ) extends UnaryTransformSupport {

  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genMyOperatorMetrics(sparkContext)

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): MyOperatorExecTransformer =
    copy(child = newChild)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genMyOperatorMetricsUpdater(metrics)

  override protected def doValidateInternal(): ValidationResult = {
    val context = new SubstraitContext
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = getRelNode(context, operatorId, myParam, child.output, null, true)
    doNativeValidation(context, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = getRelNode(context, operatorId, myParam, child.output, childCtx.root, false)
    TransformContext(output, relNode)
  }

  def getRelNode(
      context: SubstraitContext,
      operatorId: Long,
      myParam: Int,
      inputAttributes: Seq[Attribute],
      input: RelNode,
      validation: Boolean): RelNode = {
    if (!validation) {
      RelBuilder.makeMyOperatorRel(input, myParam, context, operatorId)
    } else {
      RelBuilder.makeMyOperatorRel(
        input,
        myParam,
        RelBuilder.createExtensionNode(inputAttributes.asJava),
        context,
        operatorId
      )
    }
  }
}
```

**Copy-Paste Template for Test Suite:**
```scala
package org.apache.gluten.execution

import org.apache.spark.SparkConf

class MyOperatorSuite extends VeloxWholeStageTransformerSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  test("my_operator_basic") {
    runQueryAndCompare("SELECT * FROM lineitem LIMIT 10") { df =>
      assert(df.count() == 10)
    }
  }

  test("my_operator_in_plan") {
    runQueryAndCompare("SELECT * FROM lineitem LIMIT 10") {
      checkGlutenOperatorMatch[MyOperatorExecTransformer]
    }
  }
}
```

---

## Conclusion

This guide has covered the complete process of adding operator support to Apache Gluten:

1. **Understanding**: Architecture, transformation pipeline, validation vs execution
2. **Research**: Studying existing patterns, checking backend support
3. **Implementation**: Scala transformers, backend integration, C++ conversion
4. **Testing**: Comprehensive test coverage, edge cases, performance
5. **Quality**: Code formatting, style checks, documentation
6. **Contribution**: PR creation, review process, community guidelines

**Key Takeaways:**
- Start with simple operators (like Limit) before tackling complex ones
- Follow existing patterns closely - consistency is crucial
- Test thoroughly - edge cases matter
- Validate early - catch unsupported cases during planning
- Document well - help future contributors

**Next Steps:**
- Pick an operator that currently falls back
- Study similar implementations
- Implement transformer with tests
- Submit PR and iterate with reviewers

Good luck with your contributions to Apache Gluten!

---

**Document Version:** 1.0
**Last Updated:** 2025-01-XX
**Maintainers:** Apache Gluten Community
