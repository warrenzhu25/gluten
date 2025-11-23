# Memory Management and Fallback in Gluten

## Table of Contents

- [Overview](#overview)
- [Core Memory Architecture](#core-memory-architecture)
  - [Memory Target Abstraction](#memory-target-abstraction)
  - [Memory Target Hierarchy](#memory-target-hierarchy)
- [Dynamic Off-Heap Sizing](#dynamic-off-heap-sizing)
  - [Unified Memory Management](#unified-memory-management)
  - [Dynamic Memory Balancing Mechanism](#dynamic-memory-balancing-mechanism)
- [Hierarchical Memory Tracking](#hierarchical-memory-tracking)
  - [Tree Structure](#tree-structure)
  - [Memory Acquisition Flow](#memory-acquisition-flow)
  - [Integration with Spark Memory Manager](#integration-with-spark-memory-manager)
- [Spilling Mechanism](#spilling-mechanism)
  - [Spiller Interface](#spiller-interface)
  - [Hierarchical Spilling Algorithm](#hierarchical-spilling-algorithm)
  - [Native Memory Manager Spiller](#native-memory-manager-spiller)
- [Memory Isolation and Task Slots](#memory-isolation-and-task-slots)
- [Over-Acquisition Strategy](#over-acquisition-strategy)
- [Reservation Listener and Block-Based Allocation](#reservation-listener-and-block-based-allocation)
- [Fallback to Spark Execution](#fallback-to-spark-execution)
  - [Within-Task Fallback Behavior](#within-task-fallback-behavior)
  - [Stage-Level Resource Profile Adjustment](#stage-level-resource-profile-adjustment)
  - [Unified Memory Management (Runtime Balancing)](#unified-memory-management-runtime-balancing)
  - [Memory at Transition Boundaries](#memory-at-transition-boundaries)
- [Configuration Reference](#configuration-reference)
- [Performance Tuning Tips](#performance-tuning-tips)
- [Troubleshooting](#troubleshooting)
- [Code References](#code-references)

---

## Overview

Gluten implements a sophisticated multi-layered memory management system that dynamically allocates and tracks off-heap memory while integrating with Spark's memory management. The system includes:

- **Automatic spilling mechanisms** to prevent OOM errors
- **Dynamic memory adjustment** between heap and off-heap
- **Hierarchical memory tracking** with Spark integration
- **Graceful fallback** to Spark vanilla execution when resources are constrained

This document provides a comprehensive guide to understanding and configuring Gluten's memory management, particularly in scenarios involving fallback to Spark execution.

---

## Core Memory Architecture

### Memory Target Abstraction

The foundation is the `MemoryTarget` interface, which provides a consistent API for memory operations across different allocation strategies.

**Location**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/MemoryTarget.java`

```java
public interface MemoryTarget {
  long borrow(long size);  // Request memory allocation
  long repay(long size);   // Release memory
  long usedBytes();        // Track current usage
  <T> T accept(MemoryTargetVisitor<T> visitor);
}
```

This abstraction enables:
- **Uniform memory management** across different backends (Velox, ClickHouse)
- **Composable decorators** for adding features like spilling, over-acquisition
- **Hierarchical organization** for tracking memory at different granularities

### Memory Target Hierarchy

Gluten uses a **decorator pattern** to compose multiple memory management behaviors into a layered structure.

**Location**: `gluten-arrow/src/main/java/org/apache/gluten/memory/listener/ReservationListeners.java`

**Typical Layering**:

```
ThrowOnOomMemoryTarget(
  OverAcquire(
    DynamicOffHeapSizingIfEnabled(TreeMemoryConsumer),
    DynamicOffHeapSizingIfEnabled(OverConsumer),
    overAcquiredRatio))
```

**Layer Responsibilities** (from innermost to outermost):

1. **TreeMemoryConsumer** - Hierarchical memory tracking with Spark integration
2. **DynamicOffHeapSizingMemoryTarget** - Dynamic JVM heap/off-heap balancing (optional)
3. **OverAcquire** - Proactive memory over-acquisition for safety
4. **ThrowOnOomMemoryTarget** - Final OOM handling and error reporting

This layered approach allows each decorator to add specific functionality without modifying the core allocation logic.

---

## Dynamic Off-Heap Sizing

### Unified Memory Management

**Location**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/DynamicOffHeapSizingMemoryTarget.java`

When enabled, this feature treats **on-heap and off-heap memory as a unified pool**, allowing dynamic balancing between the two based on actual usage patterns.

**Configuration**:
```properties
spark.gluten.memory.dynamic.offHeap.sizing.enabled=true
spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction=0.9
```

**Key Benefits**:
- Better memory utilization across heap and off-heap
- Automatic adjustment based on workload characteristics
- Reduces need for manual tuning of heap vs off-heap ratios

### Dynamic Memory Balancing Mechanism

The system dynamically **shrinks JVM heap memory** to make room for off-heap allocations:

```java
public long borrow(long size) {
  long totalHeapMemory = Runtime.getRuntime().totalMemory();
  long freeHeapMemory = Runtime.getRuntime().freeMemory();
  long usedOffHeapMemory = USED_OFF_HEAP_BYTES.get();

  if (exceedsMaxMemoryUsage(totalHeapMemory, usedOffHeapMemory, size, 1.0)) {
    // Synchronous GC to shrink JVM heap
    synchronized (JVM_SHRINK_SYNC_OBJECT) {
      shrinkOnHeapMemory(totalHeapMemory, freeHeapMemory, false);
    }
  } else if (shouldTriggerAsyncOnHeapMemoryShrink(...)) {
    // Proactive async GC in thread pool
    SparkThreadPoolUtil.triggerGCInThreadPool(() -> {
      shrinkOnHeapMemory(totalJvmMem, freeJvmMem, true);
    });
  }

  USED_OFF_HEAP_BYTES.addAndGet(size);
  return size;
}
```

**JVM Heap Shrinking Process**:

1. Temporarily adjusts `MaxHeapFreeRatio` to 0 via JMX
2. Calls `System.gc()` to trigger full garbage collection
3. JVM releases unused heap memory back to the operating system
4. More off-heap memory becomes available for native allocations
5. Restores original `MaxHeapFreeRatio` setting

**Important Notes**:
- Requires **Java 9+** for JMX access to `com.sun.management:type=HotSpotDiagnostic`
- May cause **GC pauses** during synchronous shrinking
- Async shrinking available for proactive memory management
- This is an **experimental feature** - test thoroughly before production use

---

## Hierarchical Memory Tracking

### Tree Structure

**Location**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/spark/TreeMemoryConsumer.java`

Gluten creates a hierarchical memory structure that mirrors the execution hierarchy and integrates with Spark's TaskMemoryManager:

```
TaskMemoryManager (Spark)
└── TreeMemoryConsumer (Spark MemoryConsumer)
    └── Child Node (e.g., "Gluten.Runtime")
        └── Child Node (e.g., "ArrowContextInstance")
            └── Virtual Children (native allocations)
```

**Benefits**:
- **Fine-grained tracking**: Memory usage visible at operator/component level
- **Targeted spilling**: Spill from specific components based on usage
- **Debugging**: Easier to identify memory-hungry components

### Memory Acquisition Flow

```java
@Override
public long borrow(long size) {
  if (size == 0) return 0;

  long acquired = acquireMemory(size);  // Calls Spark's TaskMemoryManager
  recorder.inc(acquired);
  return acquired;
}
```

**Tree Node Behavior**:

```java
public long borrow(long size) {
  ensureFreeCapacity(size);  // May trigger spilling
  return borrow0(Math.min(freeBytes(), size));
}

private boolean ensureFreeCapacity(long bytesNeeded) {
  while (true) {
    long freeBytes = freeBytes();
    if (freeBytes >= bytesNeeded) return true;

    // Spill to free memory
    long bytesToSpill = bytesNeeded - freeBytes;
    long spilledBytes = TreeMemoryTargets.spillTree(this, bytesToSpill);

    if (spilledBytes == 0) return false;  // OOM
  }
}
```

This ensures that before allocating memory, the system attempts to free up space through spilling if necessary.

### Integration with Spark Memory Manager

**Location**: `gluten-core/src/main/scala/org/apache/spark/memory/GlobalOffHeapMemoryTarget.scala`

```scala
override def borrow(size: Long): Long = {
  memoryManagerOption().map { mm =>
    val mode = if (dynamicOffHeapSizingEnabled)
                 MemoryMode.ON_HEAP
               else
                 MemoryMode.OFF_HEAP

    val succeeded = mm.acquireStorageMemory(
      BlockId(s"test_${UUID.randomUUID()}"), size, mode)

    if (succeeded) {
      recorder.inc(size)
      size
    } else {
      // OOM - log diagnostic info
      logError("Spark memory exhausted...")
      0
    }
  }.getOrElse(size)
}
```

**Key Integration Points**:
- Uses Spark's `acquireStorageMemory` or `acquireExecutionMemory`
- Respects Spark's memory limits and eviction policies
- Participates in Spark's spilling mechanism
- When dynamic off-heap sizing is enabled, allocates from ON_HEAP pool in Spark's view

---

## Spilling Mechanism

### Spiller Interface

**Location**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/Spiller.java`

```java
public interface Spiller {
  long spill(MemoryTarget self, Phase phase, long size);

  enum Phase {
    SHRINK,  // First attempt: shrink memory usage without disk I/O
    SPILL    // Second attempt: spill to disk
  }
}
```

**Two-Phase Spilling Strategy**:

1. **SHRINK Phase**: Try to free memory by compacting data structures, clearing caches, etc.
   - No disk I/O involved
   - Faster than actual spilling
   - Examples: Clear hash table caches, compact memory pools

2. **SPILL Phase**: If SHRINK is insufficient, write data to disk
   - More expensive (disk I/O)
   - Can free larger amounts of memory
   - Examples: Spill sort buffers, aggregate state to disk

### Hierarchical Spilling Algorithm

**Location**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/TreeMemoryTargets.java`

The spilling algorithm walks the memory tree, attempting to free memory hierarchically:

```java
public static long spillTree(TreeMemoryTarget node, final long bytes) {
  long remainingBytes = bytes;
  for (Spiller.Phase phase : Spiller.Phase.values()) {
    if (remainingBytes <= 0) break;
    remainingBytes -= spillTree(node, phase, remainingBytes);
  }
  return bytes - remainingBytes;
}

private static long spillTree(TreeMemoryTarget node, Phase phase, long bytes) {
  // Sort children by used bytes, descending (spill from largest first)
  Queue<TreeMemoryTarget> q = new PriorityQueue<>(
    (o1, o2) -> -(compare(o1.usedBytes(), o2.usedBytes()))
  );
  q.addAll(node.children().values());

  // Spill from children first (largest first)
  long remainingBytes = bytes;
  while (q.peek() != null && remainingBytes > 0) {
    TreeMemoryTarget head = q.remove();
    long spilled = spillTree(head, phase, remainingBytes);
    remainingBytes -= spilled;
  }

  // If still not enough, spill self
  if (remainingBytes > 0) {
    long spilled = node.getNodeSpiller().spill(node, phase, remainingBytes);
    remainingBytes -= spilled;
  }

  return bytes - remainingBytes;
}
```

**Algorithm Characteristics**:
- **Greedy**: Targets largest memory consumers first
- **Recursive**: Descends into child nodes before spilling parent
- **Multi-phase**: Tries SHRINK before SPILL for each node
- **Best-effort**: Returns amount actually spilled (may be less than requested)

### Native Memory Manager Spiller

**Location**: `gluten-arrow/src/main/scala/org/apache/gluten/memory/NativeMemoryManager.scala`

```scala
spillers.append(new Spiller() {
  override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long =
    phase match {
      case Spiller.Phase.SHRINK =>
        val shrunk = NativeMemoryManagerJniWrapper.shrink(handle, size)
        LOGGER.info(s"NativeMemoryManager: Shrunk $shrunk / $size bytes")
        shrunk
      case _ => 0L
    }
})
```

The native memory manager delegates SHRINK operations to the C++ layer, which can:
- Compact memory pools
- Clear internal caches
- Release unused memory arenas
- Trim memory allocators

---

## Memory Isolation and Task Slots

### Memory Isolation Mode

**Location**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/spark/TreeMemoryConsumers.java`

When memory isolation is enabled, each task is limited to a fair share of executor memory:

```java
public TreeMemoryTarget isolatedRoot() {
  return ofCapacity(GlutenCoreConfig.get().conservativeTaskOffHeapMemorySize());
}
```

**Calculation** (from `gluten-core/src/main/scala/org/apache/gluten/GlutenPlugin.scala`):

```scala
val taskSlots = SparkResourceUtil.getTaskSlots(conf)
val offHeapPerTask = offHeapSize / taskSlots

// Conservative estimate accounting for storage memory
val fraction = 1.0d - conf.getDouble("spark.memory.storageFraction", 0.5d)
val conservativeOffHeapPerTask = (offHeapSize * fraction).toLong / taskSlots
```

**Configuration**:
```properties
spark.gluten.memory.isolation=true
```

**When to Enable**:
- ✅ High concurrency (many task slots per executor)
- ✅ Workloads with variable memory consumption
- ✅ Preventing single task from monopolizing memory
- ❌ Single-slot executors (overhead without benefit)
- ❌ Batch workloads with uniform memory usage

When enabled, prevents OOM in concurrent execution scenarios by ensuring each task stays within its allocated budget.

---

## Over-Acquisition Strategy

### OverAcquire Wrapper

**Location**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/OverAcquire.java`

The over-acquisition strategy proactively tests if extra memory can be acquired, providing a safety buffer:

```java
public long borrow(long size) {
  long granted = target.borrow(size);
  if (granted >= size) {
    // Over-acquire additional memory as buffer
    long majorSize = target.usedBytes();
    long overSize = (long) (ratio * majorSize);
    long overAcquired = overTarget.borrow(overSize);

    // Immediately release - just testing if memory is available
    overTarget.repay(overAcquired);
  }
  return granted;
}
```

**Purpose**:
- Tests if **30% extra memory** is available (default via `spark.gluten.memory.overAcquiredMemoryRatio`)
- Provides safety buffer for non-spillable operations (e.g., final aggregation)
- Prevents OOM during operations that cannot be easily paused/spilled
- Acts as an **early warning system** - if over-acquisition fails, memory pressure is high

**Configuration**:
```properties
spark.gluten.memory.overAcquiredMemoryRatio=0.3  # 30% buffer
```

---

## Reservation Listener and Block-Based Allocation

### ManagedReservationListener

**Location**: `gluten-arrow/src/main/java/org/apache/gluten/memory/listener/ManagedReservationListener.java`

```java
public long reserve(long size) {
  synchronized (sharedLock) {
    long granted = target.borrow(size);
    sharedUsage.inc(granted);
    return granted;
  }
}
```

### Block-Based Reservation

**Location**: `gluten-arrow/src/main/java/org/apache/gluten/memory/arrow/alloc/ManagedAllocationListener.java`

Memory is reserved in blocks to reduce allocation overhead:

```java
public void onPreAllocation(long size) {
  long requiredBlocks = updateReservation(size);
  if (requiredBlocks == 0) return;

  long toBeAcquired = requiredBlocks * BLOCK_SIZE;  // Default 8MB
  long granted = target.borrow(toBeAcquired);
  sharedUsage.inc(granted);
}

public long updateReservation(long bytesToAdd) {
  synchronized (this) {
    long newBytesReserved = bytesReserved + bytesToAdd;
    // Ceiling division to block size
    long newBlocksReserved = (newBytesReserved == 0L) ? 0L
                           : (newBytesReserved - 1L) / BLOCK_SIZE + 1L;
    long requiredBlocks = newBlocksReserved - blocksReserved;
    bytesReserved = newBytesReserved;
    blocksReserved = newBlocksReserved;
    return requiredBlocks;
  }
}
```

**Configuration**:
```properties
spark.gluten.memory.reservationBlockSize=8388608  # 8MB default
```

**Benefits**:
- Reduces allocation frequency (bulk reservation)
- Amortizes overhead of Spark memory manager calls
- Simplifies tracking (block-level granularity)
- Aligns with typical memory allocator chunk sizes

---

## Fallback to Spark Execution

### Within-Task Fallback Behavior

**Key Finding**: When an operator falls back to Spark execution mid-task, memory resources are **NOT immediately released or reallocated**.

**Memory Lifecycle** (from `gluten-core/src/main/scala/org/apache/spark/task/TaskResources.scala`):

```scala
tc.addTaskCompletionListener(new TaskCompletionListener {
  override def onTaskCompletion(context: TaskContext): Unit = {
    RESOURCE_REGISTRIES.synchronized {
      currentTaskRegistries.releaseAll()  // All resources released at task end
      RESOURCE_REGISTRIES.remove(context)
    }
  }
})
```

**Memory Release Pattern**:
- ✅ All native memory resources released **at task completion**
- ❌ No bulk memory release during native → fallback transition
- ✅ Gradual release as columnar batches are consumed and GC'd
- ❌ No reallocation from off-heap to on-heap mid-task

**Example Flow**:

```
Task: NativeScan → NativeFilter → [FALLBACK: UDF] → NativeProject

Memory Usage Timeline:
├─ t0: Task starts, NativeMemoryManager allocated
├─ t1: NativeScan allocates 500MB off-heap ✓
├─ t2: NativeFilter uses off-heap memory ✓
├─ t3: ColumnarToRow converts batches (gradual release)
├─ t4: UDF (fallback) uses on-heap memory ✓
├─ t5: RowToColumnar converts back, allocates new off-heap ✓
├─ t6: NativeProject allocates more off-heap ✓
└─ t7: Task completes → ALL off-heap released ✓
```

**Implications**:
- Native and fallback operators **share the same memory pool** (TaskMemoryManager)
- Memory pressure can trigger spilling in native operators to help fallback operators
- No special memory adjustment happens at fallback boundaries within a task

### Stage-Level Resource Profile Adjustment

**Location**: `gluten-substrait/src/main/scala/org/apache/spark/sql/execution/GlutenAutoAdjustStageResourceProfile.scala`

Gluten provides **proactive stage-level memory adjustment** for Adaptive Query Execution (AQE):

```scala
// Whole stage fallback
if (wholeStageFallback) {
  val newMemoryAmount = memoryRequest.get.amount * glutenConf.autoAdjustStageRPHeapRatio
  val newExecutorMemory = new ExecutorResourceRequest(
    ResourceProfile.MEMORY, newMemoryAmount.toLong)
  executorResource.put(ResourceProfile.MEMORY, newExecutorMemory)

  val newExecutorOffheap = new ExecutorResourceRequest(
    ResourceProfile.OFFHEAP_MEM, offheapRequest.get.amount / 10)
  executorResource.put(ResourceProfile.OFFHEAP_MEM, newExecutorOffheap)
}

// Partial fallback (exceeds threshold)
else if (fallbackRatio > glutenConf.autoAdjustStageRPFallenNodeThreshold) {
  val newMemoryAmount = memoryRequest.get.amount * glutenConf.autoAdjustStageRPHeapRatio
  val newExecutorMemory = new ExecutorResourceRequest(
    ResourceProfile.MEMORY, newMemoryAmount.toLong)
  executorResource.put(ResourceProfile.MEMORY, newExecutorMemory)

  val newOffheapAmount = (offheapRequest.get.amount *
    glutenConf.autoAdjustStageRPOffheapRatio).toLong
  val newExecutorOffheap = new ExecutorResourceRequest(
    ResourceProfile.OFFHEAP_MEM, newOffheapAmount)
  executorResource.put(ResourceProfile.OFFHEAP_MEM, newExecutorOffheap)
}
```

**Configuration**:
```properties
spark.gluten.sql.autoAdjustStageResourceProfile.enabled=false
spark.gluten.sql.autoAdjustStageResourceProfile.heapRatio=1.3
spark.gluten.sql.autoAdjustStageResourceProfile.offheapRatio=0.6
spark.gluten.sql.autoAdjustStageResourceProfile.fallenNodeThreshold=0.5
```

**Behavior**:

| Scenario | Heap Adjustment | Off-Heap Adjustment |
|----------|----------------|-------------------|
| Whole stage fallback | × 1.3 (increase) | ÷ 10 (reduce drastically) |
| Partial fallback (>50% nodes) | × 1.3 (increase) | × 0.6 (reduce moderately) |
| Native execution | No change | No change |

**Example**:

```
Stage 1 (Native execution):
  Resource Profile: heap=10GB, off-heap=20GB
  Executes successfully

Stage 2 (Detected 80% fallback):
  Auto-adjust triggered!
  New Resource Profile: heap=13GB (↑30%), off-heap=12GB (↓40%)
  New tasks use adjusted resources

Stage 3 (Back to native):
  Resource Profile: heap=10GB, off-heap=20GB
  Reverts to native profile
```

**Requirements**:
- ✅ AQE enabled (`spark.sql.adaptive.enabled=true`)
- ✅ Spark 3.1+ with ResourceProfile support
- ✅ Dynamic allocation or sufficient executor resources
- ✅ Gluten config enabled

**Benefits**:
- Prevents OOM in fallback-heavy stages
- Automatically adapts to query characteristics
- No manual intervention required
- Works seamlessly with AQE's adaptive planning

### Unified Memory Management (Runtime Balancing)

**Location**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/DynamicOffHeapSizingMemoryTarget.java`

This feature provides **runtime dynamic adjustment** between heap and off-heap memory within the same executor.

**Configuration**:
```properties
spark.gluten.memory.dynamic.offHeap.sizing.enabled=true
spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction=0.9
```

**How It Works**:

Treats JVM heap + off-heap as a **unified memory pool**:

```java
borrow(size) {
  totalMemory = Runtime.totalMemory();  // Current JVM heap
  offHeapUsed = USED_OFF_HEAP_BYTES.get();

  if (totalMemory + offHeapUsed + size > maxMemory) {
    // Shrink JVM heap to make room for off-heap!
    shrinkOnHeapMemory();

    // Shrinking process:
    // 1. Set MaxHeapFreeRatio to 0 via JMX
    // 2. Call System.gc() → triggers full GC
    // 3. JVM releases unused heap back to OS
    // 4. More off-heap memory becomes available
    // 5. Restore MaxHeapFreeRatio to original value
  }

  USED_OFF_HEAP_BYTES.addAndGet(size);
}
```

**Example Scenarios**:

**Native-Heavy Workload**:
```
Initial state: heap=15GB used, off-heap=5GB used
Native execution needs 10GB more off-heap
→ Trigger GC, shrink JVM heap to 8GB
→ Off-heap now has 17GB available (5GB + 12GB freed from heap)
→ Allocation succeeds
```

**Fallback-Heavy Workload**:
```
Initial state: heap=8GB used, off-heap=15GB used
Fallback execution needs 8GB more on-heap
→ Off-heap releases memory via spilling
→ Native usage drops to 10GB
→ JVM heap can grow to accommodate fallback needs
→ On-heap expands to 13GB
```

**Trade-offs**:
- ✅ Better memory utilization (no wasted capacity)
- ✅ Automatic adjustment to workload patterns
- ⚠️ May cause GC pauses (synchronous shrinking)
- ⚠️ Requires Java 9+ with JMX access
- ⚠️ Experimental feature - test before production

### Memory at Transition Boundaries

#### ColumnarToRow (Native → Fallback)

**Location**: `backends-velox/src/main/scala/org/apache/gluten/execution/VeloxColumnarToRowExec.scala`

```scala
override def doExecuteInternal(): RDD[InternalRow] = {
  child.executeColumnar().mapPartitions { it =>
    VeloxColumnarToRowExec.toRowIterator(it, ...)
  }
}

// Memory release happens here:
recycleIterator = (iter: Iterator[InternalRow]) => {
  it.close()  // Releases native ColumnarBatch backing memory
}
```

**Memory Behavior**:
- ✅ **Gradual release**: Memory freed as batches are consumed
- ❌ **No bulk release**: Not all memory released at once
- ✅ **GC-driven**: JVM garbage collection reclaims released native memory
- ❌ **Not reallocated**: Memory returns to pool, not transferred to fallback

#### RowToColumnar (Fallback → Native)

**Location**: `gluten-substrait/src/main/scala/org/apache/gluten/execution/RowToArrowColumnarExec.scala`

```scala
override def doExecuteColumnar(): RDD[ColumnarBatch] = {
  child.execute().mapPartitions { rowIterator =>
    // Allocates NEW off-heap memory for columnar format
    ArrowWritableColumnVector.allocate(...)
  }
}
```

**Memory Behavior**:
- ✅ **New allocations**: Fresh off-heap memory allocated for columnar batches
- ⚠️ **May trigger spilling**: If insufficient memory available
- ❌ **No special adjustment**: Uses standard MemoryTarget flow
- ✅ **Respects memory limits**: Subject to task memory constraints

### OOM Detection and Reporting

**Location**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/ThrowOnOomMemoryTarget.java`

```java
@Override
public long borrow(long size) {
  long granted = target.borrow(size);
  if (granted >= size) return granted;

  // OOM detected - clean up partial grant
  if (granted != 0L) target.repay(granted);

  // Log memory usage for diagnostics
  if (TaskResources.inSparkTask()) {
    TaskResources.getLocalTaskContext()
      .taskMemoryManager().showMemoryUsage();
  }

  // Build diagnostic message
  String errorMsg = buildErrorMessage(size, granted);
  errorMsg += SparkMemoryUtil.dumpMemoryTargetStats(target);

  throw new OutOfMemoryException(errorMsg);
}
```

### Validation and Fallback Tagging

**Location**: `gluten-substrait/src/main/scala/org/apache/gluten/execution/ValidationResult.scala`

```scala
sealed trait ValidationResult {
  def ok(): Boolean
  def reason(): String
}

object ValidationResult {
  def failed(reason: String): ValidationResult = Failed(reason)

  implicit class EncodeFallbackTagImplicits(result: ValidationResult) {
    def tagOnFallback(plan: TreeNode[_]): Unit = {
      if (result.ok()) return
      FallbackTags.add(plan, result)
    }
  }
}
```

**Fallback Decision Process**:
1. Operator validates if it can execute natively
2. Validation checks: memory availability, supported operations, configurations
3. If validation fails → operator tagged with `FallbackTag`
4. Tagged operators execute using Spark's vanilla execution engine
5. No memory is "transferred" - each mode uses its allocation path

### Retry on OOM for Multi-Slot Executors

**Location**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/RetryOnOomMemoryTarget.java`

```java
@Override
public long borrow(long size) {
  long granted = target.borrow(size);
  if (granted < size) {
    LOG.info("Granted {} < requested {}, retrying...", granted, size);
    long remaining = size - granted;

    // Trigger extra spilling across ALL consumers in executor
    onRetry.run();

    granted += target.borrow(remaining);
    LOG.info("After retry: granted {}, requested {}.", granted, size);
  }
  return granted;
}
```

**Retry Callback** (from `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/MemoryTargets.java`):

```java
return new RetryOnOomMemoryTarget(consumer, () -> {
  LOG.info("Request for spilling on consumer {}...", consumer.name());
  // Spill from root to affect ALL consumers in executor
  long spilled = TreeMemoryTargets.spillTree(root, Long.MAX_VALUE);
  LOG.info("Consumer {} gets {} bytes from spilling.", consumer.name(), spilled);
});
```

**Purpose**:
- In multi-slot executors, one task's memory pressure can trigger cross-task spilling
- Helps redistribute memory when one task is starved
- Provides one more opportunity before OOM

---

## Configuration Reference

### Memory Management Core

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.gluten.memory.untracked` | `false` | Disable memory tracking (testing only) |
| `spark.gluten.memory.isolation` | `false` | Enable per-task memory limits |
| `spark.gluten.memory.reservationBlockSize` | `8388608` (8MB) | Block size for memory reservations |
| `spark.gluten.memory.overAcquiredMemoryRatio` | `0.3` | Extra memory buffer ratio (30%) |

### Dynamic Off-Heap Sizing

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.gluten.memory.dynamic.offHeap.sizing.enabled` | `false` | Enable unified heap/off-heap management |
| `spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction` | `0.9` | Memory fraction for dynamic sizing |

### Stage Resource Profile Auto-Adjustment

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.gluten.sql.autoAdjustStageResourceProfile.enabled` | `false` | Enable automatic stage resource adjustment |
| `spark.gluten.sql.autoAdjustStageResourceProfile.heapRatio` | `1.3` | Heap multiplier for fallback stages |
| `spark.gluten.sql.autoAdjustStageResourceProfile.offheapRatio` | `0.6` | Off-heap multiplier for partial fallback |
| `spark.gluten.sql.autoAdjustStageResourceProfile.fallenNodeThreshold` | `0.5` | Fallback ratio threshold (50%) |

### Spark Memory Settings (for reference)

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.memory.offHeap.enabled` | `false` | Enable off-heap memory for Spark |
| `spark.memory.offHeap.size` | `0` | Total off-heap memory size |
| `spark.memory.storageFraction` | `0.5` | Fraction reserved for storage |
| `spark.sql.adaptive.enabled` | `true` | Enable Adaptive Query Execution |

---

## Performance Tuning Tips

### Conservative Configuration (Recommended for Production)

**Best for**: Stable production workloads, mixed query types, unknown memory patterns

```properties
# Basic memory settings
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=20g  # Adjust based on executor memory

# Gluten settings
spark.gluten.memory.isolation=true
spark.gluten.memory.overAcquiredMemoryRatio=0.3
spark.gluten.memory.reservationBlockSize=8388608

# Enable stage-level adjustment with AQE
spark.sql.adaptive.enabled=true
spark.gluten.sql.autoAdjustStageResourceProfile.enabled=true
spark.gluten.sql.autoAdjustStageResourceProfile.heapRatio=1.3
spark.gluten.sql.autoAdjustStageResourceProfile.offheapRatio=0.6

# Do NOT enable experimental features
spark.gluten.memory.dynamic.offHeap.sizing.enabled=false
```

**Rationale**:
- Memory isolation prevents single task monopolizing resources
- Stage-level adjustment handles fallback gracefully
- Conservative over-acquisition ratio (30%) provides safety
- Avoids GC overhead from dynamic off-heap sizing

### Aggressive Configuration (Advanced Tuning)

**Best for**: Native-heavy workloads, performance testing, when fallback is rare

```properties
# Larger off-heap allocation
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=40g

# Aggressive settings
spark.gluten.memory.isolation=false  # Allow full executor memory usage
spark.gluten.memory.overAcquiredMemoryRatio=0.5  # Higher safety buffer
spark.gluten.memory.reservationBlockSize=16777216  # 16MB blocks

# Enable dynamic sizing (Java 9+ required)
spark.gluten.memory.dynamic.offHeap.sizing.enabled=true
spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction=0.95

# Stage adjustment still useful for rare fallbacks
spark.sql.adaptive.enabled=true
spark.gluten.sql.autoAdjustStageResourceProfile.enabled=true
```

**Trade-offs**:
- ✅ Maximum memory utilization
- ✅ Better performance for native operators
- ⚠️ Risk of OOM in high concurrency
- ⚠️ Potential GC pauses from dynamic sizing
- ⚠️ Requires careful monitoring

### Fallback-Heavy Workload Configuration

**Best for**: Queries with many UDFs, unsupported operators, frequent fallback

```properties
# Balanced memory allocation
spark.executor.memory=32g
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=12g  # Less off-heap since fallback uses heap

# Conservative Gluten settings
spark.gluten.memory.isolation=true
spark.gluten.memory.overAcquiredMemoryRatio=0.2  # Lower buffer

# Aggressive stage adjustment
spark.sql.adaptive.enabled=true
spark.gluten.sql.autoAdjustStageResourceProfile.enabled=true
spark.gluten.sql.autoAdjustStageResourceProfile.heapRatio=1.5  # More heap
spark.gluten.sql.autoAdjustStageResourceProfile.offheapRatio=0.4  # Less off-heap
spark.gluten.sql.autoAdjustStageResourceProfile.fallenNodeThreshold=0.3  # Earlier trigger
```

**Rationale**:
- More heap memory for fallback operators (larger executor heap)
- Less off-heap since native execution is limited
- Lower fallback threshold (30%) triggers adjustment earlier
- Higher heap ratio (1.5x) for fallback stages

### Memory Sizing Guidelines

#### Calculate Executor Off-Heap Memory

```
executor_memory = spark.executor.memory
task_slots = spark.executor.cores

# Conservative sizing (recommended)
off_heap_size = executor_memory * 0.6
per_task_memory = off_heap_size / task_slots

# Aggressive sizing (for native-heavy workloads)
off_heap_size = executor_memory * 1.5
per_task_memory = off_heap_size / task_slots
```

**Example**:
```
Executor: 48GB memory, 8 cores
Conservative: off-heap = 48GB * 0.6 = 28.8GB, per-task = 3.6GB
Aggressive: off-heap = 48GB * 1.5 = 72GB, per-task = 9GB
```

#### Adjust Based on Workload

| Workload Type | Heap:Off-Heap Ratio | Reasoning |
|---------------|-------------------|-----------|
| Native-heavy (scan, filter, project) | 1:2 or 1:3 | Native ops use off-heap primarily |
| Fallback-heavy (UDFs, complex expressions) | 2:1 or 3:1 | Fallback ops use heap |
| Mixed workload | 1:1 | Balanced allocation |
| Aggregation-heavy | 1:2 | Native hash aggregation uses off-heap |
| Join-heavy | 1:2 | Native hash join uses off-heap |

### Monitoring and Adjustment

#### Key Metrics to Monitor

1. **Memory Usage**:
   - Executor heap usage (via Spark UI)
   - Off-heap memory consumption (GC logs, native metrics)
   - Spilling frequency (stage metrics)

2. **Fallback Frequency**:
   - Number of operators falling back (Spark SQL UI)
   - Stages with resource profile changes (AQE logs)

3. **Performance Indicators**:
   - Task duration variance
   - GC time percentage
   - Spill size to disk

#### Iterative Tuning Process

1. **Start with conservative settings**
2. **Monitor for 2-3 production runs**
3. **Identify bottlenecks**:
   - Frequent OOM → Increase memory or enable isolation
   - High fallback ratio → Review query compatibility
   - Excessive spilling → Increase off-heap size
   - High GC time → Adjust heap/off-heap ratio
4. **Adjust one parameter at a time**
5. **Re-measure and validate**

### Workload-Specific Recommendations

#### TPCDS-like Analytics

```properties
spark.executor.memory=64g
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=80g
spark.gluten.memory.isolation=true
spark.gluten.memory.overAcquiredMemoryRatio=0.4
spark.gluten.sql.autoAdjustStageResourceProfile.enabled=true
```

**Why**: Large joins and aggregations benefit from high off-heap allocation.

#### ETL with UDFs

```properties
spark.executor.memory=48g
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=24g
spark.gluten.memory.isolation=true
spark.gluten.sql.autoAdjustStageResourceProfile.enabled=true
spark.gluten.sql.autoAdjustStageResourceProfile.heapRatio=1.5
```

**Why**: UDFs cause fallback, need more heap, less off-heap.

#### Streaming Workloads

```properties
spark.executor.memory=32g
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=40g
spark.gluten.memory.isolation=false
spark.gluten.memory.overAcquiredMemoryRatio=0.3
spark.gluten.memory.dynamic.offHeap.sizing.enabled=false
```

**Why**: Consistent throughput, avoid GC pauses from dynamic sizing.

---

## Troubleshooting

### Common OOM Scenarios

#### 1. Task-Level OOM During Native Execution

**Symptoms**:
- `OutOfMemoryException` in Gluten operators
- Error message: "Unable to acquire X bytes of memory"
- Task failures with memory-related errors

**Diagnostic Steps**:

1. Check task memory allocation:
```bash
# Look for memory target statistics in executor logs
grep "MemoryTarget" executor-logs.txt
grep "Unable to acquire" executor-logs.txt
```

2. Review memory configuration:
```scala
// In spark-submit or config
spark.memory.offHeap.size
spark.gluten.memory.isolation
```

3. Check spilling activity:
```bash
# Look for spilling logs
grep "Shrunk" executor-logs.txt
grep "Spilled" executor-logs.txt
```

**Solutions**:

**Option A**: Increase off-heap memory
```properties
spark.memory.offHeap.size=40g  # Increase from current value
```

**Option B**: Enable memory isolation (multi-slot executors)
```properties
spark.gluten.memory.isolation=true
```

**Option C**: Reduce task parallelism
```properties
spark.executor.cores=4  # Reduce from current value
# OR
spark.sql.shuffle.partitions=200  # Reduce partitions
```

**Option D**: Enable stage resource adjustment
```properties
spark.gluten.sql.autoAdjustStageResourceProfile.enabled=true
```

#### 2. Whole-Executor OOM (Multiple Tasks)

**Symptoms**:
- Executor lost due to OOM
- Container killed by YARN/K8s for exceeding memory
- Multiple concurrent tasks failing

**Diagnostic Steps**:

1. Check executor-level memory:
```bash
# Review executor memory settings
spark.executor.memory
spark.executor.memoryOverhead
spark.memory.offHeap.size

# Calculate total: executor.memory + memoryOverhead + offHeap.size
```

2. Check task concurrency:
```bash
# Number of concurrent tasks per executor
spark.executor.cores  # Each core = 1 task slot
```

3. Review native memory usage:
```bash
# In executor logs
grep "NativeMemoryManager" executor-logs.txt
grep "usedBytes" executor-logs.txt
```

**Solutions**:

**Option A**: Increase executor memory overhead
```properties
spark.executor.memoryOverhead=8g  # For off-heap allocations
# OR as fraction
spark.executor.memoryOverheadFactor=0.3  # 30% of executor memory
```

**Option B**: Enable unified memory management
```properties
spark.gluten.memory.dynamic.offHeap.sizing.enabled=true
spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction=0.9
```

**Option C**: Reduce executor parallelism
```properties
spark.executor.cores=4  # Reduce concurrent tasks
```

#### 3. OOM During Fallback Transitions

**Symptoms**:
- OOM occurs at ColumnarToRow or RowToColumnar operators
- Memory spikes during format conversion
- Works in pure native or pure Spark, fails in mixed mode

**Diagnostic Steps**:

1. Identify transition operators:
```sql
-- Check physical plan for ColumnarToRow/RowToColumnar
EXPLAIN EXTENDED your_query;
```

2. Check memory usage before/after transition:
```bash
grep "ColumnarToRow" executor-logs.txt
grep "RowToColumnar" executor-logs.txt
```

**Solutions**:

**Option A**: Enable stage resource adjustment
```properties
spark.gluten.sql.autoAdjustStageResourceProfile.enabled=true
spark.gluten.sql.autoAdjustStageResourceProfile.heapRatio=1.5
```

**Option B**: Increase reservation block size (reduces conversion overhead)
```properties
spark.gluten.memory.reservationBlockSize=16777216  # 16MB
```

**Option C**: Optimize batch size
```properties
spark.sql.execution.arrow.maxRecordsPerBatch=10000  # Reduce from default
```

#### 4. Memory Leaks

**Symptoms**:
- Memory usage continuously grows over time
- Executors crash after processing many batches
- Memory not released after task completion

**Diagnostic Steps**:

1. Enable verbose GC logging:
```properties
spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
```

2. Monitor native memory over time:
```bash
# Track native memory in executor logs
grep "usedBytes" executor-logs.txt | awk '{print $timestamp, $usedBytes}'
```

3. Check for unclosed iterators:
```bash
grep "close" executor-logs.txt
grep "cleanup" executor-logs.txt
```

**Solutions**:

**Option A**: Verify proper cleanup (code review)
- Ensure ColumnarBatch iterators are properly closed
- Check native resource cleanup in finally blocks

**Option B**: Enable aggressive resource cleanup
```scala
// In your Spark application
sparkContext.setLocalProperty("spark.gluten.sql.columnar.iterator.release", "true")
```

**Option C**: Restart executors periodically (workaround)
```properties
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.executorIdleTimeout=300s
```

### Diagnostic Commands and Logs

#### Memory Dump Analysis

**Capture memory statistics**:
```bash
# In Gluten logs, look for memory target dumps
grep "MemoryTarget Statistics" executor-logs.txt

# Expected output format:
# MemoryTarget Statistics:
#   TreeMemoryConsumer: used=5.2GB, limit=10GB
#   - Gluten.Runtime: used=3.1GB
#   - ArrowContextInstance: used=2.1GB
```

**Spark Memory Manager stats**:
```bash
# Look for TaskMemoryManager dumps
grep "showMemoryUsage" executor-logs.txt

# Expected output:
# Memory used: 8.5GB / 10GB execution, 1.2GB / 5GB storage
```

#### Enable Debug Logging

Add to `log4j.properties`:
```properties
# Gluten memory management
log4j.logger.org.apache.gluten.memory=DEBUG
log4j.logger.org.apache.gluten.memory.memtarget=DEBUG
log4j.logger.org.apache.gluten.memory.listener=DEBUG

# Spark memory
log4j.logger.org.apache.spark.memory=DEBUG

# Native memory manager
log4j.logger.org.apache.gluten.memory.NativeMemoryManager=DEBUG
```

#### Monitoring Commands

**Check current memory usage** (from driver):
```scala
// In spark-shell or application
spark.sparkContext.getExecutorMemoryStatus.foreach { case (execId, (maxMem, freeMem)) =>
  println(s"Executor $execId: ${(maxMem - freeMem) / 1024 / 1024 / 1024}GB used / ${maxMem / 1024 / 1024 / 1024}GB max")
}
```

**Query stage statistics** (with AQE):
```scala
// After query execution
val queryExecution = df.queryExecution
val adaptiveExecution = queryExecution.executedPlan
println(adaptiveExecution.treeString(verbose = true))
```

### Interpreting Memory Dumps

**Example OOM Error Message**:
```
OutOfMemoryException: Unable to acquire 536870912 bytes of memory, only 134217728 granted

MemoryTarget Statistics:
TreeMemoryConsumer (limit: 10737418240):
  used: 10603200512
  children:
    - Gluten.Runtime (used: 8589934592)
      - VeloxRuntime (used: 6442450944)
      - ArrowContext (used: 2147483648)
    - OverConsumer (used: 2013265920)

Spark TaskMemoryManager:
  Execution memory: 9.8GB used / 10GB limit
  Storage memory: 0.5GB used / 5GB limit
  Consumers: 1
```

**Interpretation**:
- Requested: 512MB (536870912 bytes)
- Granted: 128MB (134217728 bytes)
- Current usage: 9.87GB / 10GB (98.7% utilization)
- Largest consumer: VeloxRuntime (6GB)
- **Action**: Spill VeloxRuntime or increase memory limit

### Performance Issues Related to Memory

#### Excessive Spilling

**Symptoms**:
- High disk I/O on executor nodes
- Tasks taking much longer than expected
- Large spill sizes in stage metrics

**Diagnostic**:
```bash
# Check spill metrics in Spark UI
# Stage Details → Metrics → Spill (Memory) / Spill (Disk)

# Or in logs
grep "Spilled" executor-logs.txt
```

**Solutions**:
1. Increase off-heap memory allocation
2. Reduce task parallelism (fewer concurrent tasks)
3. Enable compression for spilling:
```properties
spark.gluten.sql.columnar.shuffle.codec=lz4
```

#### Excessive GC Time

**Symptoms**:
- High GC time percentage (>10%) in Spark UI
- Tasks spending more time in GC than execution
- "GC overhead limit exceeded" errors

**Diagnostic**:
```bash
# Review GC logs
grep "Full GC" gc.log
grep "GC pause" gc.log
```

**Solutions**:

**Option A**: Increase executor heap
```properties
spark.executor.memory=64g  # Increase from current
```

**Option B**: Tune GC settings
```properties
spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:InitiatingHeapOccupancyPercent=35
```

**Option C**: Shift to off-heap (reduce heap pressure)
```properties
spark.memory.offHeap.size=80g  # Increase off-heap
spark.executor.memory=32g  # Decrease heap
```

**Option D**: Disable dynamic off-heap sizing (if enabled)
```properties
spark.gluten.memory.dynamic.offHeap.sizing.enabled=false
```

#### Memory Imbalance Between Tasks

**Symptoms**:
- Some tasks OOM while others have free memory
- High variance in task duration
- Executors have free memory but tasks fail

**Diagnostic**:
```bash
# Check task metrics in Spark UI
# Look for variance in peak execution memory across tasks
```

**Solutions**:

**Option A**: Enable memory isolation
```properties
spark.gluten.memory.isolation=true
```

**Option B**: Improve data distribution
```properties
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB
```

**Option C**: Enable retry on OOM (automatic)
- Already enabled by default for multi-slot executors
- Triggers cross-task spilling

### Known Limitations and Workarounds

#### Limitation 1: No Within-Task Memory Reallocation

**Issue**: Memory is not released/reallocated during native → fallback transition within a task.

**Workaround**:
- Use stage-level resource adjustment (requires AQE)
- Structure queries to minimize mixed execution in same task

#### Limitation 2: Dynamic Off-Heap Sizing Requires Java 9+

**Issue**: JMX access to HotSpot diagnostics not available in Java 8.

**Workaround**:
- Upgrade to Java 11 or 17
- OR manually tune heap/off-heap ratio without dynamic sizing

#### Limitation 3: Stage Resource Adjustment Requires AQE

**Issue**: Auto-adjust only works with Adaptive Query Execution.

**Workaround**:
- Enable AQE (recommended anyway for performance)
- OR manually tune memory for expected fallback scenarios

#### Limitation 4: Block-Based Reservation Can Over-Allocate

**Issue**: 8MB block size may allocate more than needed for small operations.

**Workaround**:
- Adjust block size for workload:
```properties
spark.gluten.memory.reservationBlockSize=4194304  # 4MB for small operations
spark.gluten.memory.reservationBlockSize=16777216  # 16MB for large operations
```

---

## Code References

### Memory Management Core

- **MemoryTarget Interface**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/MemoryTarget.java`
- **TreeMemoryConsumer**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/spark/TreeMemoryConsumer.java`
- **GlobalOffHeapMemoryTarget**: `gluten-core/src/main/scala/org/apache/spark/memory/GlobalOffHeapMemoryTarget.scala`
- **TaskResources**: `gluten-core/src/main/scala/org/apache/spark/task/TaskResources.scala`

### Dynamic Memory Management

- **DynamicOffHeapSizingMemoryTarget**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/DynamicOffHeapSizingMemoryTarget.java`
- **OverAcquire**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/OverAcquire.java`
- **ThrowOnOomMemoryTarget**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/ThrowOnOomMemoryTarget.java`
- **RetryOnOomMemoryTarget**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/RetryOnOomMemoryTarget.java`

### Spilling

- **Spiller Interface**: `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/Spiller.java`
- **TreeMemoryTargets** (spilling algorithm): `gluten-core/src/main/java/org/apache/gluten/memory/memtarget/TreeMemoryTargets.java`
- **NativeMemoryManager** (native spiller): `gluten-arrow/src/main/scala/org/apache/gluten/memory/NativeMemoryManager.scala`

### Reservation and Allocation

- **ReservationListeners**: `gluten-arrow/src/main/java/org/apache/gluten/memory/listener/ReservationListeners.java`
- **ManagedReservationListener**: `gluten-arrow/src/main/java/org/apache/gluten/memory/listener/ManagedReservationListener.java`
- **ManagedAllocationListener**: `gluten-arrow/src/main/java/org/apache/gluten/memory/arrow/alloc/ManagedAllocationListener.java`

### Fallback Management

- **GlutenAutoAdjustStageResourceProfile**: `gluten-substrait/src/main/scala/org/apache/spark/sql/execution/GlutenAutoAdjustStageResourceProfile.scala`
- **FallbackTag**: `gluten-core/src/main/scala/org/apache/gluten/extension/columnar/FallbackTag.scala`
- **ValidationResult**: `gluten-substrait/src/main/scala/org/apache/gluten/execution/ValidationResult.scala`

### Transition Operators

- **ColumnarToRowExecBase**: `gluten-substrait/src/main/scala/org/apache/gluten/execution/ColumnarToRowExecBase.scala`
- **VeloxColumnarToRowExec**: `backends-velox/src/main/scala/org/apache/gluten/execution/VeloxColumnarToRowExec.scala`
- **RowToArrowColumnarExec**: `gluten-substrait/src/main/scala/org/apache/gluten/execution/RowToArrowColumnarExec.scala`

---

## Summary

Gluten's memory management system provides:

1. **Unified Memory Pool**: Optional dynamic balancing between heap and off-heap via JVM shrinking
2. **Hierarchical Tracking**: Tree structure mirrors task memory organization and integrates with Spark
3. **Automatic Spilling**: Two-phase (SHRINK → SPILL) with priority-based selection targeting largest consumers
4. **Proactive Over-Acquisition**: Safety buffer for non-spillable operations
5. **Memory Isolation**: Per-task limits for concurrent execution in multi-slot executors
6. **Retry Mechanisms**: Cross-consumer spilling for multi-slot executors
7. **Graceful Fallback**: Validation and fallback to Spark when OOM occurs or operators unsupported

**Memory Adjustment During Fallback**:
- ✅ **Stage-level adjustment** (between stages via AQE)
- ✅ **Runtime heap/off-heap balancing** (experimental unified memory)
- ❌ **No within-task reallocation** (task-scoped lifecycle)

The system integrates deeply with Spark's memory manager while adding sophisticated features for native execution, ensuring efficient memory utilization and robust OOM handling.

For production deployments, start with conservative settings (memory isolation + stage adjustment) and iteratively tune based on workload characteristics and monitoring data.
