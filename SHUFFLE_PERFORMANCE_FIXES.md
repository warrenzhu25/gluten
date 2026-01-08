# Shuffle Performance Fixes: Understanding the Cherry-Picked Commits

## Overview

This document explains how specific commits cherry-picked into this branch improve Spark shuffle performance by addressing two critical bottlenecks:

1. **High deserialization time** during shuffle read operations
2. **Large shuffle file sizes** during shuffle write operations

These fixes are particularly impactful for shuffle-heavy workloads like joins and aggregations.

**Target Audience:** Engineers familiar with Spark shuffle mechanics but not necessarily Gluten internals.

---

## Background: Spark Shuffle in Gluten

In Gluten, shuffle operations are offloaded to native execution engines (Velox). This means:
- **Shuffle write**: Serialization and compression happen in native code (C++)
- **Shuffle read**: Deserialization happens in native code (C++)
- **JNI boundary**: Data crosses between JVM (Spark) and native code

The commits described below optimize these native shuffle operations.

---

## Part 1: Fixing High Deserialization Time (Shuffle Read)

### The Problem

When Spark performs a shuffle read, it fetches shuffle blocks from multiple map tasks. In a typical join or aggregation, a single reduce task might read hundreds or thousands of small shuffle blocks.

**Two inefficiencies existed:**

1. **Excessive JNI overhead**: Each shuffle block required a separate JNI call to open a stream, deserialize data, and close the stream. With hundreds of blocks, this created significant overhead.

2. **Unnecessary double-buffering**: The code used Arrow's `BufferedInputStream` on top of the OS page cache, causing extra memory copies with no benefit for sequential reads.

### Solution 1: GLUTEN-10214 - Merge Input Streams for Shuffle Reader

**Commit:** `1b0d74eb4` (Sep 2025)
**PR:** [#10499](https://github.com/apache/incubator-gluten/pull/10499)

**What Changed:**

Introduced a `StreamReader` abstraction that collects all input streams upfront and provides them to the native deserializer on-demand.

**Before (Per-Block Processing):**
```
For each of 100 shuffle blocks:
  1. JNI call to open stream
  2. Deserialize batches from this stream
  3. Close stream
  4. Return to JVM
  = 200+ JNI boundary crossings (open + close for each block)
```

**After (Merged Stream Processing):**
```
1. Single JNI call to setup StreamReader with all 100 blocks
2. Native code processes all streams continuously
3. Deserialize batches without returning to JVM
  = 1 JNI crossing
```

**Why This Improves Performance:**

- **Amortized JNI overhead**: Instead of paying JNI overhead per shuffle block, we pay it once for all blocks
- **Better CPU caching**: Continuous processing in native code keeps instructions and data in CPU cache
- **Reduced context switching**: The OS doesn't thrash between JVM and native code

**Real-World Impact:**
The improvement is most significant when reading many small shuffle blocks, which is common in wide transformations (joins across many partitions, large aggregations).

---

### Solution 2: GLUTEN-10920 - Allow Disabling Shuffle Reader Buffer

**Commit:** `99f4bb157` (Oct 2025)
**PR:** [#10922](https://github.com/apache/incubator-gluten/pull/10922)

**What Changed:**

Made Arrow's `BufferedInputStream` optional via configuration. The default is now to use direct streams without extra buffering.

**The Double-Buffering Problem:**

```
WITH BufferedInputStream (old default):
Disk → OS Page Cache → Arrow Buffer (1MB) → Deserializer
     (kernel copy)       (userspace copy)

WITHOUT BufferedInputStream (new default):
Disk → OS Page Cache → Deserializer
     (kernel copy only)
```

Arrow's `BufferedInputStream` adds a 1MB user-space buffer on top of the OS's already-buffered file I/O. For shuffle reads (which are typically sequential), the OS page cache already provides excellent buffering, making the extra layer pure overhead.

**Why This Improves Performance:**

- **Eliminates redundant memory copy**: One copy instead of two
- **Saves memory bandwidth**: For 500 MB/s of shuffle reads, this saves 500 MB/s of memory bandwidth
- **Reduces CPU overhead**: Less time spent in buffer management code

**When You Might Re-Enable Buffering:**

The `BufferedInputStream` is only beneficial for very random (non-sequential) reads where the OS page cache is ineffective. For typical shuffle workloads, direct reads are faster.

**Configuration:**
```properties
# Disable buffering (default, recommended)
spark.gluten.sql.columnar.shuffle.readerBufferSize=0

# Enable buffering (only if profiling shows benefit)
spark.gluten.sql.columnar.shuffle.readerBufferSize=1048576  # 1MB
```

---

## Part 2: Reducing Shuffle File Size (Shuffle Write)

### The Problem

Large shuffle files cause multiple issues:
- Higher disk I/O and network transfer costs
- Slower shuffle write times
- Storage pressure on executors

**Three inefficiencies existed:**

1. **Block-based compression**: Data was divided into fixed chunks, and each chunk was compressed independently, losing compression opportunities across chunk boundaries

2. **Wrong buffer size for disk I/O**: A single configuration controlled both compression buffers and disk write buffers, leading to suboptimal sizing (too small for efficient disk writes)

3. **A correctness bug**: Fixed-width columns could trigger segfaults in the compression code

### Solution 1: GLUTEN-9163 Part 1 - Stream-Based Compression

**Commit:** `69565e72c` (Apr 2025)
**PR:** [#9278](https://github.com/apache/incubator-gluten/pull/9278)

**What Changed:**

Switched from block-based to stream-based compression for shuffle data.

**Block-Based Compression (Old Approach):**
```
1. Collect 64KB of data
2. Compress this 64KB block (fresh compressor state)
3. Write compressed block
4. Repeat for next 64KB

Problem: The compressor for block N cannot reference patterns
seen in block N-1. Compression restart overhead on each block.
```

**Stream-Based Compression (New Approach):**
```
1. Stream data continuously to compressor
2. Compressor maintains state (dictionary) across all data
3. Flush compressed data periodically
4. Better compression ratios

Benefit: The compressor can reference patterns from earlier
in the stream, and there's no restart overhead.
```

**Why This Improves Performance:**

**Smaller shuffle files:**
- For repeated strings (like "United States", "New York", etc.), the compressor builds a dictionary once and references it throughout the stream
- For time-series data, sequential patterns are exploited across the full dataset
- Better compression ratios, especially for data with repeated patterns

**Example:**
If you're shuffling user data with country codes, the old approach would re-encode "United States" in every 64KB block. The new approach encodes it once in the compressor's dictionary and uses cheap references thereafter.

---

### Solution 2: GLUTEN-9163 Part 2 - Separate Compression and Disk Write Buffers

**Commit:** `f91a423c7` (Apr 2025)
**PR:** [#9356](https://github.com/apache/incubator-gluten/pull/9356)

**What Changed:**

Split a single configuration into two separate configurations with appropriate defaults.

**The Problem:**

Before this fix, `sortEvictBufferSize` controlled BOTH:
- Compression buffer size (optimal: ~32KB for LZ4/ZSTD)
- Disk write buffer size (optimal: ~1MB for sequential I/O efficiency)

This created a conflict: You can't choose a value that's optimal for both.

**The Fix:**

```properties
# Compression buffer - optimal for compression algorithms
spark.gluten.sql.columnar.shuffle.compressionBufferSize=32KB

# Disk write buffer - optimal for reducing syscall overhead
spark.shuffle.spill.diskWriteBufferSize=1MB  # Matches Spark default
```

**Why This Improves Performance:**

**Better compression:**
- LZ4 uses 64KB block size internally; 32KB aligns well
- ZSTD works best with 32-128KB windows for shuffle workloads

**Faster disk writes:**
- Writing 1GB with a 32KB buffer = 32,768 write() system calls
- Writing 1GB with a 1MB buffer = 1,024 write() system calls
- Each syscall has ~5µs overhead
- Result: ~30x fewer syscalls, significantly less overhead

**Why These Sizes:**
- **32KB for compression**: Matches compression algorithm internals without wasting memory
- **1MB for disk I/O**: Matches Spark's default and is efficient for sequential writes

---

### Solution 3: GLUTEN-9163 Part 3 - Fix Segfault with Fixed-Width Inputs

**Commit:** `31d53c930` (May 2025)
**PR:** [#9766](https://github.com/apache/incubator-gluten/pull/9766)

**What Changed:**

Fixed a bug in the shuffle writer that caused segmentation faults when processing fixed-width columns (like `IntegerType`, `LongType`, `DoubleType`).

**The Bug:**

```cpp
// BEFORE (incorrect):
rowSize_.resize(inputRows, fixedRowSize_.value() + sizeof(RowSizeType));

// AFTER (correct):
rowSize_.resize(inputRows, fixedRowSize_.value());
```

The code was adding extra bytes to the calculated row size, causing buffer overruns when reading fixed-width data.

**Impact:**

This is a **correctness fix**, not a performance optimization. However, it's required for the stream compression changes to work reliably with all Spark data types.

---

## Configuration Guide

After cherry-picking all commits, these are the key configurations:

### Shuffle Read Configuration

```properties
# Disable BufferedInputStream for best performance (default)
spark.gluten.sql.columnar.shuffle.readerBufferSize=0
```

**When to change:** Only if profiling shows your workload benefits from buffering (very rare).

### Shuffle Write Configuration

```properties
# Compression buffer size (default is optimal for most workloads)
spark.gluten.sql.columnar.shuffle.compressionBufferSize=32768  # 32KB

# Disk write buffer size (uses Spark's default)
spark.shuffle.spill.diskWriteBufferSize=1048576  # 1MB
```

**When to change:**
- **Large blocks, lots of memory**: Increase `compressionBufferSize` to 64KB
- **Memory constrained**: Decrease `compressionBufferSize` to 16KB
- **Very fast SSDs**: Increase `diskWriteBufferSize` to 2MB

---

## How to Verify These Fixes

### 1. Verify Configuration

Check that the fixes are properly configured:

```bash
# Check Spark configuration
grep "readerBufferSize\|compressionBufferSize\|diskWriteBufferSize" conf/spark-defaults.conf

# Expected output:
# spark.gluten.sql.columnar.shuffle.readerBufferSize=0
# spark.gluten.sql.columnar.shuffle.compressionBufferSize=32768
# spark.shuffle.spill.diskWriteBufferSize=1048576
```

### 2. Monitor Spark UI Metrics

Run a shuffle-heavy query (join or aggregation) and check Spark UI:

**For Shuffle Read Improvements:**
- Navigate to: Spark UI → Stages → Stage Details
- Look at: "Shuffle Read Time" and "Deserialization Time"
- Expected: Lower deserialization time relative to total shuffle read time

**For Shuffle Write Improvements:**
- Navigate to: Spark UI → Stages → Stage Details
- Look at: "Shuffle Write Size" and "Shuffle Write Time"
- Expected: Smaller shuffle files (compressed size)

### 3. Disk Usage

Monitor disk usage during shuffle:

```bash
# Watch shuffle directory size during query execution
watch -n 1 du -sh /path/to/spark/shuffle/*
```

Expected: Smaller shuffle files on disk compared to previous runs with the same data.

---

## Expected Impact

### When These Fixes Help Most

**Shuffle Read (Deserialization) Fixes:**
- ✅ Queries with many small shuffle blocks (wide joins, large aggregations)
- ✅ Shuffle-heavy workloads (TPC-DS queries with multiple joins)
- ✅ Workloads where shuffle read is a significant bottleneck

**Shuffle Write (Compression) Fixes:**
- ✅ Large shuffle writes (aggregations, repartitions)
- ✅ Compressible data (strings, categorical columns, repeated values)
- ✅ Sort-based shuffle operations
- ✅ Storage or network-constrained environments

### General Expectations

These fixes address fundamental inefficiencies in shuffle operations. For shuffle-heavy workloads:

- **Shuffle read operations** should show reduced deserialization overhead
- **Shuffle write operations** should produce smaller files more quickly
- **Overall query performance** improves when shuffle is a significant portion of query time

The exact improvement depends on your specific workload characteristics, but the fixes eliminate clear inefficiencies that affect most Spark workloads.

---

## Troubleshooting

### Issue: No deserialization improvement observed

**Possible causes:**
1. Configuration not applied correctly
2. Query is not shuffle-heavy (shuffle is not the bottleneck)
3. Different bottleneck (CPU, I/O, network)

**Debug steps:**
```bash
# Verify configuration is applied
grep "readerBufferSize" conf/spark-defaults.conf

# Check Spark UI to confirm shuffle is significant portion of query time
# If shuffle read time is <10% of total time, improvements won't be noticeable
```

### Issue: No shuffle file size reduction

**Possible causes:**
1. Data is not compressible (random binary data)
2. Compression is disabled
3. Using hash shuffle instead of sort shuffle

**Debug steps:**
```bash
# Verify compression is enabled
grep "spark.shuffle.compress" conf/spark-defaults.conf
# Should be: spark.shuffle.compress=true

# Verify using sort-based shuffle
# Sort shuffle benefits most from stream compression
```

### Issue: Segfaults after cherry-picking

**Possible cause:**
Missing the segfault fix commit (31d53c930)

**Resolution:**
```bash
# Ensure all three GLUTEN-9163 commits are cherry-picked
git log --oneline | grep "GLUTEN-9163"

# Should see all three:
# 31d53c930 [GLUTEN-9163][VL][FOLLOWUP] Fix segfault triggered by fixed-width inputs
# f91a423c7 [GLUTEN-9163][VL] Separate compression buffer and disk write buffer configuration
# 69565e72c [GLUTEN-9163][VL] Use stream de/compressor in sort-based shuffle
```

---

## Summary

### What These Commits Fix

| Commit | Problem | Solution | Benefit |
|--------|---------|----------|---------|
| **GLUTEN-10214** | JNI overhead per shuffle block | Merge streams, process continuously | Reduced deserialization overhead |
| **GLUTEN-10920** | Double-buffering overhead | Direct reads, no extra buffer | Faster shuffle reads |
| **GLUTEN-9163 (Part 1)** | Block-based compression | Stream compression | Smaller shuffle files |
| **GLUTEN-9163 (Part 2)** | Wrong buffer sizes | Separate compression/disk buffers | Faster writes, better compression |
| **GLUTEN-9163 (Part 3)** | Segfault bug | Fix size calculation | Correctness |

### Recommendations

1. ✅ **Apply all 5 commits** for maximum benefit
2. ✅ **Keep default configuration** (`readerBufferSize=0`)
3. ✅ **Test on representative workload** to understand impact
4. ✅ **Monitor Spark UI metrics** to verify improvements
5. ✅ **Profile before tuning** buffer sizes (defaults are good for most cases)

---

## References

### Upstream Issues and Pull Requests

- [GLUTEN-10214](https://github.com/apache/incubator-gluten/pull/10499) - Merge inputstream for shuffle reader
- [GLUTEN-10920](https://github.com/apache/incubator-gluten/pull/10922) - Allow disabling shuffle reader buffer
- [GLUTEN-9163](https://github.com/apache/incubator-gluten/issues/9163) - Shuffle write performance improvements
  - [PR #9278](https://github.com/apache/incubator-gluten/pull/9278) - Stream de/compressor
  - [PR #9356](https://github.com/apache/incubator-gluten/pull/9356) - Separate buffer configs
  - [PR #9766](https://github.com/apache/incubator-gluten/pull/9766) - Fix segfault

### Related Documentation

- [Gluten Configuration Guide](https://github.com/apache/incubator-gluten/blob/main/docs/Configuration.md)
- [Apache Arrow Documentation](https://arrow.apache.org/docs/cpp/api/io.html)
- [Spark Shuffle Internals](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations)
