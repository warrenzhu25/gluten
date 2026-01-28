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

## Part 2: Reducing Shuffle Write Time (and File Size)

### The Problem

Shuffle write was slow due to inefficiencies in both compression and I/O:

1. **Block-based compression overhead**: Each fixed-size chunk (64KB) was compressed independently. The compressor was created/destroyed per block, incurring CPU overhead for initialization and losing cross-block compression state (dictionary). This meant more CPU time compressing and larger output (more bytes to write).

2. **Wrong buffer size for disk I/O**: A single configuration (`sortEvictBufferSize`) controlled both the compression buffer and the disk write buffer. Since compression works best with small buffers (~32KB) and disk I/O works best with large buffers (~1MB), no single value was optimal. In practice, the buffer was too small for efficient disk writes, causing excessive `write()` syscalls.

3. **A correctness bug**: Fixed-width columns could trigger segfaults in the compression code.

**Why these issues compound:** In sort-based shuffle, rows are sorted by partition then written sequentially. With block compression, the compressor couldn't exploit the natural data locality within a partition. With undersized disk buffers, each small compressed block triggered its own syscall. The result was both more CPU time (compression) and more kernel time (syscalls) than necessary.

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

**Why This Reduces Write Time:**

Write time drops for two reasons:

1. **Less CPU time on compression**: The old approach created and destroyed a compressor for every 64KB block. Each creation reinitializes internal state (hash tables, dictionaries). With stream compression, the compressor is created once per partition and reused across all data. Since sort-based shuffle groups rows by partition, the compressor's dictionary accumulates patterns (repeated strings, similar numeric ranges) that make subsequent compression faster and produce smaller output.

2. **Less data to write to disk**: Better compression ratios mean fewer bytes hit the disk. For data with repeated patterns (strings, categorical columns), stream compression can achieve significantly better ratios since the dictionary carries across the entire partition rather than resetting every 64KB.

**Example:**
If you're shuffling user data with country codes, the old approach would re-encode "United States" in every 64KB block. The new approach encodes it once in the compressor's dictionary and uses cheap references thereafter — less CPU work and smaller output.

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

**Why This Reduces Write Time:**

The key insight is that the old single buffer forced a tradeoff between compression efficiency and disk I/O efficiency. Separating them allows each to use its optimal size:

**Fewer syscalls with 1MB disk write buffer:**
- Writing 1GB with a 32KB buffer = 32,768 `write()` syscalls (~164ms of kernel overhead)
- Writing 1GB with a 1MB buffer = 1,024 `write()` syscalls (~5ms of kernel overhead)
- This is a 32x reduction in syscall overhead

**Better compression with 32KB compression buffer:**
- LZ4 uses 64KB block size internally; 32KB aligns well
- ZSTD works best with 32-128KB windows for shuffle workloads
- Smaller compression buffer means the stream compressor flushes more frequently, keeping memory usage low without sacrificing compression quality

**Combined effect on write time:** The sort shuffle writer (`VeloxSortShuffleWriter`) copies sorted rows into the 1MB `diskWriteBuffer`, then feeds them through the `ShuffleCompressedOutputStream` (which uses its own 32KB internal buffer). The result is efficient compression with infrequent disk writes — both the CPU path and the I/O path are optimized independently.

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

## Shuffle Write Pipeline (After Fixes)

The following diagram shows how data flows through the optimized write path:

```
VeloxSortShuffleWriter
  |
  | 1. Sort all rows by partition ID (radix sort)
  |    [PID 0, row A][PID 0, row B][PID 1, row C][PID 1, row D]...
  v
  | 2. For each partition, copy rows into diskWriteBuffer (1MB)
  |    - Batches rows to reduce downstream write frequency
  |    - Flushes buffer when full via evictPartitionInternal()
  v
LocalPartitionWriter::LocalSpiller
  |
  | 3. ShuffleCompressedOutputStream (stream compressor, 32KB internal buffer)
  |    - Compressor state persists across all rows in a partition
  |    - Dictionary builds up: repeated values get cheap back-references
  |    - On partition change: Flush() finalizes compression, resets compressor
  v
  | 4. write() to spill file (already batched by 1MB buffer above)
  v
Disk
```

**Before the fixes:** Each 64KB block got a fresh compressor (losing dictionary state), and the same undersized buffer served both compression and disk I/O. Now each concern has its own optimally-sized buffer, and the compressor maintains state across the full partition.

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

## Minimal Change Guide

This section identifies the 3 key performance changes, the exact files and code paths involved, and how to apply them independently with minimal risk.

### Overview: Risk vs. Benefit

| # | Change | Risk | Benefit | Independent? |
|---|--------|------|---------|-------------|
| 1 | Stream compression (write) | Medium — new class, changes spill path | Smaller shuffle files + faster writes | Yes |
| 2 | Buffer separation (write) | Low — config split only | 32x fewer `write()` syscalls | Yes |
| 3 | Disable reader buffer (read) | Very low — 16-line change | Eliminates redundant 1MB copy | Yes |

All three changes can be applied independently. Change 2 builds on the code from Change 1 but works without it (it just splits the config). The segfault fix (commit `31d53c930`) is required if you apply Change 1.

---

### Change 1: Stream Compression Instead of Block Compression

**Files to modify:**
- `cpp/core/shuffle/Utils.h` — add `ShuffleCompressedOutputStream` class
- `cpp/core/shuffle/LocalPartitionWriter.cc` — `LocalSpiller` uses `compressedOs_` instead of `toBlockPayload`
- `cpp/core/shuffle/Payload.cc` — `InMemoryPayload::serialize()` writes raw buffers to output stream

**Old code path (sort shuffle write):**
```
VeloxSortShuffleWriter::evictPartitionInternal()
  → InMemoryPayload::toBlockPayload(kCompressed, codec)  // block compress each buffer
    → BlockPayload::fromBuffers() → codec->Compress() per buffer
  → LocalSpiller::spill(BlockPayload)
    → BlockPayload::serialize(os)  // write compressed blocks
```

**New code path:**
```
VeloxSortShuffleWriter::evictPartitionInternal()
  → LocalSpiller::spill(InMemoryPayload)
    → InMemoryPayload::serialize(compressedOs)  // raw write into stream compressor
      → ShuffleCompressedOutputStream::Write()  // compressor maintains dictionary
```

**Key code pattern — `ShuffleCompressedOutputStream` (new class in `Utils.h`):**
```cpp
// Wraps an output stream with a stateful stream compressor.
// Created once per partition via codec->MakeCompressor().
// The compressor's dictionary persists across all data in the partition,
// achieving better ratios than per-block codec->Compress().
class ShuffleCompressedOutputStream {
  // Write() feeds raw data to the stream compressor
  // Flush() finalizes the compressed stream on partition boundary
};
```

**Key code pattern — `LocalSpiller` (in `LocalPartitionWriter.cc`):**
```cpp
// OLD: spill took BlockPayload (already compressed per-block)
void LocalSpiller::spill(std::unique_ptr<BlockPayload> payload);

// NEW: spill takes InMemoryPayload, writes through compressedOs_
void LocalSpiller::spill(std::unique_ptr<InMemoryPayload> payload);
// Inside: payload->serialize(compressedOs_) writes raw buffers
// compressedOs_ handles compression transparently
```

**Key code pattern — `InMemoryPayload::serialize()` (in `Payload.cc`):**
```cpp
// OLD: serialize() was not used for compressed writes (returned error)
// NEW: serialize(os) writes raw buffers to the output stream
//      The stream itself handles compression when wrapped with
//      ShuffleCompressedOutputStream
```

**Required companion fix:** Commit `31d53c930` fixes a segfault with fixed-width inputs — apply it whenever you apply this change.

---

### Change 2: Separate Disk Write Buffer from Compression Buffer

**Files to modify:**
- `cpp/core/shuffle/Options.h` — add `diskWriteBufferSize` (1MB) and `compressionBufferSize` (32KB) fields, replacing `sortEvictBufferSize`
- `cpp/velox/shuffle/VeloxSortShuffleWriter.cc` — use `diskWriteBufferSize` for the sorted buffer, remove `compressionBuffer_`

**Key code pattern — `Options.h`:**
```cpp
// OLD: single buffer for both
int64_t sortEvictBufferSize;

// NEW: separate buffers
int64_t diskWriteBufferSize = 1048576;      // 1MB — for write() syscalls
int64_t compressionBufferSize = 32768;       // 32KB — for compressor input
```

**Key code pattern — `VeloxSortShuffleWriter.cc`:**
```cpp
// OLD: used sortEvictBufferSize for the sorted data buffer
// and maintained a separate compressionBuffer_ for compression

// NEW: uses diskWriteBufferSize (1MB) for the sorted data buffer
// compressionBuffer_ removed — compression buffering is handled
// internally by ShuffleCompressedOutputStream (32KB)
```

**Why 1MB matters:** Writing 1GB with 32KB buffer = 32,768 syscalls. With 1MB buffer = 1,024 syscalls. That's 32x fewer kernel crossings.

---

### Change 3: Disable Reader Buffer (Double-Buffering)

**Files to modify:**
- `cpp/velox/shuffle/VeloxShuffleReader.cc` — conditionally skip `BufferedInputStream` wrapper

**Key code pattern:**
```cpp
// OLD: always wrapped with BufferedInputStream
auto bufferedStream = arrow::io::BufferedInputStream::Create(
    bufferSize, pool, std::move(inputStream));

// NEW: conditional based on readerBufferSize config
if (readerBufferSize > 0) {
  // wrap with BufferedInputStream (opt-in)
  stream = arrow::io::BufferedInputStream::Create(...);
} else {
  // use raw stream directly (default)
  stream = std::move(inputStream);
}
```

**Why this helps:** The OS page cache already buffers sequential reads. The extra 1MB `BufferedInputStream` just adds a redundant memcpy. Disabling it eliminates one full copy of all shuffle data.

**Config:** `spark.gluten.sql.columnar.shuffle.readerBufferSize=0` (default after this change).

---

### Recommended Application Order

1. **Change 3 (disable reader buffer)** — Very low risk, immediate benefit, single file
2. **Change 2 (buffer separation)** — Low risk, config-only change, two files
3. **Change 1 (stream compression)** — Medium risk, largest benefit, three files + segfault fix

Apply Change 3 first to get an easy win. Change 2 is straightforward config splitting. Change 1 delivers the biggest improvement but touches the most code — test thoroughly with representative workloads.

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
