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
package org.apache.gluten.memory.memtarget;

import org.apache.gluten.config.GlutenConfig;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.util.SparkThreadPoolUtil;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * The memory target used by dynamic off-heap sizing. Since
 * https://github.com/apache/incubator-gluten/issues/5439.
 */
@Experimental
public class DynamicOffHeapSizingMemoryTarget implements MemoryTarget {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicOffHeapSizingMemoryTarget.class);
  private static final AtomicLong USED_OFF_HEAP_BYTES = new AtomicLong();

  // This is to enforce process level maximum. Spark's Unified Memory Manager will enforce
  // per task constraints.
  private static final long TOTAL_MEMORY_SHARED = Runtime.getRuntime().maxMemory();

  private static final double ASYNC_TOTAL_MEMORY_THRESHOLD_RATIO;
  private static final double ASYNC_ON_HEAP_MEMORY_THRESHOLD_RATIO;
  private static final double GC_MAX_HEAP_FREE_RATIO;
  private static final int MAX_GC_RETRY;
  private static final long INITIAL_GC_RETRY_WAIT_TIME;
  private static final long GC_MAX_WAIT_TIME;

  // Test only.
  private static final AtomicLong TOTAL_EXPLICIT_GC_COUNT = new AtomicLong(0L);

  private static final int ORIGINAL_MAX_HEAP_FREE_RATIO;
  private static final int ORIGINAL_MIN_HEAP_FREE_RATIO;

  private final MemoryTarget delegated;

  public MemoryTarget delegated() {
    return delegated;
  }

  // Stores the current state of the process
  public static class MemoryState {
    public final long totalOnHeapMemory;
    public final long totalOffHeapMemory;
    public final long freeOnHeapMemory;
    public final long maximumMemory;
    public final long usedOnHeapMemory;

    private MemoryState(
        long totalOnHeapMemory,
        long totalOffHeapMemory,
        long freeOnHeapMemory,
        long maximumMemory) {
      this.totalOnHeapMemory = totalOnHeapMemory;
      this.totalOffHeapMemory = totalOffHeapMemory;
      this.freeOnHeapMemory = freeOnHeapMemory;
      this.maximumMemory = maximumMemory;
      this.usedOnHeapMemory = totalOnHeapMemory - freeOnHeapMemory;
    }

    private MemoryState() {
      this(
          Runtime.getRuntime().totalMemory(),
          USED_OFF_HEAP_BYTES.get(),
          Runtime.getRuntime().freeMemory(),
          TOTAL_MEMORY_SHARED);
    }

    public static MemoryState captureCurrentState() {
      return new MemoryState();
    }

    boolean totalWithRequestExceedAsyncThreshold(long requestedSize) {
      return requestedSize + totalOffHeapMemory + totalOnHeapMemory
          >= TOTAL_MEMORY_SHARED * ASYNC_TOTAL_MEMORY_THRESHOLD_RATIO;
    }

    private boolean totalWithRequestExceedMaxThreshold(long requestedSize) {
      return requestedSize + totalOffHeapMemory + totalOnHeapMemory >= TOTAL_MEMORY_SHARED;
    }

    boolean totalOnHeapExceedsAsyncThreshold() {
      return totalOnHeapMemory > TOTAL_MEMORY_SHARED * ASYNC_ON_HEAP_MEMORY_THRESHOLD_RATIO;
    }

    boolean freeOnHeapExceedsMinHeapThreshold() {
      return freeOnHeapMemory > totalOnHeapMemory * (ORIGINAL_MIN_HEAP_FREE_RATIO / 100.0);
    }

    // This checks if GC_MAX_HEAP_FREE_RATIO percentage of the totalOnHeapMemory can be reclaimed.
    // If less than GC_MAX_HEAP_FREE_RATIO can be reclaimed don't waste time shrinking.
    public boolean canShrinkJVMMemory() {
      // Check if the JVM memory can be shrunk by a full GC.
      return freeOnHeapMemory > totalOnHeapMemory * GC_MAX_HEAP_FREE_RATIO;
    }

    private boolean shouldTriggerAsyncOnHeapMemoryShrink(long requestedSize) {
      // Check if addition of requestedSize causes (totalOnHeapMemory + totalOffHeapMemory) to cross
      // ASYNC_GC_MAX_TOTAL_MEMORY_USAGE_RATIO percentage of TOTAL_MEMORY_SHARED
      return totalWithRequestExceedAsyncThreshold(requestedSize)
          // Ensure totalOnHeapMemory usage is at least ASYNC_GC_MAX_ON_HEAP_MEMORY_RATIO of
          // TOTAL_MEMORY_SHARED.
          && totalOnHeapExceedsAsyncThreshold()
          // If there is more freeOnHeapMemory than ORIGINAL_MIN_HEAP_FREE_RATIO of
          // totalOnHeapMemory,
          // JVM is keeping too much free memory, reclaim it.
          && freeOnHeapExceedsMinHeapThreshold();
    }

    @Override
    public String toString() {
      return String.format(
          "Total Allocated On Heap Memory: %s, "
              + "Total Allocated Off Heap Memory: %s, "
              + "Free On Heap Memory: %s, "
              + "Used On Heap Memory: %s, "
              + "Maximum Shared Memory: %s",
          Utils.bytesToString(totalOnHeapMemory),
          Utils.bytesToString(totalOffHeapMemory),
          Utils.bytesToString(freeOnHeapMemory),
          Utils.bytesToString(usedOnHeapMemory),
          Utils.bytesToString(maximumMemory));
    }
  }

  static {
    // Parse JVM args
    RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    List<String> jvmArgs = runtimeMxBean.getInputArguments();
    int originalMaxHeapFreeRatio = 70;
    int originalMinHeapFreeRatio = 40;
    for (String arg : jvmArgs) {
      if (arg.startsWith("-XX:MaxHeapFreeRatio=")) {
        String valuePart = arg.substring(arg.indexOf('=') + 1);
        try {
          originalMaxHeapFreeRatio = Integer.parseInt(valuePart);
        } catch (NumberFormatException e) {
          LOG.warn(
              "Failed to parse MaxHeapFreeRatio from JVM argument: {}. Using default value: {}.",
              arg,
              originalMaxHeapFreeRatio);
        }
      } else if (arg.startsWith("-XX:MinHeapFreeRatio=")) {
        String valuePart = arg.substring(arg.indexOf('=') + 1);
        try {
          originalMinHeapFreeRatio = Integer.parseInt(valuePart);
        } catch (NumberFormatException e) {
          LOG.warn(
              "Failed to parse MinHeapFreeRatio from JVM argument: {}. Using default value: {}.",
              arg,
              originalMinHeapFreeRatio);
        }
      } else if (arg.startsWith("-XX:+ExplicitGCInvokesConcurrent")) {
        // If this is set -XX:+ExplicitGCInvokesConcurrent, System.gc() does not trigger Full GC,
        // so explicit JVM shrinking is not effective.
        LOG.error(
            "Explicit JVM shrinking is not effective because -XX:+ExplicitGCInvokesConcurrent"
                + " is set. Please check the JVM arguments: {}. ",
            arg);

      } else if (arg.startsWith("-XX:+DisableExplicitGC")) {
        // If -XX:+DisableExplicitGC is set, calls to System.gc() are ignored,
        // so explicit JVM shrinking will not work as intended.
        LOG.error(
            "Explicit JVM shrinking is disabled because -XX:+DisableExplicitGC is set. "
                + "System.gc() calls will be ignored and JVM shrinking will not work. "
                + "Please check the JVM arguments: {}. ",
            arg);
      }
    }
    ORIGINAL_MIN_HEAP_FREE_RATIO = originalMinHeapFreeRatio;
    ORIGINAL_MAX_HEAP_FREE_RATIO = originalMaxHeapFreeRatio;

    if (!isJava9OrLater()) {
      // For JDK 8, we cannot change MaxHeapFreeRatio programmatically at runtime.
      LOG.error("Dynamic off-heap sizing is not supported before JDK 9.");
    }

    GlutenConfig conf = GlutenConfig.get();

    ASYNC_TOTAL_MEMORY_THRESHOLD_RATIO = conf.dynamicOffHeapSizingAsyncTotalMemoryThresholdRatio();
    ASYNC_ON_HEAP_MEMORY_THRESHOLD_RATIO =
        conf.dynamicOffHeapSizingAsyncOnHeapMemoryThresholdRatio();
    GC_MAX_HEAP_FREE_RATIO = conf.dynamicOffHeapSizingGCHeapFreeRatio();
    MAX_GC_RETRY = conf.dynamicOffHeapSizingMaxGCRetry();
    INITIAL_GC_RETRY_WAIT_TIME = conf.dynamicOffHeapSizingInitialGCWaitTime();
    GC_MAX_WAIT_TIME = conf.dynamicOffHeapSizingGCMaxWaitTime();

    LOG.info(
        "Initialized DynamicOffHeapSizingMemoryTarget with TOTAL_MEMORY_SHARED = {}",
        TOTAL_MEMORY_SHARED);
  }

  public DynamicOffHeapSizingMemoryTarget(MemoryTarget delegated) {
    this.delegated = delegated;
  }

  @Override
  public long borrow(long size) {
    if (size == 0) {
      return 0;
    }

    // Inform Spark's Unified Memory Manager of the allocation. It will return the amount of free
    // memory available. This will also trigger operator spilling if under memory pressure. With
    // DynamicOffHeapSizingMemoryTarget, both Spark-based and Gluten-based operators are treated
    // as on-heap and can be spilled to free up memory. Data spilled by Spark operators is cleaned
    // up by an explicit GC call triggered later in this code. Gluten operators are expected to
    // clean up their own spilled data.
    long reserved = delegated.borrow(size);

    // Only JVM shrinking can reclaim space from the total JVM memory.
    // See https://github.com/apache/incubator-gluten/issues/9276.

    MemoryState state = MemoryState.captureCurrentState();

    if (state.totalWithRequestExceedMaxThreshold(reserved)) {
      // Perform GC synchronously to shrink memory; native tasks need to wait for this to obtain
      // more memory.
      MemoryState newState = shrinkOnHeapMemory(reserved);
      // Check if we can allocate the requested size again after JVM shrinking(GC).
      if (newState.totalWithRequestExceedMaxThreshold(reserved)) {
        LOG.warn(
            "Failing allocation as unified memory is OOM. "
                + "Used Off-heap: {}, "
                + "Used On-Heap: {}, "
                + "Free On-heap: {}, "
                + "Total On-heap: {}, "
                + "Max On-heap: {}, "
                + "Allocation: {}.",
            newState.totalOffHeapMemory,
            newState.usedOnHeapMemory,
            newState.freeOnHeapMemory,
            newState.totalOnHeapMemory,
            newState.maximumMemory,
            reserved);

        delegated.repay(reserved);
        return 0;
      }
    }

    USED_OFF_HEAP_BYTES.addAndGet(reserved);

    if (state.shouldTriggerAsyncOnHeapMemoryShrink(reserved)) {
      // Proactively trigger memory shrinking in the thread pool to prevent GC from blocking
      // native task execution.
      SparkThreadPoolUtil.triggerGCInThreadPool(
          DynamicOffHeapSizingMemoryTarget::asyncShrinkOnHeapMemory);
    }

    return reserved;
  }

  @Override
  public long repay(long size) {
    delegated.repay(size);
    USED_OFF_HEAP_BYTES.addAndGet(-size);
    return size;
  }

  @Override
  public long usedBytes() {
    return delegated.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public static boolean isJava9OrLater() {
    String spec = System.getProperty("java.specification.version", "1.8");
    // "1.8" → 8, "9" → 9, "11" → 11, etc.
    if (spec.startsWith("1.")) {
      spec = spec.substring(2);
    }
    try {
      return Integer.parseInt(spec) >= 9;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static long getTotalExplicitGCCount() {
    return TOTAL_EXPLICIT_GC_COUNT.get();
  }

  private static long getTotalGcCount() {
    return ManagementFactory.getGarbageCollectorMXBeans().stream()
        .mapToLong(GarbageCollectorMXBean::getCollectionCount)
        .sum();
  }

  // Trigger GC and wait for it to complete.
  private static synchronized void triggerGC() throws InterruptedException {
    long startTime = System.nanoTime();
    long beforeGCCount = getTotalGcCount();

    System.gc();

    while (beforeGCCount >= getTotalGcCount()
        && NANOSECONDS.toMillis(System.nanoTime() - startTime) < GC_MAX_WAIT_TIME) {
      Thread.sleep(10);
    }

    if (getTotalGcCount() == beforeGCCount) {
      throw new InterruptedException(
          String.format(
              "GC did not complete after %s ms",
              NANOSECONDS.toMillis(System.nanoTime() - startTime)));
    }

    TOTAL_EXPLICIT_GC_COUNT.incrementAndGet();
  }

  private static synchronized MemoryState shrinkOnHeapMemory(long required) {
    MemoryState state = MemoryState.captureCurrentState();
    if (state.totalWithRequestExceedMaxThreshold(required)) {
      return shrinkOnHeapMemory0(state, required);
    } else {
      return state;
    }
  }

  private static synchronized void asyncShrinkOnHeapMemory() {
    MemoryState state = MemoryState.captureCurrentState();
    if (state.totalWithRequestExceedAsyncThreshold(0)) {
      shrinkOnHeapMemory0(state, 0);
    }
  }

  // Sets MaxHeapFreeRatio and MinHeapFreeRatio to GC_MAX_HEAP_FREE_RATIO before shrinking JVM
  // This will increase the chances of JVM releasing memory.
  public static synchronized MemoryState shrinkOnHeapMemory0(MemoryState state, long required) {
    boolean updateMaxHeapFreeRatio = false;
    Object hotSpotBean = null;
    String maxHeapFreeRatioName = "MaxHeapFreeRatio";
    String minHeapFreeRatioName = "MinHeapFreeRatio";
    int newValue = (int) (GC_MAX_HEAP_FREE_RATIO * 100);

    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      Class<?> beanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
      hotSpotBean =
          ManagementFactory.newPlatformMXBeanProxy(
              mbs, "com.sun.management:type=HotSpotDiagnostic", beanClass);

      Method setOption = beanClass.getMethod("setVMOption", String.class, String.class);
      if (newValue < ORIGINAL_MIN_HEAP_FREE_RATIO) {
        // Adjust the MinHeapFreeRatio to avoid the violation of the MaxHeapFreeRatio.
        setOption.invoke(hotSpotBean, minHeapFreeRatioName, Integer.toString(newValue));
      }
      if (newValue < ORIGINAL_MAX_HEAP_FREE_RATIO) {
        setOption.invoke(hotSpotBean, maxHeapFreeRatioName, Integer.toString(newValue));
        updateMaxHeapFreeRatio = true;
        LOG.info(
            String.format(
                "Updated VM flags: MaxHeapFreeRatio from %d to %d.",
                ORIGINAL_MAX_HEAP_FREE_RATIO, newValue));
      }
      return shrinkOnHeapMemory1(state, required);
    } catch (Exception e) {
      LOG.warn(
          "Failed to update JVM heap free ratio via HotSpotDiagnosticMXBean: {}", e.toString());
      return state;
    } finally {
      // Reset the MaxHeapFreeRatio to the original values.
      if (hotSpotBean != null && updateMaxHeapFreeRatio) {
        try {
          Class<?> beanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
          Method setOption = beanClass.getMethod("setVMOption", String.class, String.class);
          setOption.invoke(
              hotSpotBean, maxHeapFreeRatioName, Integer.toString(ORIGINAL_MAX_HEAP_FREE_RATIO));
          LOG.info("Reverted VM flags back.");
        } catch (Exception ignore) {
          // best‐effort revert
        }
      }
    }
  }

  // Trigger GC. If GC could not do the job, retry if synchronous GC.
  private static synchronized MemoryState shrinkOnHeapMemory1(
      MemoryState beforeState, long required) throws InterruptedException {
    boolean isAsyncGc = required == 0;
    long startTime = System.nanoTime();
    if (isAsyncGc) {
      LOG.info("Starting async gc to shrink JVM memory: {}", beforeState);
    } else {
      LOG.info(
          "Starting full gc to shrink JVM memory: Required: {}, {}",
          Utils.bytesToString(required),
          beforeState);
    }
    // Explicitly calling System.gc() to trigger a full garbage collection.
    // This is necessary in this context to attempt to shrink JVM memory usage
    // when off-heap memory allocation is constrained. Use of System.gc() is
    // generally discouraged due to its unpredictable performance impact, but
    // here it is used as a last resort to prevent memory allocation failures.
    triggerGC();
    MemoryState newState = MemoryState.captureCurrentState();
    int gcRetryTimes = 0;
    int timeToWait = 200;
    while (!isAsyncGc
        && gcRetryTimes < MAX_GC_RETRY
        && newState.totalWithRequestExceedMaxThreshold(required)
        && newState.canShrinkJVMMemory()) {
      // Wait before retry
      Thread.sleep(INITIAL_GC_RETRY_WAIT_TIME);
      timeToWait = timeToWait * 2;

      // System.gc() is just a suggestion; the JVM may ignore it or perform only a partial GC.
      // Here, the total memory is not reduced but the free memory ratio is bigger than the
      // GC_MAX_HEAP_FREE_RATIO. So we need to call System.gc() again to try to reduce the total
      // memory.
      // This is a workaround for the JVM's behavior of not reducing the total memory after GC.
      triggerGC();
      newState = MemoryState.captureCurrentState();
      gcRetryTimes++;
    }

    if (isAsyncGc) {
      LOG.info(
          "Finished async gc to shrink JVM memory: {} [Time Taken: {} ms].",
          newState,
          NANOSECONDS.toMillis(System.nanoTime() - startTime));
    } else {
      LOG.info(
          "Finished full gc to shrink JVM memory: Required: "
              + "{}, {} "
              + "[GC Retry times: {}, Time Taken: {} ms].",
          Utils.bytesToString(required),
          newState,
          gcRetryTimes,
          NANOSECONDS.toMillis(System.nanoTime() - startTime));
    }

    return newState;
  }
}
