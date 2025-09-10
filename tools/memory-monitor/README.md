# Spark Executor Monitor

A lightweight, in-process monitoring tool for Apache Spark applications. It's designed to run on Spark executors to capture key resource metrics for a specific block of code, helping you diagnose memory leaks, understand resource consumption, and profile performance.

The monitor periodically collects data in the background, and upon completion of the monitored code, it can generate an SVG time-series graph and upload it directly to Google Cloud Storage (GCS).

## Features
- **Periodic Metric Collection**: Gathers data on each executor at a configurable interval.
- **Default Probes**: Comes with built-in probes for common memory metrics:
    - Used On-Heap Memory: `Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()`
    - Allocated On-Heap Memory: `Runtime.getRuntime.totalMemory()`
    - Max On-Heap Memory: `Runtime.getRuntime.maxMemory()`
    - Resident Set Size (RSS): Physical memory usage of the JVM process.
- **Custom Probes**: Easily extend the monitor with your own custom lambdas to track any value you need.
- **Graph Generation**: Automatically generates a clean SVG time-series graph of the collected metrics using the `EvilPlot` library.
- **GCS Integration**: Seamlessly uploads the generated graph to a specified Google Cloud Storage bucket.

## Getting Started
You have two options: using the precompiled JAR or building it from the source.

### Option 1: Use the Precompiled JAR (Recommended)
A precompiled, ready-to-use JAR is available in a public GCS bucket. This is the quickest way to get started.

You can add it directly to an interactive spark-shell session:

```bash
spark-shell --jars gs://dataproc-experimental/memory-monitor-1.3.0-SNAPSHOT.jar
```

Or use it in your spark-submit command for a full application:

```bash
spark-submit \
  --master <your-master-url> \
  --class <your.main.Class> \
  --jars gs://dataproc-experimental/memory-monitor-1.3.0-SNAPSHOT.jar \
  /path/to/your-application.jar
```
### Option 2: Build from Source

If you prefer to build from the source, package the project into a self-contained JAR file using Maven.

```bash
mvn clean package
```

This will create the JAR in the `target/` directory. You can then use this local JAR file with the `--jars` flag.


## Using the Monitor in Your Code
Once the JAR is included in your Spark session, simply import the `Monitor` object and wrap the code you want to profile.

```scala
import com.google.Monitor

// Your Spark code...
// ...

Monitor.observe("gs://your-gcs-bucket/path/to/output-graph.png") {
    // This is the block of code that will be monitored
    val rdd = sc.parallelize(1 to 1000000)
    val result = rdd.map(x => (x, x * x)).reduceByKey(_ + _)
    result.collect()
}

// ...
// Rest of your Spark code
```
## The `observe` API
The `observe` method is the main entry point. It has several overloaded versions to provide flexibility.

### Basic Usage
This version monitors the code with default probes and prints the results to the console. No graph is generated.

#### Signature:

```scala
def observe[T](f: => T): T
```

#### Example:

```scala
Monitor.observe {
  // Code to monitor
}
```

### Monitoring with GCS Graph Upload
These versions execute the code block and, upon completion, generate an SVG graph and upload it to the specified GCS path.

#### Signature:

```scala
def observe[T](gcsPath: String)(f: => T): T
def observe[T](gcsPath: String, interval: Long)(f: => T): T
```

- `gcsPath`: The full GCS path for the output SVG file (e.g., `"gs://my-bucket/graphs/run1.svg"`).
- `interval`: The monitoring interval in milliseconds. **Defaults to 1000ms**.

#### Example:

```scala
// Monitor with default 1-second interval
Monitor.observe("gs://my-bucket/graphs/run1.svg") {
  // ... code ...
}

// Monitor with a custom 200ms interval
Monitor.observe("gs://my-bucket/graphs/run2.svg", 200L) {
  // ... code ...
}
```
### Adding Custom Monitoring Probes
You can supply your own metrics to monitor by providing a sequence of custom lambdas.

A custom probe is a tuple of `(String, () => Any)`:

- The String is the name of the metric, which will appear in the graph's legend.
- The lambda `() => Any` is a function that, when called, returns the metric's current value. The returned value should be numeric (e.g., `Long`, `Double`, `Int`).

#### Signature:

```scala
def observe[T](customLambdas: Seq[(String, () => Any)])(f: => T): T
def observe[T](customLambdas: Seq[(String, () => Any)], gcsPath: String)(f: => T): T
def observe[T](customLambdas: Seq[(String, () => Any)], gcsPath: String, interval: Long)(f: => T): T
```

#### Example:
Let's monitor the number of active threads in the JVM in addition to the default memory probes.

```scala
import com.google.Monitor
import java.lang.management.ManagementFactory

// 1. Define your custom probe
val threadProbe = "Active JVM Threads" -> { () =>
  ManagementFactory.getThreadMXBean.getThreadCount.toLong
}

// 2. Pass it to the observe method
Monitor.observe(
  customLambdas = Seq(threadProbe),
  gcsPath = "gs://my-bucket/graphs/run-with-threads.svg",
  interval = 500L
) {
  // ... code that might create a lot of threads ...
}
```
**Note**: Your custom probes are added in addition to the default memory and RSS probes.

## Output
You get two forms of output from the monitor.

### 1. Console Output
The raw data is printed to the driver's standard output in a CSV-like format. Each line represents a single observation from one executor.

**Format**: `milliseconds_since_start,value1,value2,value3,...`

```
1001,25436789,512000000,2048000000,87543210
2003,28976543,512000000,2048000000,89987654
...
```

### 2. SVG Graph in GCS
If a gcsPath is provided, a time-series graph is generated and saved to that location.

- **X-Axis**: Time elapsed since the start of the observe block (in milliseconds).
- **Y-Axis**: Metric values, automatically scaled to **Megabytes (MB)**.
- **Legend**: A legend at the bottom of the graph identifies the line for each metric.