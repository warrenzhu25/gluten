/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file is taken from the Open Source Spark BigQuery Connector
 * https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/71ff7f1e3b9bed4624688e85655863b294f955c1/spark-bigquery-dsv2/spark-bigquery-dsv2-common/src/main/java/com/google/cloud/spark/bigquery/v2/context/ArrowColumnBatchPartitionReaderContext.java
 *
 * This file reimplements the OSS Spark BQ Connector's logic to output column
 * vector that wraps the underlying Arrow Vector returned by BQ, but the
 * column vector type is changed to ensure that ArrowWritableColumnVector is returned
 * which is a format that Gluten can work upon.
 */
package com.google.cloud.spark.bigquery.v2.context;

import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.GlutenParallelArrowReader;
import com.google.cloud.bigquery.connector.common.IteratorMultiplexer;
import com.google.cloud.bigquery.connector.common.NonInterruptibleBlockingBytesChannel;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.cloud.spark.bigquery.repackaged.com.google.common.collect.ImmutableList;
import com.google.cloud.spark.bigquery.repackaged.com.google.common.util.concurrent.MoreExecutors;
import com.google.cloud.spark.bigquery.repackaged.com.google.protobuf.ByteString;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;
import org.apache.gluten.vectorized.ArrowWritableColumnVector;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GlutenArrowColumnBatchPartitionReaderContext
    implements InputPartitionReaderContext<ColumnarBatch> {
  private static final long maxAllocation = 500 * 1024 * 1024;

  // ... All inner classes (ArrowReaderAdapter, SimpleAdapter, ParallelReaderAdapter) remain
  // unchanged ...
  interface ArrowReaderAdapter extends AutoCloseable {
    boolean loadNextBatch() throws IOException;

    VectorSchemaRoot root() throws IOException;
  }

  static class SimpleAdapter implements ArrowReaderAdapter {
    private final ArrowReader reader;

    SimpleAdapter(ArrowReader reader) {
      this.reader = reader;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      return reader.loadNextBatch();
    }

    @Override
    public VectorSchemaRoot root() throws IOException {
      return reader.getVectorSchemaRoot();
    }

    @Override
    public void close() throws Exception {
      reader.close(false);
    }
  }

  static class ParallelReaderAdapter implements ArrowReaderAdapter {
    private final GlutenParallelArrowReader reader;
    private final VectorLoader loader;
    private final VectorSchemaRoot root;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private IOException initialException;

    ParallelReaderAdapter(
        BufferAllocator allocator,
        List<ArrowReader> readers,
        ExecutorService executor,
        BigQueryStorageReadRowsTracer tracer,
        AutoCloseable closeable) {
      Schema schema = null;
      closeables.add(closeable);
      try {
        schema = readers.get(0).getVectorSchemaRoot().getSchema();
      } catch (IOException e) {
        initialException = e;
        closeables.addAll(readers);
        this.reader = null;
        this.loader = null;
        this.root = null;
        return;
      }
      BufferAllocator readerAllocator =
          allocator.newChildAllocator("ParallelReaderAllocator", 0, maxAllocation);
      root = VectorSchemaRoot.create(schema, readerAllocator);
      closeables.add(root);
      loader = new VectorLoader(root);
      this.reader = new GlutenParallelArrowReader(readers, executor, loader, tracer);
      closeables.add(0, reader);
      closeables.add(readerAllocator);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      if (initialException != null) {
        throw new IOException(initialException);
      }
      return reader.next();
    }

    @Override
    public VectorSchemaRoot root() {
      return root;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(closeables);
    }
  }


  private final ReadRowsHelper readRowsHelper;
  private final ArrowReaderAdapter reader;
  private final BufferAllocator allocator;
  private ColumnVector[] columnVectors;
  private ColumnarBatch currentBatch;
  private final BigQueryStorageReadRowsTracer tracer;
  private boolean closed = false;
  private final List<AutoCloseable> closeables = new ArrayList<>();

  public GlutenArrowColumnBatchPartitionReaderContext(
      Iterator<ReadRowsResponse> readRowsResponses,
      ByteString schema,
      ReadRowsHelper readRowsHelper,
      List<String> namesInOrder,
      BigQueryStorageReadRowsTracer tracer,
      Optional<StructType> userProvidedSchema,
      int numBackgroundThreads,
      ResponseCompressionCodec responseCompressionCodec) {
    this.allocator = ArrowBufferAllocators.contextInstance()
      .newChildAllocator(this.getClass().getSimpleName(), 0, maxAllocation);
    this.readRowsHelper = readRowsHelper;
    this.tracer = tracer;
    closeables.add(null);

    if (numBackgroundThreads == 1) {
      InputStream fullStream =
          makeSingleInputStream(readRowsResponses, schema, tracer, responseCompressionCodec);
      reader =
          new ParallelReaderAdapter(
              allocator,
              ImmutableList.of(newArrowStreamReader(fullStream)),
              MoreExecutors.newDirectExecutorService(),
              tracer.forkWithPrefix("BackgroundReader"),
              null);
    } else if (numBackgroundThreads > 1) {
      ExecutorService backgroundParsingService =
          new ThreadPoolExecutor(
              1,
              numBackgroundThreads - 1,
              2,
              TimeUnit.SECONDS,
              new SynchronousQueue<>(),
              new ThreadPoolExecutor.CallerRunsPolicy());
      IteratorMultiplexer multiplexer =
          new IteratorMultiplexer(readRowsResponses, numBackgroundThreads);
      List<ArrowReader> readers = new ArrayList<>();
      for (int x = 0; x < numBackgroundThreads; x++) {
        BigQueryStorageReadRowsTracer multiplexedTracer = tracer.forkWithPrefix("multiplexed-" + x);
        // Use the new factory here
        InputStream responseStream =
            new SequenceInputStream(
                ReadRowsResponseInputStreamEnumerationFactory.INSTANCE.create(
                    multiplexer.getSplit(x), multiplexedTracer, responseCompressionCodec));
        InputStream schemaAndBatches = new SequenceInputStream(schema.newInput(), responseStream);
        closeables.add(multiplexedTracer::finished);
        readers.add(newArrowStreamReader(schemaAndBatches));
      }
      reader =
          new ParallelReaderAdapter(
              allocator,
              readers,
              backgroundParsingService,
              tracer.forkWithPrefix("MultithreadReader"),
              multiplexer);
    } else {
      InputStream fullStream =
          makeSingleInputStream(readRowsResponses, schema, tracer, responseCompressionCodec);
      reader = new SimpleAdapter(newArrowStreamReader(fullStream));
    }
  }

  private InputStream makeSingleInputStream(
      Iterator<ReadRowsResponse> readRowsResponses,
      ByteString schema,
      BigQueryStorageReadRowsTracer tracer,
      ResponseCompressionCodec responseCompressionCodec) {
    // Use the new factory here as well
    InputStream batchStream =
        new SequenceInputStream(
            ReadRowsResponseInputStreamEnumerationFactory.INSTANCE.create(
                readRowsResponses, tracer, responseCompressionCodec));
    return new SequenceInputStream(schema.newInput(), batchStream);
  }

  public boolean next() throws IOException {
    tracer.nextBatchNeeded();
    if (closed) {
      return false;
    }
    tracer.rowsParseStarted();
    closed = !reader.loadNextBatch();

    if (closed) {
      return false;
    }

    VectorSchemaRoot root = reader.root();
    if (columnVectors == null) {
      ArrowWritableColumnVector[] columns =
          ArrowWritableColumnVector.loadColumns(root.getRowCount(), root.getFieldVectors());
      columnVectors = Arrays.stream(columns).toArray(ColumnVector[]::new);
    }
    currentBatch = new ColumnarBatch(columnVectors);
    currentBatch.setNumRows(root.getRowCount());
    tracer.rowsParseFinished(currentBatch.numRows());
    return true;
  }

  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public Optional<BigQueryStorageReadRowsTracer> getBigQueryStorageReadRowsTracer() {
    return Optional.of(tracer);
  }

  public void close() throws IOException {
    closed = true;
    if (currentBatch != null) {
      currentBatch.close();
    }
    try {
      tracer.finished();
      closeables.set(0, reader);
      AutoCloseables.close(closeables);
      allocator.close();
    } catch (Exception e) {
      throw new IOException("Failure closing arrow components. stream: " + readRowsHelper, e);
    } finally {
      try {
        readRowsHelper.close();
      } catch (Exception e) {
        throw new IOException("Failure closing stream: " + readRowsHelper, e);
      }
    }
  }

  private ArrowStreamReader newArrowStreamReader(InputStream fullStream) {
    BufferAllocator childAllocator =
        allocator.newChildAllocator("readerAllocator" + (closeables.size() - 1), 0, maxAllocation);
    closeables.add(childAllocator);
    return new ArrowStreamReader(
        new NonInterruptibleBlockingBytesChannel(fullStream),
        childAllocator,
        CommonsCompressionFactory.INSTANCE);
  }
}