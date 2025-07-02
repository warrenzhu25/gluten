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
 * https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/71ff7f1e3b9bed4624688e85655863b294f955c1/spark-bigquery-dsv2/spark-bigquery-dsv2-common/src/main/java/com/google/cloud/spark/bigquery/v2/context/ArrowInputPartitionContext.java
 *
 * This file mirrors the file of the same name from the OSS BQ Connector
 * but does a runtime check to use Gluten's Partition Reader if Gluten is being
 * used and direct read is enabled.
 */
package com.google.cloud.spark.bigquery.v2.context;

import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.spark.bigquery.metrics.SparkBigQueryReadSessionMetrics;
import com.google.cloud.spark.bigquery.metrics.SparkMetricsSource;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.cloud.spark.bigquery.repackaged.com.google.common.base.Joiner;
import com.google.cloud.spark.bigquery.repackaged.com.google.common.collect.ImmutableList;
import com.google.cloud.spark.bigquery.repackaged.com.google.protobuf.ByteString;
import org.apache.gluten.GlutenPlugin;
import org.apache.gluten.config.GlutenConfig;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.cloud.spark.bigquery.repackaged.com.google.common.base.Optional.fromJavaUtil;

public class ArrowInputPartitionContext implements InputPartitionContext<ColumnarBatch> {

  private final BigQueryClientFactory bigQueryReadClientFactory;
  private final BigQueryTracerFactory tracerFactory;
  private final List<String> streamNames;
  private final ReadRowsHelper.Options options;
  private final ImmutableList<String> selectedFields;
  private final ByteString serializedArrowSchema;
  private final com.google.cloud.spark.bigquery.repackaged
      .com.google.common.base.Optional<StructType> userProvidedSchema;
  private final SparkBigQueryReadSessionMetrics sparkBigQueryReadSessionMetrics;
  private final ResponseCompressionCodec responseCompressionCodec;

  public ArrowInputPartitionContext(
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      List<String> names,
      ReadRowsHelper.Options options,
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse,
      Optional<StructType> userProvidedSchema,
      SparkBigQueryReadSessionMetrics sparkBigQueryReadSessionMetrics) {
    this(
        bigQueryReadClientFactory,
        tracerFactory,
        names,
        options,
        selectedFields,
        readSessionResponse,
        userProvidedSchema,
        sparkBigQueryReadSessionMetrics,
        ResponseCompressionCodec.RESPONSE_COMPRESSION_CODEC_UNSPECIFIED);
  }

  public ArrowInputPartitionContext(
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      List<String> names,
      ReadRowsHelper.Options options,
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse,
      Optional<StructType> userProvidedSchema,
      SparkBigQueryReadSessionMetrics sparkBigQueryReadSessionMetrics,
      ResponseCompressionCodec responseCompressionCodec) {
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.streamNames = names;
    this.options = options;
    this.selectedFields = selectedFields;
    this.serializedArrowSchema =
        readSessionResponse.getReadSession().getArrowSchema().getSerializedSchema();
    this.tracerFactory = tracerFactory;
    this.userProvidedSchema = fromJavaUtil(userProvidedSchema);
    this.sparkBigQueryReadSessionMetrics = sparkBigQueryReadSessionMetrics;
    this.responseCompressionCodec = responseCompressionCodec;
  }

  @Override
  public InputPartitionReaderContext<ColumnarBatch> createPartitionReaderContext() {
    SparkMetricsSource sparkMetricsSource = new SparkMetricsSource();

    TaskContext.get()
        .registerAccumulator(sparkBigQueryReadSessionMetrics.getBytesReadAccumulator());
    TaskContext.get().registerAccumulator(sparkBigQueryReadSessionMetrics.getRowsReadAccumulator());
    TaskContext.get()
        .registerAccumulator(sparkBigQueryReadSessionMetrics.getParseTimeAccumulator());
    TaskContext.get().registerAccumulator(sparkBigQueryReadSessionMetrics.getScanTimeAccumulator());

    SparkEnv.get().metricsSystem().registerSource(sparkMetricsSource);
    BigQueryStorageReadRowsTracer tracer =
        tracerFactory.newReadRowsTracer(
            Joiner.on(",").join(streamNames),
            sparkMetricsSource,
            Optional.of(sparkBigQueryReadSessionMetrics));
    List<ReadRowsRequest.Builder> readRowsRequests =
        streamNames.stream()
            .map(name -> ReadRowsRequest.newBuilder().setReadStream(name))
            .collect(Collectors.toList());
    ReadRowsHelper readRowsHelper =
        new ReadRowsHelper(bigQueryReadClientFactory, readRowsRequests, options);
    tracer.startStream();
    Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();

    SQLConf conf = SQLConf.get();
    boolean useGluten =
        Boolean.valueOf(conf.getConfString(GlutenConfig.VELOX_BIGQUERY_DIRECT_READ().key(),
            "false")) &&
            conf.getConfString("spark.plugins", "")
                .contains(GlutenPlugin.class.getCanonicalName()) &&
            Boolean.valueOf(conf.getConfString(GlutenConfig.GLUTEN_ENABLED().key(), "true"));

    if (useGluten) {
      return new GlutenArrowColumnBatchPartitionReaderContext(
          readRowsResponses,
          serializedArrowSchema,
          readRowsHelper,
          selectedFields,
          tracer,
          userProvidedSchema.toJavaUtil(),
          options.numBackgroundThreads(),
          responseCompressionCodec
      );
    }

    // Delegate the complex instantiation to the singleton factory
    return PartitionReaderContextFactory.INSTANCE.createPartitionReaderContext(
        ArrowColumnBatchPartitionReaderContext.class.getCanonicalName(),
        readRowsResponses,
        serializedArrowSchema,
        readRowsHelper,
        selectedFields,
        tracer,
        userProvidedSchema.toJavaUtil(),
        options.numBackgroundThreads(),
        responseCompressionCodec);
  }

  @Override
  public boolean supportColumnarReads() {
    return true;
  }
}
