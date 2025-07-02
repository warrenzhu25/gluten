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
package com.google.cloud.spark.bigquery.v2.context;

import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.repackaged.com.google.protobuf.ByteString;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * A singleton factory responsible for creating InputPartitionReaderContext instances
 * using reflection. This allows for compatibility with different versions of the
 * BigQuery connector.
 */
public enum PartitionReaderContextFactory {
  INSTANCE; // The single instance of the factory.

  @SuppressWarnings("unchecked")
  public InputPartitionReaderContext<ColumnarBatch> createPartitionReaderContext(
      String className,
      Iterator<ReadRowsResponse> readRowsResponses,
      ByteString serializedArrowSchema,
      ReadRowsHelper readRowsHelper,
      List<String> selectedFields,
      BigQueryStorageReadRowsTracer tracer,
      Optional<StructType> userProvidedSchema,
      int numBackgroundThreads,
      ReadSession.TableReadOptions.ResponseCompressionCodec responseCompressionCodec) {

    try {
      Class<?> readerClass = Class.forName(className);
      Object[] constructorArgs;
      Constructor<?> constructor;

      try {
        // Try to find the constructor with the new ResponseCompressionCodec parameter
        Class<?>[] parameterTypes = {
            java.util.Iterator.class,
            Class.forName("com.google.cloud.spark.bigquery.repackaged.com.google.protobuf" +
                ".ByteString"),
            Class.forName("com.google.cloud.bigquery.connector.common.ReadRowsHelper"),
            java.util.List.class,
            Class.forName("com.google.cloud.bigquery.connector.common" +
                ".BigQueryStorageReadRowsTracer"),
            java.util.Optional.class,
            int.class,
            Class.forName("com.google.cloud.spark.bigquery.repackaged" +
                ".com.google.cloud.bigquery.storage.v1" +
                ".ReadSession$TableReadOptions$ResponseCompressionCodec")
        };

        constructor = readerClass.getDeclaredConstructor(parameterTypes);
        constructorArgs =
            new Object[]{
                readRowsResponses,
                serializedArrowSchema,
                readRowsHelper,
                selectedFields,
                tracer,
                userProvidedSchema,
                numBackgroundThreads,
                responseCompressionCodec
            };
      } catch (NoSuchMethodException e) {
        // Fallback to the old constructor if the new one is not found
        Class<?>[] parameterTypes = {
            java.util.Iterator.class,
            Class.forName("com.google.cloud.spark.bigquery.repackaged.com.google.protobuf" +
                ".ByteString"),
            Class.forName("com.google.cloud.bigquery.connector.common.ReadRowsHelper"),
            java.util.List.class,
            Class.forName("com.google.cloud.bigquery.connector.common" +
                ".BigQueryStorageReadRowsTracer"),
            java.util.Optional.class,
            int.class
        };
        constructor = readerClass.getDeclaredConstructor(parameterTypes);
        constructorArgs =
            new Object[]{
                readRowsResponses,
                serializedArrowSchema,
                readRowsHelper,
                selectedFields,
                tracer,
                userProvidedSchema,
                numBackgroundThreads
            };
      }

      return (InputPartitionReaderContext<ColumnarBatch>) constructor.newInstance(constructorArgs);

    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create partition reader context for class " + className, e);
    }
  }
}