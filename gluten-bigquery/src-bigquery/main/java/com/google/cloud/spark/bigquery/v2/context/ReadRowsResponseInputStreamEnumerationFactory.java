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
import com.google.cloud.bigquery.connector.common.ReadRowsResponseInputStreamEnumeration;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1.ReadSession;

import java.lang.reflect.Constructor;
import java.util.Iterator;

/**
 * A singleton factory to create ReadRowsResponseInputStreamEnumeration instances,
 * handling different constructor signatures for version compatibility.
 */
public enum ReadRowsResponseInputStreamEnumerationFactory {
  INSTANCE; // The single instance of the factory

  public ReadRowsResponseInputStreamEnumeration create(
      Iterator<ReadRowsResponse> serverResponses,
      BigQueryStorageReadRowsTracer tracer,
      ReadSession.TableReadOptions.ResponseCompressionCodec responseCompressionCodec) {
    try {
      Class<?> readerClass =
          Class.forName(ReadRowsResponseInputStreamEnumeration.class.getCanonicalName());
      Constructor<?> constructor;
      Object[] constructorArgs;
      try {
        // Try to find the constructor with the new ResponseCompressionCodec parameter
        Class<?>[] parameterTypes = {
            java.util.Iterator.class,
            Class.forName("com.google.cloud.bigquery.connector.common" +
                ".BigQueryStorageReadRowsTracer"),
            Class.forName("com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery" +
                ".storage.v1.ReadSession$TableReadOptions$ResponseCompressionCodec")
        };
        constructor = readerClass.getDeclaredConstructor(parameterTypes);
        constructorArgs = new Object[]{serverResponses, tracer, responseCompressionCodec};
      } catch (NoSuchMethodException e) {
        // Fallback to the old constructor
        Class<?>[] parameterTypes = {
            java.util.Iterator.class,
            Class.forName("com.google.cloud.bigquery.connector.common" +
                ".BigQueryStorageReadRowsTracer")
        };
        constructor = readerClass.getDeclaredConstructor(parameterTypes);
        constructorArgs = new Object[]{serverResponses, tracer};
      }
      return (ReadRowsResponseInputStreamEnumeration) constructor.newInstance(constructorArgs);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create ReadRowsResponseInputStreamEnumeration", e);
    }
  }
}
