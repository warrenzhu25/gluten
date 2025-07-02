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
package com.google.cloud.spark.bigquery.v2.context

import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.nio.charset.StandardCharsets

class MockBigQueryGlutenColumnarPartitionReader extends PartitionReader[ColumnarBatch] {
  private var batchReturned = false
  private var batch: ColumnarBatch = _

  override def next(): Boolean = {
    if (batchReturned) return false
    createBatch()
    batchReturned = true
    true
  }

  override def get(): ColumnarBatch = this.batch

  override def close(): Unit = {
    if (batch != null) {
      batch.close()
    }
  }

  private def createBatch(): Unit = {
    val schema = StructType(
      Seq(StructField("id", DataTypes.LongType), StructField("word", DataTypes.StringType)))
    val numRows = 10000
    val mockData = (1 to numRows).map(i => (200L + i, s"direct_word_$i"))

    // 1. Allocate vectors using the factory method, based on the schema.
    val vectors = ArrowWritableColumnVector.allocateColumns(numRows, schema)
    val idVector = vectors(0).asInstanceOf[ArrowWritableColumnVector]
    val wordVector = vectors(1).asInstanceOf[ArrowWritableColumnVector]

    // 2. Populate the vectors row by row.
    mockData.zipWithIndex.foreach {
      case ((id, word), i) =>
        idVector.putLong(i, id)
        val wordBytes = word.getBytes(StandardCharsets.UTF_8)
        wordVector.putByteArray(i, wordBytes, 0, wordBytes.length)
    }

    // 3. Set the final row count on each vector.
    vectors.foreach(_.setValueCount(numRows))

    // 4. Create the batch. The cast is necessary because Array in Scala is invariant.
    this.batch = new ColumnarBatch(
      vectors.asInstanceOf[Array[org.apache.spark.sql.vectorized.ColumnVector]],
      numRows)
  }
}
