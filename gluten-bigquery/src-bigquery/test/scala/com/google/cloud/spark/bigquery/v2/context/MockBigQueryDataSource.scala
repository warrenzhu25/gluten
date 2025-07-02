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

import org.apache.gluten.GlutenPlugin
import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, ScanBuilder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.cloud.spark.bigquery.{ArrowSchemaConverter, DataSourceVersion, SchemaConverters, SchemaConvertersConfiguration, SparkBigQueryConfig}
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.{Schema => BQSchema, StandardTableDefinition, TableId, TableInfo}
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1.DataFormat
import com.google.cloud.spark.bigquery.repackaged.com.google.common.collect.ImmutableMap
import com.google.cloud.spark.bigquery.repackaged.org.apache.arrow.memory.RootAllocator
import com.google.cloud.spark.bigquery.repackaged.org.apache.arrow.vector.{BigIntVector, VarCharVector}
import com.google.cloud.spark.bigquery.v2.Spark31BigQueryScanBuilder

import java.nio.charset.StandardCharsets
import java.util
import java.util.Optional

class MockBigQueryDataSource extends TableProvider with DataSourceRegister {
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    new MockBigQueryTable(Some(schema))
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    new MockBigQueryTable().schema()
  }

  override def shortName(): String = "mock-gluten-bigquery-source"
}

object MockBigQueryDataSource {
  def createMockReaderContext(
      properties: CaseInsensitiveStringMap,
      schema: StructType): BigQueryDataSourceReaderContext = {
    val sparkSession = SparkSession.active
    val sqlContext = sparkSession.sqlContext
    val config = createConfigForArrow(properties, schema, sparkSession)
    val tableId = TableId.of("my_project", "my_dataset", "my_mock_table")
    val schemaConverter = SchemaConverters.from(SchemaConvertersConfiguration.from(config))
    val bqSchema: BQSchema = schemaConverter.toBigQuerySchema(schema)
    val tableDefinition = StandardTableDefinition.of(bqSchema)
    val mockTableInfo = TableInfo.of(tableId, tableDefinition)

    val readSessionCreatorConfig = config.toReadSessionCreatorConfig()

    new BigQueryDataSourceReaderContext(
      mockTableInfo,
      null,
      null,
      null,
      readSessionCreatorConfig,
      Optional.empty(),
      Optional.of(schema),
      "test-app-id",
      config,
      sqlContext,
      sparkSession,
      config.toReadTableOptions
    )
  }

  def createConfigForArrow(
      properties: CaseInsensitiveStringMap,
      schema: StructType,
      spark: SparkSession): SparkBigQueryConfig = {

    val options: java.util.Map[String, String] =
      new java.util.HashMap[String, String](properties)
    options.put("readDataFormat", DataFormat.ARROW.toString)
    options.put("table", "my_project.my_dataset.my_mock_table")

    val customDefaults: ImmutableMap[String, String] = ImmutableMap.of()
    val dataSourceVersion: DataSourceVersion = DataSourceVersion.V2
    val schemaAsJavaOptional: Optional[StructType] = Optional.of(schema)
    val tableIsMandatory = true

    SparkBigQueryConfig.from(
      options,
      customDefaults,
      dataSourceVersion,
      spark,
      schemaAsJavaOptional,
      tableIsMandatory
    )
  }
}

class MockBigQueryTable(customSchema: Option[StructType] = None) extends Table with SupportsRead {
  override def name(): String = "mock_bigquery_table"
  override def schema(): StructType = customSchema.getOrElse {
    StructType(
      Seq(StructField("id", DataTypes.LongType), StructField("word", DataTypes.StringType)))
  }
  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.BATCH_READ)
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new MockBigQueryScanBuilder(schema(), options)
}

class MockBigQueryScanBuilder(schema: StructType, properties: CaseInsensitiveStringMap)
  extends Spark31BigQueryScanBuilder(
    MockBigQueryDataSource.createMockReaderContext(properties, schema)
  ) {

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new MockBigQueryInputPartition())
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new MockBigQueryPartitionReaderFactory()
  }
}

class MockBigQueryInputPartition extends InputPartition

class MockBigQueryPartitionReaderFactory extends PartitionReaderFactory {
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val conf = SQLConf.get
    val useGluten =
      conf.getConfString(GlutenConfig.VELOX_BIGQUERY_DIRECT_READ.key, "false").toBoolean &&
        conf.getConfString("spark.plugins", "").contains(classOf[GlutenPlugin].getCanonicalName) &&
        conf.getConfString(GlutenConfig.GLUTEN_ENABLED.key, "true").toBoolean
    if (useGluten) {
      new MockBigQueryGlutenColumnarPartitionReader()
    } else {
      new MockBigQueryColumnarPartitionReader
    }
  }
  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = null
}

class MockBigQueryColumnarPartitionReader extends PartitionReader[ColumnarBatch] {
  private var batchReturned = false
  private val allocator = new RootAllocator(Long.MaxValue)
  private var batch: ColumnarBatch = _

  private def createBatch(): Unit = {
    val idVector = new BigIntVector("id", allocator)
    val wordVector = new VarCharVector("word", allocator)
    val numRows = 10000
    val data = (1 to numRows).map(i => (100L + i, s"word_$i"))
    idVector.allocateNew(data.length)
    wordVector.allocateNew(data.length)
    data.zipWithIndex.foreach {
      case ((id, word), i) =>
        idVector.setSafe(i, id)
        wordVector.setSafe(i, word.getBytes(StandardCharsets.UTF_8))
    }
    idVector.setValueCount(data.length)
    wordVector.setValueCount(data.length)
    val idConverter = ArrowSchemaConverter.newArrowSchemaConverter(idVector, null)
    val wordConverter = ArrowSchemaConverter.newArrowSchemaConverter(wordVector, null)
    this.batch = new ColumnarBatch(Array(idConverter, wordConverter), data.length)
  }

  override def next(): Boolean = {
    if (!batchReturned) {
      createBatch()
      batchReturned = true
      true
    } else false
  }
  override def get(): ColumnarBatch = this.batch
  override def close(): Unit = {
    if (batch != null) batch.close()
    allocator.close()
  }
}
