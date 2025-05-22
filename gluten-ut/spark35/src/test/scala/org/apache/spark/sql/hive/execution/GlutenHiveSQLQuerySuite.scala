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
package org.apache.spark.sql.hive.execution

import org.apache.gluten.execution.GlutenPlan

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.FileSourceScanLike
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class GlutenHiveSQLQuerySuite extends GlutenHiveSQLQuerySuiteBase {

  override def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
  }

  Seq("parquet", "orc").foreach {
    format =>
      testGluten(s"$format file with CHAR") {
        sql(s"DROP TABLE IF EXISTS test_$format")
        sql(
          s"CREATE TABLE test_$format (c10 char(10), s string, c7 char(7)) " +
            s"USING hive OPTIONS(fileFormat '$format')")
        sql(s"INSERT INTO test_$format VALUES('test ', 'test ', 'test ')")

        val df = spark.table(s"test_$format")
        checkAnswer(df, Row("test      ", "test ", "test   "))

        val hasSparkPlan = df.queryExecution.executedPlan.collect {
          case p if !p.isInstanceOf[GlutenPlan] => p
        }.nonEmpty

        assert(!hasSparkPlan)

        spark.sessionState.catalog.dropTable(
          TableIdentifier(s"test_$format"),
          ignoreIfNotExists = true,
          purge = false)
      }

      testGluten(s"$format file with VARCHAR") {
        sql(s"DROP TABLE IF EXISTS test_$format")
        sql(
          s"CREATE TABLE test_$format (vc10 varchar(10), s string, vc7 varchar(7)) " +
            s"USING hive OPTIONS(fileFormat '$format')")
        sql(s"INSERT INTO test_$format VALUES('test ', 'test ', 'test ')")

        val df = spark.table(s"test_$format")
        checkAnswer(df, Row("test ", "test ", "test "))

        val hasSparkPlan = df.queryExecution.executedPlan.collect {
          case p if !p.isInstanceOf[GlutenPlan] => p
        }.nonEmpty

        assert(!hasSparkPlan)

        spark.sessionState.catalog.dropTable(
          TableIdentifier(s"test_$format"),
          ignoreIfNotExists = true,
          purge = false)
      }
  }

  testGluten("orc file with CHAR and string schema") {
    sql("DROP TABLE IF EXISTS t")
    sql("CREATE TABLE t (c10 char(10), s string, c7 char(7)) USING hive OPTIONS(fileFormat 'orc')")
    sql("INSERT INTO t VALUES('test ', 'test ', 'test ')")

    val path = spark
      .table("t")
      .queryExecution
      .executedPlan
      .collectFirst { case f: FileSourceScanLike => f }
      .toSeq
      .flatMap(_.relation.location.rootPaths)
      .map(_.toString)
      .head

    val customSchema = StructType(
      Array(
        StructField("c10", StringType, nullable = true),
        StructField("s", StringType, nullable = true),
        StructField("c7", StringType, nullable = true)
      ))

    spark.read.schema(customSchema).orc(path).createOrReplaceTempView("test_orc")

    val df = spark.table("test_orc")
    checkAnswer(df, Row("test      ", "test ", "test   "))

    val hasSparkPlan = df.queryExecution.executedPlan.collect {
      case p if !p.isInstanceOf[GlutenPlan] => p
    }.nonEmpty

    assert(!hasSparkPlan)

    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_orc"),
      ignoreIfNotExists = true,
      purge = false)

    spark.sessionState.catalog.dropTable(
      TableIdentifier("t"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("hive orc scan") {
    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      sql("DROP TABLE IF EXISTS test_orc")
      sql(
        "CREATE TABLE test_orc (name STRING, favorite_color STRING)" +
          " USING hive OPTIONS(fileFormat 'orc')")
      sql("INSERT INTO test_orc VALUES('test_1', 'red')");
      val df = spark.sql("select * from test_orc")
      checkAnswer(df, Seq(Row("test_1", "red")))
      checkOperatorMatch[HiveTableScanExecTransformer](df)
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_orc"),
      ignoreIfNotExists = true,
      purge = false)
  }

}
