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
package org.apache.gluten.execution

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf

abstract class BigQuerySuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = ""
  override protected val fileFormat: String = ""

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.adaptive.enabled", "false")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.read
      .format("com.google.cloud.spark.bigquery.v2.context.MockBigQueryDataSource")
      .load()
      .createOrReplaceTempView("table")
  }

  test("Read Mock BigQuery without direct read") {
    withSQLConf(
      GlutenConfig.VELOX_BIGQUERY_DIRECT_READ.key -> "false"
    ) {
      runQueryAndCompare("select count(*) from table", noFallBack = false) {
        df => checkFallbackOperators(df, 3)
      }
    }
  }

  test("Read Mock BigQuery without direct read with pessimistic fallback") {
    withSQLConf(
      GlutenConfig.VELOX_BIGQUERY_DIRECT_READ.key -> "false",
      GlutenConfig.PESSIMISTIC_FALLBACK.key -> "true"
    ) {
      runQueryAndCompare("select count(*) from table", noFallBack = false) {
        df => checkFallbackOperators(df, -1)
      }
    }
  }

  test("Read Mock BigQuery with direct read") {
    withSQLConf(
      GlutenConfig.VELOX_BIGQUERY_DIRECT_READ.key -> "true"
    ) {
      val df = sql("select count(*) from table")
      checkFallbackOperators(df, 2)
    }
  }

  test("Read Mock BigQuery with direct read with pessimistic fallback") {
    withSQLConf(
      GlutenConfig.VELOX_BIGQUERY_DIRECT_READ.key -> "true",
      GlutenConfig.PESSIMISTIC_FALLBACK.key -> "true"
    ) {
      val df = sql("select count(*) from table")
      checkFallbackOperators(df, 2)
    }
  }

  test("Read Mock BigQuery with direct read but gluten disabled, should fallback") {
    withSQLConf(
      GlutenConfig.VELOX_BIGQUERY_DIRECT_READ.key -> "true",
      GlutenConfig.GLUTEN_ENABLED.key -> "false"
    ) {
      runQueryAndCompare("select count(*) from table", noFallBack = false) {
        df => checkFallbackOperators(df, -1)
      }
    }
  }
}
