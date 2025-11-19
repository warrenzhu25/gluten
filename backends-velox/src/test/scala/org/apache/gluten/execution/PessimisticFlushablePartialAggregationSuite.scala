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
import org.apache.spark.sql.execution.ExpandExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

import org.scalatest.matchers.should.Matchers

class PessimisticFlushablePartialAggregationSuite
  extends VeloxWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper
  with FallbackHelper
  with Matchers {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  private var customConf: SparkConf = _

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")

    if (customConf != null) {
      conf.setAll(customConf.getAll)
    }
    conf
  }

  override def beforeAll(): Unit = {
    try {
      super.afterAll()
    } catch {
      case e: Exception =>
        logWarning("Exception during cleanup in beforeAll", e)
    }

    super.beforeAll()

    spark
      .range(100)
      .selectExpr("cast(id % 3 as int) as c1", "id as c2")
      .write
      .format("parquet")
      .mode("overwrite")
      .saveAsTable("tmp1")
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    afterAll()
  }

  private val VELOX_FLUSHABLE_AGG_KEY =
    "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation"

  test("Pessimistic fallback: flushable aggregation enabled (default behavior under fallback)") {
    customConf = new SparkConf()
      .set(GlutenConfig.PESSIMISTIC_FALLBACK.key, "true")
      .set(GlutenConfig.PESSIMISTIC_FLUSHABLE_PARTIAL_AGGREGATION.key, "true")

    beforeAll()

    runQueryAndCompare("select c1, count(c2) from tmp1 group by rollup(c1)") {
      df =>
        val plan = df.queryExecution.executedPlan

        val sessionConf = spark.conf
        sessionConf
          .get(GlutenConfig.PESSIMISTIC_FLUSHABLE_PARTIAL_AGGREGATION.key)
          .shouldEqual("true")
        sessionConf.get(VELOX_FLUSHABLE_AGG_KEY).shouldEqual("true")
        sessionConf.getOption(GlutenConfig.COLUMNAR_EXPAND_ENABLED.key).shouldEqual(Some("false"))
        sessionConf.getOption(GlutenConfig.COLUMNAR_GENERATE_ENABLED.key).shouldEqual(Some("false"))

        assert(collect(plan) { case e: ExpandExec => e }.nonEmpty, "Expected Vanilla ExpandExec")
        assert(
          collect(plan) { case h: HashAggregateExec => h }.nonEmpty,
          "Expected Vanilla HashAggregateExec")

        assert(collect(plan) {
          case e: ProjectExecTransformer => e
        }.isEmpty) // Simplified check for Gluten ops if needed
    }
  }

  test("Pessimistic fallback: flushable aggregation explicitly disabled (user override)") {
    customConf = new SparkConf()
      .set(GlutenConfig.PESSIMISTIC_FALLBACK.key, "true")
      .set(GlutenConfig.PESSIMISTIC_FLUSHABLE_PARTIAL_AGGREGATION.key, "false")

    beforeAll()

    runQueryAndCompare("select c1, count(c2) from tmp1 group by rollup(c1)") {
      df =>
        val plan = df.queryExecution.executedPlan

        val sessionConf = spark.conf
        sessionConf
          .get(GlutenConfig.PESSIMISTIC_FLUSHABLE_PARTIAL_AGGREGATION.key)
          .shouldEqual("false")
        sessionConf.get(VELOX_FLUSHABLE_AGG_KEY).shouldEqual("false")
        sessionConf.getOption(GlutenConfig.COLUMNAR_EXPAND_ENABLED.key).shouldEqual(None)
        sessionConf.getOption(GlutenConfig.COLUMNAR_GENERATE_ENABLED.key).shouldEqual(None)

        assert(
          collect(plan) { case e: ExpandExec => e }.isEmpty,
          "Did not expect Vanilla ExpandExec")
        assert(
          collect(plan) { case h: HashAggregateExec => h }.isEmpty,
          "Did not expect Vanilla HashAggregateExec")

        assert(countGlutenPlans(plan) > 0)
    }
  }

  test("Pessimistic fallback: parent switch DISABLED (user override)") {
    customConf = new SparkConf()
      .set(GlutenConfig.PESSIMISTIC_FALLBACK.key, "false")
      .set(GlutenConfig.PESSIMISTIC_FLUSHABLE_PARTIAL_AGGREGATION.key, "true")

    beforeAll()

    runQueryAndCompare("select c1, count(c2) from tmp1 group by rollup(c1)") {
      df =>
        val plan = df.queryExecution.executedPlan

        val sessionConf = spark.conf
        sessionConf
          .get(GlutenConfig.PESSIMISTIC_FLUSHABLE_PARTIAL_AGGREGATION.key)
          .shouldEqual("true")
        sessionConf.get(VELOX_FLUSHABLE_AGG_KEY).shouldEqual("false")
        sessionConf.getOption(GlutenConfig.COLUMNAR_EXPAND_ENABLED.key).shouldEqual(None)
        sessionConf.getOption(GlutenConfig.COLUMNAR_GENERATE_ENABLED.key).shouldEqual(None)

        assert(
          collect(plan) { case e: ExpandExec => e }.isEmpty,
          "Did not expect Vanilla ExpandExec")
        assert(
          collect(plan) { case h: HashAggregateExec => h }.isEmpty,
          "Did not expect Vanilla HashAggregateExec")
        assert(countGlutenPlans(plan) > 0)
    }
  }

  test("Pessimistic fallback: ENABLED, User provides all nested configs (forced override)") {
    customConf = new SparkConf()
      .set(GlutenConfig.PESSIMISTIC_FALLBACK.key, "true")
      .set(GlutenConfig.PESSIMISTIC_FLUSHABLE_PARTIAL_AGGREGATION.key, "true")
      .set(GlutenConfig.COLUMNAR_EXPAND_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_GENERATE_ENABLED.key, "true")

    beforeAll()

    runQueryAndCompare("select c1, count(c2) from tmp1 group by rollup(c1)") {
      df =>
        val plan = df.queryExecution.executedPlan

        val sessionConf = spark.conf
        sessionConf
          .get(GlutenConfig.PESSIMISTIC_FLUSHABLE_PARTIAL_AGGREGATION.key)
          .shouldEqual("true")
        sessionConf.get(VELOX_FLUSHABLE_AGG_KEY).shouldEqual("true")
        sessionConf.getOption(GlutenConfig.COLUMNAR_EXPAND_ENABLED.key).shouldEqual(Some("true"))
        sessionConf.getOption(GlutenConfig.COLUMNAR_GENERATE_ENABLED.key).shouldEqual(Some("true"))

        assert(
          collect(plan) { case e: ExpandExec => e }.isEmpty,
          "Did not expect Vanilla ExpandExec")
        assert(
          collect(plan) { case h: HashAggregateExec => h }.isEmpty,
          "Did not expect Vanilla HashAggregateExec")
        assert(countGlutenPlans(plan) > 0)
    }
  }
}
