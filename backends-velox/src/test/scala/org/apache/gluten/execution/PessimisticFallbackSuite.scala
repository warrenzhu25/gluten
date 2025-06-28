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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class PessimisticFallbackSuite
  extends VeloxWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper
  with ConfigurationHelper
  with FallbackHelper {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark
      .range(100)
      .selectExpr("cast(id % 3 as int) as c1", "id as c2")
      .write
      .format("parquet")
      .saveAsTable("tmp1")
    spark
      .range(100)
      .selectExpr("cast(id % 9 as int) as c1")
      .write
      .format("parquet")
      .saveAsTable("tmp2")
    spark
      .range(100)
      .selectExpr("cast(id % 3 as int) as c1", "cast(id % 9 as int) as c2")
      .write
      .format("parquet")
      .saveAsTable("tmp3")
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set(GlutenConfig.PESSIMISTIC_FALLBACK.key, "true")
  }

  test("Leaf node not supported") {
    var planWithoutFallback: SparkPlan = null
    withSQLConf(
      GlutenConfig.GLUTEN_ENABLED.key -> "false"
    ) {
      runQueryAndCompare("select c1, count(*) from tmp1 where c1 <= 1 group by c1") {
        df => planWithoutFallback = df.queryExecution.executedPlan
      }
    }
    withSQLConf(
      GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false"
    ) {
      runQueryAndCompare("select c1, count(*) from tmp1 where c1 <= 1 group by c1") {
        df =>
          val planWithFallback = df.queryExecution.executedPlan
          val errorMessage =
            s"""
               |WITH FALL BACK:
               |$planWithFallback
               |
               |WITHOUT FALLBACK:
               |$planWithoutFallback
               |""".stripMargin
          assert(
            countGlutenPlans(planWithoutFallback) == countGlutenPlans(planWithFallback),
            errorMessage)
          assert(
            planWithoutFallback.map(identity).size == planWithFallback.map(identity).size,
            errorMessage)
          assert(
            planWithoutFallback.canonicalized.fastEquals(planWithFallback.canonicalized),
            errorMessage)
          assert(countGlutenPlans(planWithFallback) == 0)
          assertContinuousSupport(planWithFallback)
      }
    }
  }

  test("Filter node not supported") {
    withSQLConf(
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "false"
    ) {
      runQueryAndCompare("select c1, count(*) from tmp1 where c1 <= 1 group by c1") {
        df =>
          val plan = df.queryExecution.executedPlan
          assert(countGlutenPlans(plan) == 3, plan)
          assertContinuousSupport(plan)
      }
    }
  }

  test("Hash Aggregate node not supported") {
    withSQLConf(
      GlutenConfig.COLUMNAR_HASHAGG_ENABLED.key -> "false"
    ) {
      runQueryAndCompare("select c1, count(*) from tmp1 where c1 <= 1 group by c1") {
        df =>
          val plan = df.queryExecution.executedPlan
          assert(countGlutenPlans(plan) == 4, plan)
          assertContinuousSupport(plan)
      }
    }
  }

  test("All supported") {
    runQueryAndCompare("select c1, count(*) from tmp1 where c1 <= 1 group by c1") {
      df =>
        val plan = df.queryExecution.executedPlan
        assert(countGlutenPlans(plan) == 11, plan)
        assertContinuousSupport(plan)
    }
  }

  test("Pessimistic fallback disabled") {
    withSQLConf(
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "false",
      GlutenConfig.PESSIMISTIC_FALLBACK.key -> "false"
    ) {
      runQueryAndCompare("select c1, count(*) from tmp1 where c1 <= 1 group by c1") {
        df =>
          val plan = df.queryExecution.executedPlan
          assertThrows[TestException] {
            assertContinuousSupport(plan)
          }
      }
    }
  }

  test("BHJ not supported") {
    withSQLConf(
      GlutenConfig.COLUMNAR_BROADCAST_EXCHANGE_ENABLED.key -> "false"
    ) {
      runQueryAndCompare(
        "select c1, n1 from (select c1 + 2 as n1 from tmp2), tmp1 where c1 == n1") {
        df =>
          val plan = df.queryExecution.executedPlan
          assert(countGlutenPlans(plan) == 9, plan)
          assertContinuousSupport(plan)
      }
    }
  }

  test("SHJ not supported and AQE Disabled") {
    withSQLConf(
      GlutenConfig.COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1"
    ) {
      runQueryAndCompare(
        "select c1, n1 from (select c1 + 2 as n1 from tmp2), tmp1 where c1 == n1") {
        df =>
          val plan = df.queryExecution.executedPlan
          assert(countGlutenPlans(plan) == 20, plan)
          assertContinuousSupport(plan)
      }
    }
  }

  test("Rewrite Test with Fallback") {
    var planWithoutFallback: SparkPlan = null
    withSQLConf(
      GlutenConfig.PESSIMISTIC_FALLBACK.key -> "false"
    ) {
      runQueryAndCompare("select count(c1, c2) from tmp1 where c1 <= 1") {
        df => planWithoutFallback = df.queryExecution.executedPlan
      }
    }
    runQueryAndCompare("select count(c1, c2) from tmp1 where c1 <= 1") {
      df =>
        val planWithFallback = df.queryExecution.executedPlan
        val errorMessage =
          s"""
             |WITH FALL BACK:
             |$planWithFallback
             |
             |WITHOUT FALLBACK:
             |$planWithoutFallback
             |""".stripMargin
        assert(
          countGlutenPlans(planWithoutFallback) == countGlutenPlans(planWithFallback),
          errorMessage)
        assert(
          planWithoutFallback.map(identity).size == planWithFallback.map(identity).size,
          errorMessage)
        assert(
          planWithoutFallback.canonicalized.fastEquals(planWithFallback.canonicalized),
          errorMessage)
        assertContinuousSupport(planWithFallback)
    }
  }
}
