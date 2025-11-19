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
import org.apache.gluten.extension.PessimisticTransformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.util.SparkTestUtil

class PessimisticFallbackSuite
  extends VeloxWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper
  with ConfigurationHelper
  with FallbackHelper {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  val funcId_bloom_filter_agg = new FunctionIdentifier("bloom_filter_agg")
  val funcId_might_contain = new FunctionIdentifier("might_contain")

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

    // Register 'bloom_filter_agg' to builtin.
    spark.sessionState.functionRegistry.registerFunction(
      funcId_bloom_filter_agg,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "bloom_filter_agg"),
      (children: Seq[Expression]) =>
        children.size match {
          case 1 => new BloomFilterAggregate(children.head)
          case 2 => new BloomFilterAggregate(children.head, children(1))
          case 3 => new BloomFilterAggregate(children.head, children(1), children(2))
        }
    )

    // Register 'might_contain' to builtin.
    spark.sessionState.functionRegistry.registerFunction(
      funcId_might_contain,
      new ExpressionInfo(classOf[BloomFilterMightContain].getName, "might_contain"),
      (children: Seq[Expression]) => BloomFilterMightContain(children.head, children(1))
    )
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set(GlutenConfig.PESSIMISTIC_FALLBACK.key, "true")
  }

  test("Pessimistic Fallback for Bloom Filter") {
    spark.udf.register("new_udf", () => true)

    val veloxBloomFilterMaxNumBits = 4194304L
    val table = "tmp1"
    val numEstimatedItems = 5000000L

    def test(f1: Boolean, f2: Boolean, nativeAggCount: Int, sparkAggCount: Int): Unit = {
      val sqlString = s"""
                         |SELECT c2
                         |FROM $table
                         |WHERE might_contain(
                         |            (SELECT bloom_filter_agg(c2,
                         |              cast($numEstimatedItems as long),
                         |              cast($veloxBloomFilterMaxNumBits as long))
                         |             FROM $table), c2) ${if (f1) "AND new_udf()" else ""}
                         |
                         |UNION ALL
                         |
                         |SELECT c2
                         |FROM $table
                         |WHERE might_contain(
                         |            (SELECT bloom_filter_agg(c2,
                         |              cast($numEstimatedItems as long),
                         |              cast($veloxBloomFilterMaxNumBits as long))
                         |             FROM $table), c2) ${if (f2) "AND new_udf()" else ""}
                      """.stripMargin
      val df = spark.sql(sqlString)
      df.collect()
      SparkTestUtil.waitForListenerBus(spark.sparkContext)
      assert(!PessimisticTransformer.fallbackEnabledForId(df.queryExecution.id))
      assert(collectWithSubqueries(df.queryExecution.executedPlan) {
        case o: FlushableHashAggregateExecTransformer => o
        case o: HashAggregateExecTransformer => o
      }.size == nativeAggCount)
      assert(collectWithSubqueries(df.queryExecution.executedPlan) {
        case o: ObjectHashAggregateExec => o
      }.size == sparkAggCount)
    }

    withSQLConf(
      GlutenConfig.PESSIMISTIC_FALLBACK.key -> "true",
      GlutenConfig.PESSIMISTIC_BLOOM_FILTER.key -> "true"
    ) {
      // Test with all the applications of the Bloom Filter as supported
      test(f1 = false, f2 = false, 4, 0)

      // Test with any application of the Bloom Filter as not supported
      test(f1 = true, f2 = false, 0, 2)
      test(f1 = true, f2 = false, 0, 2)
      test(f1 = true, f2 = true, 0, 2)
    }

    withSQLConf(
      GlutenConfig.PESSIMISTIC_FALLBACK.key -> "true",
      GlutenConfig.PESSIMISTIC_BLOOM_FILTER.key -> "true",
      "spark.sql.adaptive.enabled" -> "false"
    ) {
      test(f1 = false, f2 = false, 0, 2)
      test(f1 = true, f2 = false, 0, 2)
      test(f1 = true, f2 = false, 0, 2)
      test(f1 = true, f2 = true, 0, 2)
    }
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
