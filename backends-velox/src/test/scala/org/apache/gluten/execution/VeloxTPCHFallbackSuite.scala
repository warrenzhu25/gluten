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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlan

class VeloxTPCHFallbackAllSupportSuite extends VeloxTPCHSuite with FallbackHelper {
  override def subType(): String = "v1-bhj"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
      .set(GlutenConfig.PESSIMISTIC_FALLBACK.key, "true")
  }

  override protected def runTPCHQuery(
      queryNum: Int,
      tpchQueries: String,
      queriesResults: String,
      compareResult: Boolean = true,
      noFallBack: Boolean = true)(customCheck: DataFrame => Unit): Unit = {
    var planWithoutFallback: SparkPlan = null
    withSQLConf(GlutenConfig.PESSIMISTIC_FALLBACK.key -> "false") {
      withDataFrame(tpchSQL(queryNum, tpchQueries)) {
        df =>
          df.collect()
          planWithoutFallback = df.queryExecution.executedPlan
      }
    }
    withDataFrame(tpchSQL(queryNum, tpchQueries)) {
      df =>
        if (compareResult) {
          verifyTPCHResult(df, s"q$queryNum", queriesResults)
        } else {
          df.collect()
        }
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
          planWithoutFallback.canonicalized.fastEquals(planWithFallback.canonicalized),
          errorMessage)
        assert(
          planWithoutFallback.map(identity).size == planWithFallback.map(identity).size,
          errorMessage)
        assert(
          countGlutenPlans(planWithoutFallback) == countGlutenPlans(planWithFallback),
          errorMessage)
        checkDataFrame(noFallBack, customCheck, df)
    }
  }

  // q2: Sometimes plan is different
  // q17: In this when running on local, leaf node is not supported.
  override protected def excluded: Seq[String] = Seq("TPC-H q2", "TPC-H q17")
}

class VeloxTPCHFallbackNoLeafSupportSuite extends VeloxTPCHSuite with FallbackHelper {
  override def subType(): String = "v1-bhj"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
      .set(GlutenConfig.PESSIMISTIC_FALLBACK.key, "true")
      .set(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key, "false")
  }

  override protected def runTPCHQuery(
      queryNum: Int,
      tpchQueries: String,
      queriesResults: String,
      compareResult: Boolean = true,
      noFallBack: Boolean = true)(customCheck: DataFrame => Unit): Unit = {
    var planWithoutFallback: SparkPlan = null
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      withDataFrame(tpchSQL(queryNum, tpchQueries)) {
        df =>
          df.collect()
          planWithoutFallback = df.queryExecution.executedPlan
      }
    }
    withDataFrame(tpchSQL(queryNum, tpchQueries)) {
      df =>
        if (compareResult) {
          verifyTPCHResult(df, s"q$queryNum", queriesResults)
        } else {
          df.collect()
        }
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
        checkDataFrame(noFallBack, customCheck, df)
    }
  }
}
