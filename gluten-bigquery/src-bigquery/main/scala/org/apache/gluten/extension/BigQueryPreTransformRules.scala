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
package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.datasource.v2.ArrowBatchScanExec

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import com.google.cloud.spark.bigquery.SparkBigQueryConfig
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1.DataFormat
import com.google.cloud.spark.bigquery.v2.Spark31BigQueryScanBuilder
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext

import java.lang.reflect.Field

object BigQueryPreTransformRules {
  def rules: Seq[Rule[SparkPlan]] = batchScanRule :: Nil

  val batchScanRule: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformUp {
      case plan: BatchScanExec
          if GlutenConfig.get.enableBigQueryDirectRead &&
            plan.scan.isInstanceOf[Spark31BigQueryScanBuilder] &&
            isArrowReadFormat(plan.scan.asInstanceOf[Spark31BigQueryScanBuilder]) =>
        ArrowBatchScanExec(plan, plan.output, plan.runtimeFilters)
    }

  private def isArrowReadFormat(scan: Spark31BigQueryScanBuilder): Boolean = {
    try {
      val ctxField: Field = classOf[Spark31BigQueryScanBuilder].getDeclaredField("ctx")
      ctxField.setAccessible(true)
      val ctx = ctxField.get(scan).asInstanceOf[BigQueryDataSourceReaderContext]
      val optionsField: Field = classOf[BigQueryDataSourceReaderContext].getDeclaredField("options")
      optionsField.setAccessible(true)
      val optionsConfig = optionsField.get(ctx).asInstanceOf[SparkBigQueryConfig]
      val readDataFormatField: Field =
        classOf[SparkBigQueryConfig].getDeclaredField("readDataFormat")
      readDataFormatField.setAccessible(true)
      val dataFormat = readDataFormatField.get(optionsConfig).asInstanceOf[DataFormat]

      dataFormat == DataFormat.ARROW
    } catch {
      case _: Exception =>
        false
    }
  }
}
