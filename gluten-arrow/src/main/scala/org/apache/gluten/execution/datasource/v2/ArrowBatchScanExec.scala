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
package org.apache.gluten.execution.datasource.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.BaseArrowScanExec
import org.apache.spark.sql.execution.datasources.v2.{ArrowBatchScanExecShim, BatchScanExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ArrowBatchScanExec(
    original: BatchScanExec,
    override val output: Seq[AttributeReference],
    override val runtimeFilters: Seq[Expression])
  extends ArrowBatchScanExecShim(original, output, runtimeFilters)
  with BaseArrowScanExec {
  override def doCanonicalize(): ArrowBatchScanExec =
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output)
    )

  override def nodeName: String = "Arrow" + original.nodeName

  override lazy val metrics = {
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")) ++
      customMetrics
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].map {
      b =>
        numOutputRows += b.numRows()
        b
    }
  }
}
