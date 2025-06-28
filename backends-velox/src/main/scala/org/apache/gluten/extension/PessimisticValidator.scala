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

import org.apache.gluten.execution.{ColumnarToRowExecBase, GlutenPlan}
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.heuristic.RewrittenNodeWall
import org.apache.gluten.extension.columnar.validator.Validator

import org.apache.spark.sql.execution.{BaseSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}

import scala.annotation.tailrec

/**
 * Checks if all children are offloaded to Gluten. If any one node is not offloaded, validation
 * fails for current node.
 */
case object PessimisticValidator extends Validator {

  @tailrec
  private def childGlutenSupport(plan: SparkPlan): ValidationResult = {
    plan match {
      case _: ColumnarToRowExecBase => ValidationResult.failed("Row Format") // Break in chain
      case _: GlutenPlan => ValidationResult.succeeded
      case q: QueryStageExec => childGlutenSupport(q.plan)
      case e: Exchange => childGlutenSupport(e.child)
      case r: ReusedExchangeExec => childGlutenSupport(r.child)
      case a: AQEShuffleReadExec => childGlutenSupport(a.child)
      case s: BaseSubqueryExec => childGlutenSupport(s.child)
      case r: RewrittenNodeWall => childGlutenSupport(r.originalChild)
      case o =>
        val reason = FallbackTags.getOption(o) match {
          case Some(f) => f.reason()
          case None => s"${o.nodeName} not pushed to Gluten"
        }
        ValidationResult.failed(reason)
    }
  }

  private val FALLBACK_TAG = "[PESSIMISTIC FALLBACK] "

  override def validate(plan: SparkPlan): Validator.OutCome = {
    val unsupportedGlutenOperators = plan.children.map(childGlutenSupport).filterNot(_.ok())
    if (unsupportedGlutenOperators.isEmpty) {
      Validator.Passed
    } else {
      val reason = FALLBACK_TAG + unsupportedGlutenOperators
        .map(_.reason())
        .map(_.replace(FALLBACK_TAG, ""))
        .mkString(", ")
      Validator.Failed(reason)
    }
  }
}
