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

import org.apache.spark.sql.execution.{ColumnarInputAdapter, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, ExchangeQueryStageExec}

import scala.annotation.tailrec

trait FallbackHelper {
  def countGlutenPlans(plan: SparkPlan): Int = {
    val currentNodeGlutenCount = plan match {
      case _: GlutenPlan => 1
      case _ => 0
    }
    currentNodeGlutenCount + getAllChildren(plan).map(countGlutenPlans).sum
  }

  @tailrec private def isGluten(plan: SparkPlan): Boolean = {
    plan match {
      case a: AdaptiveSparkPlanExec => isGluten(a.executedPlan)
      case _: GlutenPlan => true
      case s: AQEShuffleReadExec => isGluten(s.child)
      case e: ExchangeQueryStageExec => isGluten(e.plan)
      case _: ColumnarInputAdapter => true
      case _ => false
    }
  }

  def getAllChildren(plan: SparkPlan): Seq[SparkPlan] = {
    plan match {
      case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
      case e: ExchangeQueryStageExec => Seq(e.plan)
      case o => o.children
    }
  }

  case class TestException(plan: SparkPlan)
    extends Exception(s"Chain broken at ${plan.nodeName}\n$plan")

  def assertContinuousSupport(plan: SparkPlan): Boolean = {
    val childrenHasGluten =
      getAllChildren(plan).map(assertContinuousSupport).reduceOption(_ && _).getOrElse(true)
    val currentNodeHasGluten = isGluten(plan)
    (childrenHasGluten, currentNodeHasGluten) match {
      case (true, true) => true // Valid
      case (true, false) => false // Valid
      case (false, false) => false // Valid
      case (false, true) => throw TestException(plan)
    }
  }
}
