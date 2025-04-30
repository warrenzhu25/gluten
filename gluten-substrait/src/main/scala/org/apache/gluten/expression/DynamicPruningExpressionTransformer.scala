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
package org.apache.gluten.expression

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.substrait.expression.ExpressionNode

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, DynamicPruningExpression, EqualTo, Expression, InSet, Literal}
import org.apache.spark.sql.execution.InSubqueryExec

case class DynamicPruningExpressionTransformer(
    substraitExprName: String,
    dp: DynamicPruningExpression,
    attributeSeq: Seq[Attribute]
) extends LeafExpressionTransformer
  with Logging {
  // Spark collects all the ExecSubqueryExpression first,
  // executes them before executing the main query
  override def original: Expression = dp

  override def doTransform(args: Object): ExpressionNode = {
    if (TransformerState.underValidationState) {
      return getTrueExpression().doTransform(args)
    }

    // If DynamicPruningExpression is an InSubquery, is materialized and
    // number of rows is less than threshold, transform the expression using InSet
    // otherwise return true literal
    val inSubqueryExec = dp.child.asInstanceOf[InSubqueryExec]
    if (inSubqueryExec.values().isDefined) {
      val valueSet = inSubqueryExec.values().get.toSet
      if (valueSet.size <= GlutenConfig.get.dataprocRuntimeInFilterRowCountThreshold) {
        val inSet = InSet(inSubqueryExec.child, inSubqueryExec.values.get.toSet)
        InSetTransformer(
          substraitExprName,
          ExpressionConverter.replaceWithExpressionTransformer(inSubqueryExec.child, attributeSeq),
          inSet).doTransform(args)
      } else {
        getTrueExpression().doTransform(args)
      }
    } else {
      // it should never reach here, putting a fallback to not cause failure
      getTrueExpression().doTransform(args)
    }
  }

  private def getTrueExpression(): ExpressionTransformer = {
    // Simple true fails in velox. Therefore return true = true
    ExpressionConverter.replaceWithExpressionTransformer(
      EqualTo(Literal(true), Literal(true)),
      attributeSeq
    )
  }
}
