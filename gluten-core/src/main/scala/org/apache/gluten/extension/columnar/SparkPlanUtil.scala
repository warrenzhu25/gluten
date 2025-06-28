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
package org.apache.gluten.extension.columnar

import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.execution.SparkPlan

object SparkPlanUtil {

  /**
   * Similar to TreeNode.transformUp. But when traversing down the tree to reach the leaves, cond is
   * used to evaluate an early stop to the traversal. If cond is met rule starts applying to all the
   * parents from that node onwards.
   * @param plan
   *   plan to transform
   * @param cond
   *   cond is to stop traversing down the tree
   * @param rule
   *   transformation rule
   * @return
   *   transformed tree
   */
  def transformUpWithPruning(plan: SparkPlan)(cond: SparkPlan => Boolean)(
      rule: PartialFunction[SparkPlan, SparkPlan]): SparkPlan = {
    if (cond.apply(plan)) {
      return plan
    }
    val afterRuleOnChildren = plan.mapChildren(transformUpWithPruning(_)(cond)(rule))
    val newNode = if (plan.fastEquals(afterRuleOnChildren)) {
      CurrentOrigin.withOrigin(plan.origin) {
        rule.applyOrElse(plan, identity[SparkPlan])
      }
    } else {
      CurrentOrigin.withOrigin(plan.origin) {
        rule.applyOrElse(afterRuleOnChildren, identity[SparkPlan])
      }
    }
    if (plan eq newNode) {
      plan
    } else {
      // If the transform function replaces this node with a new one, carry over the tags.
      newNode.copyTagsFrom(plan)
      newNode
    }
  }
}
