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

import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.SparkPlanUtil.transformUpWithPruning
import org.apache.gluten.extension.columnar.heuristic.{AddFallbackTags, RewriteSparkPlanRulesManager}
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.rewrite.RewriteSingleNode
import org.apache.gluten.extension.columnar.validator.Validator

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * This Rule rewrites, validates and offloads sequentially for each node from bottom to top. If any
 * node is not offloaded, all parents of that node will also not be offloaded. This is unlike
 * HeuristicTransform.WithRewrites, which applies rewrite rule to entire tree, and then validate
 * rule to the entire rewritten tree and finally offload rule to the entire validated tree.
 */
case class PessimisticTransformer(
    validator: Validator,
    rewriteRules: Seq[RewriteSingleNode],
    offloadRules: Seq[OffloadSingleNode])
  extends Rule[SparkPlan] {
  private val validate = AddFallbackTags(validator)
  private val pessimisticValidate = AddFallbackTags(validator.andThen(PessimisticValidator))
  // RewriteSparkPlanRulesManager can replace 1 node with n nodes where n >= 1
  // After rewriting it does validation of all the new nodes without offloading the
  // validated nodes. This can cause an issue when n > 1 as PessimisticValidator will
  // mark the new nodes as not supported as their children is not yet offloaded to Gluten.
  // To avoid this we skip PessimisticValidator in the rewrite validation and perform it later.
  private val rewrite = new RewriteSparkPlanRulesManager(validate, rewriteRules)

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case p =>
        val rewrittenPlan = rewrite.rewrite.applyOrElse(p, identity[SparkPlan])
        if (rewrittenPlan.fastEquals(p)) {
          // No rewrite, validate and offload single node
          pessimisticValidate.addFallbackTag(p)
          offloadRules.foldLeft(p)((p, rule) => rule.offload(p))
        } else {
          // Rewrite has occurred, validate and offload all the new nodes.
          val childrenBeforeRewrite = p.children
          var rewriteFailReason: Option[String] = None
          val transformedRewrite = transformUpWithPruning(rewrittenPlan) {
            p1 =>
              // Do not touch the children as they have already been validated and offloaded
              childrenBeforeRewrite.exists(_.fastEquals(p1))
          } {
            case p1 =>
              pessimisticValidate.addFallbackTag(p1)
              // If any 1 of the new rewritten nodes fails validation, the original plan is
              // returned.
              val currentNodeReason = FallbackTags.getOption(p1).map(_.reason())
              rewriteFailReason = (rewriteFailReason, currentNodeReason) match {
                case (Some(s), _) => Some(s)
                case (None, Some(s)) => Some(s)
                case _ => None
              }
              if (rewriteFailReason.isDefined) {
                p1
              } else {
                offloadRules.foldLeft(p1)((p, rule) => rule.offload(p))
              }
          }
          rewriteFailReason
            .map {
              reason =>
                FallbackTags.add(p, reason)
                p
            }
            .getOrElse(transformedRewrite)
        }
    }
  }
}
