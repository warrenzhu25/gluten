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

package org.apache.gluten.component

import org.apache.gluten.backendsapi.velox.VeloxBackend
import org.apache.gluten.extension.BigQueryPreTransformRules
import org.apache.gluten.extension.injector.Injector
import org.apache.spark.internal.Logging

import scala.util.Try

class VeloxBigQueryComponent extends Component with Logging {
  /** Base information. */
  override def name(): String = "velox-bigquery"

  override def buildInfo(): Component.BuildInfo = Component.BuildInfo("VeloxBigQuery", "N/A", "N/A", "N/A")

  override def dependencies(): Seq[Class[_ <: Component]] = classOf[VeloxBackend] :: Nil

  /** Query planner rules. */
  override def injectRules(injector: Injector): Unit = {
    val bigQueryClassName = "com.google.cloud.spark.bigquery.v2.Spark31BigQueryScanBuilder"
    val isBigQueryAvailable = Try(Class.forName(bigQueryClassName)).isSuccess

    if (isBigQueryAvailable) {
      val legacy = injector.gluten.legacy
      val ras = injector.gluten.ras
      BigQueryPreTransformRules.rules.foreach {
        r =>
          legacy.injectPreTransform(_ => r)
          ras.injectPreTransform(_ => r)
      }
    } else {
      logWarning(s"BigQuery connector class '$bigQueryClassName' not found. " +
        "VeloxBigQueryComponent rules will not be injected.")
    }
  }
}
