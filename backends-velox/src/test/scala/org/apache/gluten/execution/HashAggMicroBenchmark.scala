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

import org.apache.gluten.tags.SkipTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenQueryTest
import org.apache.spark.sql.test.SharedSparkSession

// scalastyle:off line.size.limit
// scalastyle:off println
@SkipTest
class HashAggMicroBenchmark extends GlutenQueryTest with SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.executor.memory", "2GB")
      .set("spark.gluten.memory.dynamic.offHeap.sizing.enabled", "true")
      .set("spark.ui.enabled", "true")
      .set("spark.gluten.ui.enabled", "true")
  }

  def benchmark(
      numRows: Long,
      numCols: Int,
      numKeyCols: Int,
      outputRows: Long,
      mapperPartitions: Int,
      synthesizerOnly: Boolean,
      keyType: String): Map[String, Long] = {
    import org.apache.spark.sql.functions._
    import scala.collection.immutable.ListMap
    import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
    import org.apache.spark.sql.execution.ProjectExec
    import org.apache.spark.sql.execution.aggregate.HashAggregateExec
    import org.apache.spark.sql.catalyst.expressions.aggregate._
    import org.apache.spark.sql.execution.{CodegenSupport, UnaryExecNode, WholeStageCodegenExec}
    import org.apache.spark.sql.functions.rand

    if (numCols >= 1000) throw new RuntimeException(s"numCols: $numCols > 1000")
    if (numKeyCols >= 1000) throw new RuntimeException(s"numKeyCols: $numKeyCols > 1000")

    println(
      s"numRows: $numRows, numCols: $numCols, outputRows: $outputRows, numKeyCols: $numKeyCols")
    // Sample Output for originalHaDf.queryExecution.executedPlan
    // VeloxColumnarToRow
    // +- ^(1) HashAggregateTransformer(keys=[k#124L], functions=[sum(v1#126L)], isStreamingAgg=true, ignoreNullKeys=false, output=[k#124L, sum(v1)#132L])
    //    +- ^(1) HashAggregateTransformer(keys=[k#124L], functions=[partial_sum(v1#126L)], isStreamingAgg=true, ignoreNullKeys=false, output=[k#124L, sum#136L])
    //       +- ^(1) InputIteratorTransformer[k#124L, v1#126L]
    //          +- RowToVeloxColumnar
    //             +- *(1) Project [id#122L AS k#124L, cast(rand(385992894183472464) as bigint) AS v1#126L]
    //                +- *(1) Range (0, 1000000, step=1, splits=1)
    val originalHaDf = {
      val groupFunctions = (1 to numKeyCols).map(id => s"k$id")
      val aggregateFunctions = (1 to numCols).map(id => s"v$id" -> "sum")
      val dummySourceColumns =
        ((1 to numCols).map(id => s"v$id" -> rand(42).cast("bigint")) ++ (1 to numKeyCols).map(
          id => s"k$id" -> rand(42).cast("bigint").cast(keyType))).toMap
      spark
        .range(0, numRows, 1, mapperPartitions)
        .toDF("z")
        .withColumns(dummySourceColumns)
        .drop("z")
        .groupBy(groupFunctions.head, groupFunctions.tail: _*)
        .agg(aggregateFunctions.head, aggregateFunctions.tail: _*)
    }

    // Return projection List to Map Attributes from -> to
    def addAlias(from: Seq[Attribute], to: Seq[Attribute]): Seq[Alias] = {
      val projectionList = from.sortBy(_.name).sortBy(_.name).zip(to.sortBy(_.name)).map {
        case (from0, to0) =>
          Alias(
            AttributeReference(from0.name, from0.dataType, from0.nullable, from0.metadata)(
              from0.exprId,
              from0.qualifier),
            to0.name
          )(to0.exprId)
      }
      projectionList
    }

    val projections =
      (1 to numKeyCols).map(_ => col("k").cast(keyType)) ++ // Projections for Group By Keys
        Seq.fill(numCols)((rand(420) * numRows).cast("bigint")) // Projections for Aggregate Values

    val originalHaSparkPlan = originalHaDf.queryExecution.executedPlan
    // println(originalHaSparkPlan)
    // Sample output for synthesizedInputPlan
    // *(1) Project [cast((rand(420) * 10000.0) as bigint) AS 000#141L, cast((rand(420) * 1000000.0) as bigint) AS 001#142L]
    // +- *(1) Range (0, 1000000, step=1, splits=1)
    val synthesizedInputPlan = {
      val projectionsWithName = ListMap(projections.zipWithIndex.map {
        case (col, idx) => f"$idx%03d" -> col
      }: _*)
      spark
        .range(0, numRows, 1, mapperPartitions)
        .toDF("id")
        .withColumn(
          "k",
          (rand(420) * outputRows).cast("bigint")
        ) // If outputRows 10000, [0.0,1.0] -> [0.0,10000.0] -> [0,10000]
        .withColumns(projectionsWithName)
        .drop("k")
        .drop("id")
        .queryExecution
        .executedPlan
    }

    // println(synthesizedInputPlan)

    // This removed wrapper WSCGExec operator and directly gets ProjectExec
    val trimmedSynthesizedInputPlan = synthesizedInputPlan.find(_.isInstanceOf[ProjectExec]).get

    // Sample Output for finalExecutionBenchmarkPlan
    // ^(1) HashAggregateTransformer(keys=[k#124L], functions=[sum(v1#126L)], isStreamingAgg=false, ignoreNullKeys=false, output=[k#124L, sum(v1)#132L])
    // +- ^(1) InputIteratorTransformer[k#124L, sum#136L]
    //    +- RowToVeloxColumnar
    //       +- *(1) Project [0#141L AS k#124L, 1#142L AS sum#136L]
    //          +- *(1) Project [cast((rand(420) * 10000.0) as bigint) AS 0#141L, cast((rand(420) * 1000000.0) as bigint) AS 1#142L]
    //             +- *(1) Range (0, 1000000, step=1, splits=1)
    val planToTest = originalHaSparkPlan.find {

      // Change this for Different Operator
      case ghat: org.apache.gluten.execution.RegularHashAggregateExecTransformer
          if ghat.aggregateExpressions.forall(m => m.mode == Final || m.mode == Complete) =>
        true
      case gha: HashAggregateExec
          if gha.aggregateExpressions.forall(m => m.mode == Final || m.mode == Complete) =>
        true

      // case ghat: org.apache.gluten.execution.RegularHashAggregateExecTransformer if ghat.aggregateExpressions.forall(m => m.mode == Partial || m.mode == PartialMerge) => true
      // case ghat: org.apache.gluten.execution.FlushableHashAggregateExecTransformer if ghat.aggregateExpressions.forall(m => m.mode == Partial || m.mode == PartialMerge) => true
      // case gha: HashAggregateExec if gha.aggregateExpressions.forall(m => m.mode == Partial || m.mode == PartialMerge) => true

      case _ => false
    }.get
    val finalExecutionBenchmarkPlan = if (synthesizerOnly) {
      planToTest match {
        case gp: org.apache.gluten.execution.UnaryTransformSupport =>
          val projectionList = addAlias(trimmedSynthesizedInputPlan.output, gp.child.output)
          org.apache.gluten.execution.WholeStageTransformer(
            org.apache.spark.sql.execution.InputIteratorTransformer(
              org.apache.spark.sql.execution.ColumnarInputAdapter(
                org.apache.gluten.execution.RowToVeloxColumnarExec(WholeStageCodegenExec(
                  ProjectExec(projectionList, trimmedSynthesizedInputPlan))(0)))))(1)
        case sp: CodegenSupport with UnaryExecNode =>
          val projectionList = addAlias(trimmedSynthesizedInputPlan.output, sp.child.output)
          WholeStageCodegenExec(ProjectExec(projectionList, trimmedSynthesizedInputPlan))(0)
      }
    } else {
      planToTest match {
        case gp: org.apache.gluten.execution.UnaryTransformSupport =>
          val projectionList = addAlias(trimmedSynthesizedInputPlan.output, gp.child.output)
          org.apache.gluten.execution.WholeStageTransformer(
            gp.withNewChildren(
              Seq(
                org.apache.spark.sql.execution.InputIteratorTransformer(
                  org.apache.spark.sql.execution.ColumnarInputAdapter(
                    org.apache.gluten.execution.RowToVeloxColumnarExec(WholeStageCodegenExec(
                      ProjectExec(projectionList, trimmedSynthesizedInputPlan))(0)))))))(1)
        case sp: CodegenSupport with UnaryExecNode =>
          val projectionList = addAlias(trimmedSynthesizedInputPlan.output, sp.child.output)
          WholeStageCodegenExec(
            sp.withNewChildren(Seq(ProjectExec(projectionList, trimmedSynthesizedInputPlan))))(0)
        case s: org.apache.spark.sql.execution.ColumnarShuffleExchangeExec =>
          val projectionList = addAlias(trimmedSynthesizedInputPlan.output, s.child.output)
          s.withNewChildren(
            Seq(
              org.apache.gluten.execution.VeloxResizeBatchesExec(
                org.apache.gluten.execution.RowToVeloxColumnarExec(WholeStageCodegenExec(
                  ProjectExec(projectionList, trimmedSynthesizedInputPlan))(0)),
                1024,
                Int.MaxValue
              )))
      }
    }

    println(finalExecutionBenchmarkPlan)

    val start = System.nanoTime()
    val sqlMetrics = {
      finalExecutionBenchmarkPlan match {
        case plan: org.apache.gluten.execution.GlutenPlan if plan.supportsColumnar =>
          finalExecutionBenchmarkPlan.executeColumnar().foreach(identity)
        case _ =>
          finalExecutionBenchmarkPlan.execute().foreach(identity)
      }
      finalExecutionBenchmarkPlan.children.head.metrics.map(m => m._1 -> m._2.value)
    }
    val end = System.nanoTime()
    val timeTaken = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(end - start)

    // This code gets diskBytesSpilled and memoryBytesSpilled via reflection for the latest stage run
    val taskMetrics = {
      val sc = spark.sparkContext
      import java.lang.reflect.{Field, Method}
      import scala.collection.JavaConverters._
      val uiField: Field = sc.getClass.getDeclaredField("_ui")
      uiField.setAccessible(true)
      val uiOption = uiField.get(sc).asInstanceOf[Option[Any]]
      val sparkUI_Object: Any = uiOption.get
      val sparkUI_Class = sparkUI_Object.getClass
      val storeField: Field = sparkUI_Class.getDeclaredField("store")
      storeField.setAccessible(true)
      val store_Object: Any = storeField.get(sparkUI_Object)
      val storeClass = store_Object.getClass
      val stageDataParameterTypes: Array[Class[_]] = Array(
        classOf[Int],
        classOf[Boolean],
        classOf[java.util.List[org.apache.spark.status.api.v1.TaskStatus]],
        classOf[Boolean],
        classOf[Array[Double]]
      )
      val stageDataMethod: Method =
        storeClass.getDeclaredMethod("stageData", stageDataParameterTypes: _*)
      stageDataMethod.setAccessible(true)
      val stageListParameterTypes: Array[Class[_]] = Array(
        classOf[java.util.List[org.apache.spark.status.api.v1.StageStatus]],
        classOf[Boolean],
        classOf[Boolean],
        classOf[Array[Double]],
        classOf[java.util.List[org.apache.spark.status.api.v1.TaskStatus]]
      )
      val stageListMethod: Method =
        storeClass.getDeclaredMethod("stageList", stageListParameterTypes: _*)
      stageListMethod.setAccessible(true)
      val stageListArgs: Array[AnyRef] = Array(
        null,
        java.lang.Boolean.FALSE,
        java.lang.Boolean.FALSE,
        Array.empty[Double],
        List[org.apache.spark.status.api.v1.TaskStatus]().asJava
      )
      val stageListResult = stageListMethod
        .invoke(store_Object, stageListArgs: _*)
        .asInstanceOf[Seq[org.apache.spark.status.api.v1.StageData]]
      val myStageId: Int = stageListResult.head.stageId
      val stageDataArgs: Array[AnyRef] = Array(
        myStageId.asInstanceOf[AnyRef],
        java.lang.Boolean.FALSE,
        List[org.apache.spark.status.api.v1.TaskStatus]().asJava,
        java.lang.Boolean.FALSE,
        Array.empty[Double]
      )
      val stageDataResult = stageDataMethod
        .invoke(store_Object, stageDataArgs: _*)
        .asInstanceOf[Seq[org.apache.spark.status.api.v1.StageData]]
      val stage = stageDataResult.head
      Seq(
        "diskBytesSpilled" -> stage.diskBytesSpilled,
        "peakExecutionMemory" -> stage.peakExecutionMemory,
        "memoryBytesSpilled" -> stage.memoryBytesSpilled,
        "timeTakenMs" -> timeTaken
      )
    }
    val allMetrics = sqlMetrics ++ taskMetrics
    allMetrics
  }

  def toCsv(data: Map[String, Long]): String = {
    toCsv(Seq(data), Nil)
  }

  def toCsv(data: Seq[Map[String, Long]], headers: Seq[Int]): String = {
    val keys = data.flatMap(_.keys).distinct.sorted
    val headerRow = if (headers.nonEmpty) {
      Seq("input" + headers.mkString(",", ",", ""))
    } else {
      Seq.empty[String]
    }
    val dataRows = keys.map {
      key =>
        val values = data.map(_.get(key).map(_.toString).getOrElse("N/A")).mkString(",")
        s"$key,$values"
    }
    (headerRow ++ dataRows).mkString("\n")
  }

  test("Micro Benchmark") {
    withSQLConf(
      "spark.gluten.enabled" -> "false",
      "spark.sql.adaptive.enabled" -> "false",
      "spark.gluten.sql.columnar.project" -> "false",
      "spark.gluten.sql.columnar.range" -> "false",
      "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation" -> "false",
      "spark.gluten.sql.columnar.backend.velox.spillPrefixsortEnabled" -> "true"
    ) {
      println(
        toCsv(
          benchmark(
            numRows = 1000,
            numCols = 1,
            numKeyCols = 1,
            outputRows = 10,
            mapperPartitions = 1,
            synthesizerOnly = false,
            keyType = "string")))
      // benchmark(numRows = 1000, numCols = 1, numKeyCols = 1, outputRows = 10, mapperPartitions = 1, synthesizerOnly = false, keyType = "bigint")
      // benchmark(numRows = 1000000000, numCols = 1, numKeyCols = 1, outputRows = 100000, mapperPartitions = 1, synthesizerOnly = false, keyType = "bigint")
      // benchmark(numRows = 1000000000, numCols = 1, numKeyCols = 1, outputRows = 100000, mapperPartitions = 1, synthesizerOnly = false, keyType = "string")
      // benchmark(numRows = 100000000, numCols = 1, numKeyCols = 2, outputRows = _, mapperPartitions = 1, synthesizerOnly = false)
      // benchmark(1000000000, 1, 1, 10000, 1, synthesizerOnly = false)

      /*
      {
        // spark.gluten.sql.columnar.shuffle
        val numRows = 10000000000L
        val partition = 100
        val numCols = 1
        val aggregateFunctions = (1 to numCols).map(id => s"v$id" -> "sum")
        val dummySourceColumns = ((1 to numCols).map(id => s"v$id" -> rand().cast("bigint")) ++ Seq("k" -> rand().cast("bigint"))).toMap
        spark
          .range(0, numRows, 1, partition)
          .toDF("k2")
          .withColumns(dummySourceColumns)
          .drop("k2")
          .groupBy("k")
          .agg(aggregateFunctions.head, aggregateFunctions.tail: _*)
      }
      {
        val input = Seq(1000000, 10000000, 100000000, 1000000000)
        benchmark(input.head, 1, 1, 10000, 1, synthesizerOnly = false)
        println(toCsv(input.map(benchmark(_, 1, 1, 10000, 1, synthesizerOnly = false)), input))
        println(toCsv(input.map(benchmark(_, 1, 1, 10000, 1, synthesizerOnly = true)), input))
      }
      {
        val input = Seq(10000,100000,1000000,2000000,4000000,8000000,10000000,20000000, 30000000, 40000000, 50000000)
        // val input = Seq(10000, 100000, 1000000, 2000000, 4000000)
        // val input = Seq(8000000,10000000)
        // val input = Seq(20000000, 30000000, 40000000, 50000000)
        benchmark(100000000, 1, 2, 10000, 1, synthesizerOnly = false)
        println(toCsv(input.map(benchmark(100000000, 1, 1, _, 1, synthesizerOnly = false)), input))
        println(toCsv(input.map(benchmark(100000000, 1, 1, _, 1, synthesizerOnly = true)), input))
      }
      {
        val input = Seq(1,20,40,60,80, 100)
        benchmark(100000000, 1, 10000, synthesizerOnly = false)
        println(toCsv(input.map(benchmark(100000000, _, 4000000, synthesizerOnly = false)), input))
        println(toCsv(input.map(benchmark(100000000, _, 4000000, synthesizerOnly = true)), input))
      }
       */

      /*
      {
        val input = Seq(1,20,40,60)
        benchmark(100000000, 1, 10000, synthesizerOnly = false)
        println(toCsv(input.map(benchmark(100000000, _, 30000000, synthesizerOnly = false)), input))
        println(toCsv(input.map(benchmark(100000000, _, 30000000, synthesizerOnly = true)), input))
      }
       */

      /*
      {
        // val input = Seq(1,5,10,15,20)
        val input = Seq(25,30)
        benchmark(100000000, 1, 1, 1000000, 1, synthesizerOnly = false)
        println(toCsv(input.map(benchmark(100000000, 1, _, 1000000, 1, synthesizerOnly = false)), input))
        println(toCsv(input.map(benchmark(100000000, 1, _, 1000000, 1, synthesizerOnly = true)), input))
      }
       */

      // benchmark(100000000, 1, 25, 1000000, 1, synthesizerOnly = false)
    }
  }
}
// scalastyle:on line.size.limit
// scalastyle:on println
