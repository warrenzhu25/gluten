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
package com.google

import org.apache.spark.SparkContext

import com.cibo.evilplot.colors.HTMLNamedColors
import com.cibo.evilplot.geometry.Extent
import com.cibo.evilplot.numeric.Point
import com.cibo.evilplot.plot.{LinePlot, Overlay, Plot}
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.File
import java.lang.management.ManagementFactory
import java.net.URI

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object Monitor {
  @volatile private var initialized = false
  @volatile private var interval = 1000L

  private def initialize(customLambdas: Seq[(String, () => Any)], interval: Long): Unit = {
    if (!initialized) {
      probeLambdas ++= customLambdas
      this.interval = interval
      initialized = true
    }
    val sc = SparkContext.getOrCreate()
    sc.parallelize(1 to 100).foreachPartition {
      _ =>
        initialized.synchronized {
          if (!initialized) {
            probeLambdas ++= customLambdas
            initialized = true
          }
        }
    }
  }

  /**
   * Clear observation buffers to free up memory on the executor
   */
  def reset(): Unit = {
    val sc = SparkContext.getOrCreate()
    sc.parallelize(1 to 100).foreachPartition {
      _ =>
        observations.synchronized {
          observations.clear()
        }
    }
  }

  private def generateSvgGraph(
      lineNames: Seq[String],
      data: mutable.ArrayBuffer[(Long, Seq[Any])],
      outputFile: File
  ): Try[Unit] = Try {
    require(data.nonEmpty, "Input data cannot be empty.")
    require(data.head._2.nonEmpty, "Sequence of Y-values cannot be empty.")
    require(lineNames.size == data.head._2.size, "Line Names do not match.")

    // 1. Manually create a stable color palette
    val colorPalette = Seq(
      HTMLNamedColors.blue,
      HTMLNamedColors.red,
      HTMLNamedColors.green,
      HTMLNamedColors.orange,
      HTMLNamedColors.purple,
      HTMLNamedColors.black,
      HTMLNamedColors.cyan
    )

    // 2. Extract X-axis values
    val xValues: Seq[Double] = data.map(_._1.toDouble).toSeq

    // 3. Transpose Y-axis values
    val numLines = data.head._2.size
    val yValuesByLine: Seq[Seq[Double]] = (0 until numLines).map {
      lineIndex =>
        data.map {
          case (_, ySeq) =>
            ySeq(lineIndex).asInstanceOf[Long].toDouble / (1024 * 1024)
        }.toSeq
    }

    // 4. Create a sequence of individual LinePlots
    val allLinePlots: Seq[Plot] = yValuesByLine.zipWithIndex.map {
      case (yValues, index) =>
        val points = xValues.zip(yValues).map { case (x, y) => Point(x, y) }
        LinePlot.series(
          data = points,
          name = lineNames(index),
          color = colorPalette(index % colorPalette.length)
        )
    }

    // 5. Combine the plots using the top-level Overlay object
    val combinedPlot = Overlay(allLinePlots: _*) // Use :_* to pass a Seq to a varargs method
      .xAxis()
      .yAxis()
      .title("Time Series Graph")
      .xLabel("Time")
      .yLabel("MB")
      .bottomLegend()

    // 6. Render the final plot to the specified SVG file path
    combinedPlot.render(Extent(800, 600)).write(outputFile)
  }

  private def getObservations(afterTime: Long): Map[String, ArrayBuffer[(Long, Seq[Any])]] = {
    val sc = SparkContext.getOrCreate()
    sc.parallelize(1 to 100)
      .mapPartitions {
        _ =>
          val executorId = org.apache.spark.SparkEnv.get.executorId
          if (initialized) {
            Iterator(Seq(executorId -> observations))
          } else {
            Iterator()
          }
      }
      .collect()
      .flatten
      .groupBy(_._1)
      .map(
        x =>
          x._1 -> x._2.head._2
            .filter(_._1 >= afterTime)
            .map(o => o._1 - afterTime -> o._2))
  }

  def observe[T](f: => T): T = {
    observe(Nil, None, 1000)(f)
  }

  def observe[T](gcsPath: String)(f: => T): T = {
    observe(Nil, Some(gcsPath), 1000)(f)
  }

  def observe[T](gcsPath: String, interval: Long)(f: => T): T = {
    observe(Nil, Some(gcsPath), interval)(f)
  }

  def observe[T](customLambdas: Seq[(String, () => Any)])(f: => T): T = {
    observe(customLambdas, None, 1000)(f)
  }

  def observe[T](customLambdas: Seq[(String, () => Any)], gcsPath: String)(f: => T): T = {
    observe(customLambdas, Some(gcsPath), 1000)(f)
  }

  def observe[T](customLambdas: Seq[(String, () => Any)], gcsPath: String, interval: Long)(
      f: => T): T = {
    observe(customLambdas, Some(gcsPath), interval)(f)
  }

  private def observe[T](
      customLambdas: Seq[(String, () => Any)],
      gcsURIOption: Option[String],
      interval: Long)(f: => T): T = {
    initialize(customLambdas, interval)
    val start = System.currentTimeMillis()
    val ret = f
    val observations = getObservations(start)
    val singleNode = observations.head._2
    gcsURIOption.foreach {
      gcsURI =>
        val tempFile = File.createTempFile("out", ".png")
        generateSvgGraph(probeLambdas.map(_._1), singleNode, tempFile)
        val conf = new Configuration()
        val gcsPath = new Path(gcsURI)
        val localPath = new Path(tempFile.toURI)
        var fs: FileSystem = null
        try {
          fs = FileSystem.get(new URI(gcsURI), conf)
          fs.copyFromLocalFile(false, true, localPath, gcsPath)
          println(s"Successfully moved temporary file to $gcsURI")
        } catch {
          case e: Exception =>
            e.printStackTrace()
            println(s"Failed to move file to GCS: ${e.getMessage}")
        } finally {
          if (fs != null) {
            fs.close()
          }
          tempFile.delete() // Clean up local file
        }
    }

    val stringOutput = singleNode.map(o => s"${o._1},${o._2.mkString(",")}").mkString("\n")
    println(stringOutput)
    ret
  }

  private val observations = mutable.ArrayBuffer[(Long, Seq[Any])]()

  private val probeLambdas = mutable.ArrayBuffer[(String, () => Any)]()
  probeLambdas += "Used OH Memory" -> (
    () => {
      Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
    })
  probeLambdas += "Allocated OH Memory" -> (
    () => {
      Runtime.getRuntime.totalMemory()
    })
  probeLambdas += "Max Memory" -> (
    () => {
      Runtime.getRuntime.maxMemory()
    })
  probeLambdas += "RSS" -> (
    () => {
      import sys.process._
      val pid = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)
      val rss = (s"top -p $pid -b -n 1" #| s"grep $pid").!!.trim.split("\\s+")(5).trim
      val unitChar = rss.last
      val result = if (unitChar == 'g' || unitChar == 'm' || unitChar == 'k') {
        val numericPartStr = rss.substring(0, rss.length - 1).toDouble
        val multiplier = unitChar match {
          case 'g' => 1024.0 * 1024.0 * 1024.0
          case 'm' => 1024.0 * 1024.0
        }
        numericPartStr * multiplier
      } else {
        rss.toLong * 1024
      }
      result.toLong
    })

  private val monitorThread = new Thread(
    () => {
      while (true) {
        Thread.sleep(interval)
        if (initialized) {
          val currentTime = System.currentTimeMillis()
          val newObservation = (currentTime, probeLambdas.map(f => f._2()))
          observations += newObservation
        }
      }
    })
  monitorThread.setName("MonitorThread")
  monitorThread.setDaemon(true)
  monitorThread.start()
}
