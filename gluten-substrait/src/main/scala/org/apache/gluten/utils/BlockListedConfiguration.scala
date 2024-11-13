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
package org.apache.gluten.utils

import org.apache.gluten.extension.ValidationResult

import org.apache.spark.sql.SparkSession

import collection.JavaConverters._

import scala.util.matching.Regex

sealed trait BlockListedConfiguration {
  def validate(session: SparkSession): ValidationResult
}

/** Used to represent a Spark / SparkSQL configuration. */
case class BlockListedSparkConfiguration(
    key: String,
    value: Value
) extends BlockListedConfiguration {
  override def validate(session: SparkSession): ValidationResult = {
    session.conf
      .getOption(key)
      .filter(value.matches)
      .map(v => ValidationResult.failed(s"$key with $v is not supported"))
      .getOrElse(ValidationResult.succeeded)
  }
}

/**
 * Used to represent a Hadoop configuration. These are either set in yarn-site.xml or by adding
 * `spark.hadoop` prefix.
 */
case class BlockListedHadoopConfiguration(
    key: String,
    value: Value
) extends BlockListedConfiguration {
  override def validate(session: SparkSession): ValidationResult = {
    Option(session.sparkContext.hadoopConfiguration.get(key))
      .filter(value.matches)
      .map(v => ValidationResult.failed(s"$key with $v is not supported"))
      .getOrElse(ValidationResult.succeeded)
  }
}

/**
 * Represents a Hadoop Configuration Prefix. This matches against all hadoop configurations starting
 * with a given prefix.
 */
case class BlockListedHadoopConfigurationPrefix(
    prefix: String,
    value: Value
) extends BlockListedConfiguration {
  override def validate(session: SparkSession): ValidationResult = {
    session.sparkContext.hadoopConfiguration
      .getPropsWithPrefix(prefix)
      .asScala
      .find(kv => value.matches(kv._2))
      .map(kv => ValidationResult.failed(s"${kv._1} with ${kv._2} is not supported"))
      .getOrElse(ValidationResult.succeeded)
  }
}

sealed trait Value {
  def matches(value: String): Boolean
}

object ANY extends Value {
  override def matches(value: String): Boolean = true
}

sealed trait MatchPattern {
  def matches(s: String): Boolean
}

case class ValueLiteral(s: String) extends MatchPattern {
  def matches(s2: String): Boolean = s.equals(s2)
}

case class RegexPattern(regex: Regex) extends MatchPattern {
  def matches(s2: String): Boolean = regex.pattern.matcher(s2).matches()
}

case class OneOf(values: Seq[MatchPattern]) extends Value {
  override def matches(value: String): Boolean = values.exists(_.matches(value))
}

object OneOf {
  def apply(value: String): OneOf = OneOf(Seq(ValueLiteral(value)))
}

case class AnyExcept(values: Seq[MatchPattern]) extends Value {
  override def matches(value: String): Boolean = !values.exists(_.matches(value))
}

object AnyExcept {
  def apply(value: String): AnyExcept = AnyExcept(Seq(ValueLiteral(value)))
}
