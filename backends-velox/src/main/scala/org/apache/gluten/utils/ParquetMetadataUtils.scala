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
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat.ParquetReadFormat

import org.apache.spark.util.SerializableConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

object ParquetMetadataUtils {

  /**
   * Validates whether Parquet encryption is enabled for the given paths.
   *
   *   - If the file format is not Parquet, skip this check and return success.
   *   - If there is at least one Parquet file with encryption enabled, fail the validation.
   *
   * @param format
   *   File format, e.g., `ParquetReadFormat`
   * @param rootPaths
   *   List of file paths to scan
   * @param serializableHadoopConf
   *   Optional Hadoop configuration
   * @return
   *   [[ValidationResult]] validation success or failure
   */
  def validateEncryption(
      format: ReadFileFormat,
      rootPaths: Seq[String],
      serializableHadoopConf: Option[SerializableConfiguration],
      fileLimit: Int
  ): ValidationResult = {
    if (format != ParquetReadFormat || rootPaths.isEmpty) {
      return ValidationResult.succeeded
    }

    val conf = serializableHadoopConf.map(_.value).getOrElse(new Configuration())

    rootPaths.foreach {
      rootPath =>
        val fs = new Path(rootPath).getFileSystem(conf)
        try {
          val encryptionDetected =
            checkForEncryptionWithLimit(fs, new Path(rootPath), conf, fileLimit = fileLimit)
          if (encryptionDetected) {
            return ValidationResult.failed("Encrypted Parquet file detected.")
          }
        } catch {
          case e: Exception =>
        }
    }
    ValidationResult.succeeded
  }

  /**
   * Validates whether Parquet encoding is supported for the given paths.
   *
   *   - If the file format is not Parquet, skip this check and return success.
   *   - If there is at least one Parquet file with unsupported encoding enabled, fail the
   *     validation.
   *
   * @param format
   *   File format, e.g., `ParquetReadFormat`
   * @param rootPaths
   *   List of file paths to scan
   * @param serializableHadoopConf
   *   Optional Hadoop configuration
   * @return
   *   [[ValidationResult]] validation success or failure
   */
  def validateEncoding(
      format: ReadFileFormat,
      rootPaths: Seq[String],
      serializableHadoopConf: Option[SerializableConfiguration],
      fileLimit: Int
  ): ValidationResult = {
    if (format != ParquetReadFormat || rootPaths.isEmpty) {
      return ValidationResult.succeeded
    }

    val conf = serializableHadoopConf.map(_.value).getOrElse(new Configuration())

    rootPaths.foreach {
      rootPath =>
        val fs = new Path(rootPath).getFileSystem(conf)
        try {
          val unsupportedEncodingDetected =
            checkForUnsupportedEncodingWithLimit(
              fs,
              new Path(rootPath),
              conf,
              fileLimit = fileLimit)
          if (unsupportedEncodingDetected) {
            return ValidationResult.failed("Unsupported encoding detected.")
          }
        } catch {
          case e: Exception =>
        }
    }
    ValidationResult.succeeded
  }

  /**
   * Validates whether Parquet codec is supported for the given paths.
   *
   *   - If the file format is not Parquet, skip this check and return success.
   *   - If there is at least one Parquet file with unsupported codec enabled, fail the validation.
   *
   * @param format
   *   File format, e.g., `ParquetReadFormat`
   * @param rootPaths
   *   List of file paths to scan
   * @param serializableHadoopConf
   *   Optional Hadoop configuration
   * @return
   *   [[ValidationResult]] validation success or failure
   */
  def validateCodec(
      format: ReadFileFormat,
      rootPaths: Seq[String],
      serializableHadoopConf: Option[SerializableConfiguration],
      fileLimit: Int
  ): ValidationResult = {
    if (format != ParquetReadFormat || rootPaths.isEmpty) {
      return ValidationResult.succeeded
    }

    val conf = serializableHadoopConf.map(_.value).getOrElse(new Configuration())

    rootPaths.foreach {
      rootPath =>
        val fs = new Path(rootPath).getFileSystem(conf)
        try {
          val unsupportedCodecDetected =
            checkForUnsupportedCodecWithLimit(fs, new Path(rootPath), conf, fileLimit = fileLimit)
          if (unsupportedCodecDetected) {
            return ValidationResult.failed("Unsupported codec detected.")
          }
        } catch {
          case e: Exception =>
        }
    }
    ValidationResult.succeeded
  }

  /**
   * Validates whether Parquet metadata contains rebase info for the given paths.
   *
   *   - If the file format is not Parquet, skip this check and return success.
   *   - If there is at least one Parquet file with rebase metadata defined, fail the validation.
   *
   * @param format
   *   File format, e.g., `ParquetReadFormat`
   * @param rootPaths
   *   List of file paths to scan
   * @param serializableHadoopConf
   *   Optional Hadoop configuration
   * @return
   *   [[ValidationResult]] validation success or failure
   */
  def validateRebaseMetadata(
      format: ReadFileFormat,
      rootPaths: Seq[String],
      serializableHadoopConf: Option[SerializableConfiguration],
      fileLimit: Int
  ): ValidationResult = {
    if (format != ParquetReadFormat || rootPaths.isEmpty) {
      return ValidationResult.succeeded
    }

    val conf = serializableHadoopConf.map(_.value).getOrElse(new Configuration())

    rootPaths.foreach {
      rootPath =>
        val fs = new Path(rootPath).getFileSystem(conf)
        try {
          val rebaseMetadataDetected =
            checkForRebaseMetadataWithLimit(fs, new Path(rootPath), conf, fileLimit = fileLimit)
          if (rebaseMetadataDetected) {
            return ValidationResult.failed("Rebase metadata detected.")
          }
        } catch {
          case e: Exception =>
        }
    }
    ValidationResult.succeeded
  }

  /**
   * Check any Parquet file under the given path is encrypted using a recursive iterator. Only the
   * first `fileLimit` files are processed for efficiency.
   *
   * @param fs
   *   FileSystem to use
   * @param path
   *   Root path to check
   * @param conf
   *   Hadoop configuration
   * @param fileLimit
   *   Maximum number of files to inspect
   * @return
   *   True if an encrypted file is detected, false otherwise
   */
  private def checkForEncryptionWithLimit(
      fs: FileSystem,
      path: Path,
      conf: Configuration,
      fileLimit: Int
  ): Boolean = {

    val filesIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, true)
    var checkedFileCount = 0
    while (filesIterator.hasNext && checkedFileCount < fileLimit) {
      val fileStatus = filesIterator.next()
      checkedFileCount += 1
      if (SparkShimLoader.getSparkShims.isParquetFileEncrypted(fileStatus, conf)) {
        return true
      }
    }
    false
  }

  /**
   * Check any Parquet file under the given path has unsupported Encoding using a recursive
   * iterator. Only the first `fileLimit` files are processed for efficiency.
   *
   * @param fs
   *   FileSystem to use
   * @param path
   *   Root path to check
   * @param conf
   *   Hadoop configuration
   * @param fileLimit
   *   Maximum number of files to inspect
   * @return
   *   True if an unsupported file encoding is detected, false otherwise
   */
  private def checkForUnsupportedEncodingWithLimit(
      fs: FileSystem,
      path: Path,
      conf: Configuration,
      fileLimit: Int
  ): Boolean = {

    val filesIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, true)
    var checkedFileCount = 0
    while (filesIterator.hasNext && checkedFileCount < fileLimit) {
      val fileStatus = filesIterator.next()
      checkedFileCount += 1
      if (SparkShimLoader.getSparkShims.isUnsupportedEncodingForParquetFile(fileStatus, conf)) {
        return true
      }
    }
    false
  }

  /**
   * Check any Parquet file under the given path has unsupported codec using a recursive iterator.
   * Only the first `fileLimit` files are processed for efficiency.
   *
   * @param fs
   *   FileSystem to use
   * @param path
   *   Root path to check
   * @param conf
   *   Hadoop configuration
   * @param fileLimit
   *   Maximum number of files to inspect
   * @return
   *   True if an unsupported file codec is detected, false otherwise
   */
  private def checkForUnsupportedCodecWithLimit(
      fs: FileSystem,
      path: Path,
      conf: Configuration,
      fileLimit: Int
  ): Boolean = {

    val filesIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, true)
    var checkedFileCount = 0
    while (filesIterator.hasNext && checkedFileCount < fileLimit) {
      val fileStatus = filesIterator.next()
      checkedFileCount += 1
      if (SparkShimLoader.getSparkShims.isUnsupportedCodecForParquetFile(fileStatus, conf)) {
        return true
      }
    }
    false
  }

  /**
   * Check any Parquet file under the given path has datetime rebase metadata using a recursive
   * iterator. Only the first `fileLimit` files are processed for efficiency.
   *
   * @param fs
   *   FileSystem to use
   * @param path
   *   Root path to check
   * @param conf
   *   Hadoop configuration
   * @param fileLimit
   *   Maximum number of files to inspect
   * @return
   *   True if rebase metadata is detected, false otherwise
   */
  private def checkForRebaseMetadataWithLimit(
      fs: FileSystem,
      path: Path,
      conf: Configuration,
      fileLimit: Int
  ): Boolean = {

    val filesIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, true)
    var checkedFileCount = 0
    while (filesIterator.hasNext && checkedFileCount < fileLimit) {
      val fileStatus = filesIterator.next()
      checkedFileCount += 1
      if (SparkShimLoader.getSparkShims.containsDatetimeRebaseMetadata(fileStatus, conf)) {
        return true
      }
    }
    false
  }
}
