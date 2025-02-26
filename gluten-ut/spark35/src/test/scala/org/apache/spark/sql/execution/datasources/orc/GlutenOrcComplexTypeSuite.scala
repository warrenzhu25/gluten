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
package org.apache.spark.sql.execution.datasources.orc

import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, QueryTest, Row}


class GlutenOrcComplexTypeSuite extends QueryTest with GlutenSQLTestsBaseTrait {

  testGluten("Test Array<int> type") {
    withTempPath { path =>
      val arr = ArrayType(IntegerType)
      val rw = Row(Seq(1,2,3))
      val data = sparkContext.parallelize(Seq(rw))
      val df = spark.createDataFrame(data,new StructType().add("arr", arr))
      df.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), rw)
    }
  }

  testGluten("Test Struct<int, string> type") {
    withTempPath { path =>
      val st = StructType(
        StructField("int", IntegerType) ::
        StructField("str", StringType) :: Nil
      )
      val rw = Row(Row(1,"hi"))
      val data = sparkContext.parallelize(Seq(rw))
      val df = spark.createDataFrame(data,new StructType().add("struct", st))
      df.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), rw)
    }
  }

  testGluten("Test Map<int,string> type") {
    withTempPath { path =>
      val mp = MapType(IntegerType, StringType)
      val rw = Row(Map((1,"hi")))
      val data = sparkContext.parallelize(Seq(rw))
      val df = spark.createDataFrame(data,new StructType().add("mp", mp))
      df.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), rw)
    }
  }

  //Falls back to Spark
  testGluten("Test Array<Array<int>> type") {
    withTempPath { path =>
      val arr = ArrayType(ArrayType(IntegerType))
      val rw = Row(Seq(Seq(1,4),Seq(1,2),Seq(1,3)))
      val data = sparkContext.parallelize(Seq(rw))
      val df = spark.createDataFrame(data,new StructType().add("arr", arr))
      df.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), rw)
    }
  }

  //Falls back to Spark
  testGluten("Test Array<Struct<int,String>> type") {
    withTempPath { path =>
      val arr = ArrayType(StructType(
        StructField("int", IntegerType) ::
          StructField("string", StringType) :: Nil
      ))
      val rw = Row(Seq(Row(1,"a"), Row(2,"b")))
      val data = sparkContext.parallelize(Seq(rw))
      val df = spark.createDataFrame(data,new StructType().add("arr", arr))
      df.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), rw)
    }
  }

  testGluten("Test Array<Map<int,String>> type") {
    withTempPath { path =>
      val arr = ArrayType(MapType(IntegerType, StringType))
      val rw = Row(Seq(Map((1,"a"),(1,"b")), Map((2,"c"), (3,"d"))))
      val data = sparkContext.parallelize(Seq(rw))
      val df = spark.createDataFrame(data,new StructType().add("arr", arr))
      df.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), rw)
    }
  }

  testGluten("Test Struct<Array<int>, string> type") {
    withTempPath { path =>
      val st = StructType(
        StructField("arr", ArrayType(IntegerType)) ::
          StructField("str", StringType) :: Nil
      )
      val rw = Row(Row(Seq(1,2),"hi"))
      val data = sparkContext.parallelize(Seq(rw))
      val df = spark.createDataFrame(data,new StructType().add("struct", st))
      df.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), rw)
    }
  }

  testGluten("Test Struct<Struct<int,int>, string> type") {
    withTempPath { path =>
      val st = StructType(
        StructField("strt", StructType(
          StructField("int1", IntegerType) ::
          StructField("int2", IntegerType) :: Nil
        )) ::
          StructField("str", StringType) :: Nil
      )
      val rw = Row(Row(Row(1,2),"hi"))
      val data = sparkContext.parallelize(Seq(rw))
      val df = spark.createDataFrame(data,new StructType().add("struct", st))
      df.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), rw)
    }
  }

  testGluten("Test Struct<Map<int,int>, string> type") {
    withTempPath { path =>
      val st = StructType(
        StructField("mp", MapType(IntegerType, IntegerType)) ::
          StructField("str", StringType) :: Nil
      )
      val rw = Row(Row(Map((1,1),(1,2)),"hi"))
      val data = sparkContext.parallelize(Seq(rw))
      val df = spark.createDataFrame(data,new StructType().add("struct", st))
      df.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), rw)
    }
  }

  testGluten("Test Map<Array<int>,string> type") {
    withTempPath { path =>
      val mp = MapType(ArrayType(IntegerType), StringType)
      val rw = Row(Map((Seq(1,2),"hi")))
      val data = sparkContext.parallelize(Seq(rw))
      val df = spark.createDataFrame(data,new StructType().add("mp", mp))
      df.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), rw)
    }
  }

  //Fallsback to spark
  testGluten("Test Map<Struct<int,string>,string> type") {
    withTempPath { path =>
      val mp = MapType(StructType(
        StructField("int", IntegerType) ::
        StructField("st", StringType) :: Nil), StringType)
      val rw = Row(Map((Row(1,"a"),"hi")))
      val data = sparkContext.parallelize(Seq(rw))
      val df = spark.createDataFrame(data,new StructType().add("mp", mp))
      df.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), rw)
    }
  }

  testGluten("Test Map<Map<int,string>,string> type") {
    withTempPath { path =>
      val mp = MapType(MapType(IntegerType, StringType), StringType)
      val rw = Row(Map((Map((1,"a"),(2,"b")),"hi")))
      val data = sparkContext.parallelize(Seq(rw))
      val df = spark.createDataFrame(data,new StructType().add("mp", mp))
      df.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), rw)
    }
  }

  //TODO: Add Map value side tests
}
