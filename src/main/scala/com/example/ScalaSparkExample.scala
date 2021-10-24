package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
 * @author ${user.name}
 */

case class Person(name: String, age: Int)

object ScalaSparkExample {

  def main(args: Array[String]): Unit = {


    try {
      val spark = getSparkSession
      import spark.implicits._
      Logger.getLogger("org").setLevel(Level.FATAL)

      val data = Seq(
        Row(8, "bat"),
        Row(64, "mouse"),
        Row(-27, "horse"),
        Row(-27, "horse"),
        Row(-27, "horse")
      )
      val schema = StructType(
        List(
          StructField("number", IntegerType, nullable = true),
          StructField("word", StringType, nullable = true)
        )
      )
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )
      df.show()

      val df2 = df.withColumn("test_field", lit("textmext"))
      df2.show()

      df2.groupBy("word")
        .agg(sum("number"), count("word"))
        .show()

    }
    catch {
      case e: Exception => println(e.toString)
    }


  }

  def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("Spark Example")
      .getOrCreate()
  }
}

