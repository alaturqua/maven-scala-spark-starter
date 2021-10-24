package com.commerzbank

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]): Unit = {
    val spark = getSparkSession
    import spark.implicits._
    Logger.getLogger("org").setLevel(Level.FATAL)

    try {
      val df = spark.read.json("file:/C:/tools/spark/examples/src/main/resources/people.json")
      df.printSchema()
      df.select("name").show()
      df.select($"name", $"age" + 1).show()
      df.filter($"age" > 21).show()
      df.groupBy("age").count().show()

      df.createOrReplaceTempView("people")

      val sqlDF = spark.sql("SELECT * FROM people")
      sqlDF.show()
    }
    catch {
      case e: Exception => println(e.toString)
    }






  }

  def getSparkSession: SparkSession ={
    return SparkSession
      .builder()
      .appName("Spark Example")
      .getOrCreate()
  }
}

