package com.mongodb.spark

import org.apache.spark.sql.SparkSession


object ReadMongo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MyApp")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.user")
      .getOrCreate()

    // 设置log级别
    spark.sparkContext.setLogLevel("WARN")

    val df = MongoSpark.load(spark)
    df.show()

    df.createOrReplaceTempView("user")

    val resDf = spark.sql("select name,age,sex from user")
    resDf.show()

    spark.stop()
    System.exit(0)
  }
}
