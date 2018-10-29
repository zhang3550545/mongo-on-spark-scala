package com.mongodb.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object ReadMongoSchema {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MyApp")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.user")
      .getOrCreate()

    // 设置log级别
    spark.sparkContext.setLogLevel("WARN")

    val schema = StructType(
      List(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("sex", StringType)
      )
    )

    // 通过schema约束，直接获取需要的字段
    val df = spark.read.format("com.mongodb.spark.sql").schema(schema).load()
    df.show()

    df.createOrReplaceTempView("user")

    val resDf = spark.sql("select * from user")
    resDf.show()

    spark.stop()
    System.exit(0)
  }
}
