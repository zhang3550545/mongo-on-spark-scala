package com.mongodb.spark

import org.apache.spark.sql.SparkSession
import org.bson.Document

object WriteMongo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MyApp")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.user")
      .getOrCreate()

    // 设置log级别
    spark.sparkContext.setLogLevel("WARN")

    val document1 = new Document()
    document1.append("name", "sunshangxiang").append("age", 18).append("sex", "female")
    val document2 = new Document()
    document2.append("name", "diaochan").append("age", 24).append("sex", "female")
    val document3 = new Document()
    document3.append("name", "huangyueying").append("age", 23).append("sex", "female")

    val seq = Seq(document1, document2, document3)
    val df = spark.sparkContext.parallelize(seq)

    // 将数据写入mongo
    MongoSpark.save(df)

    spark.stop()
    System.exit(0)
  }
}
