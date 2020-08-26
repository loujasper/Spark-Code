package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object spark05_UDAF_LoadSave {


  def main(args: Array[String]): Unit = {
    val con: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

    val spark = SparkSession.builder().config(con).getOrCreate()

    import spark.implicits._

//    TODO 通用的读取数据
//    注意：SparkSql读取数据默认采用的是列式存储格式，
//    如果读取数据格式为Json文件，可以使用.format("json",)
//    Spark读取Json文件时，要求文件中的每一行符合JSon的合适要求
    val ss = spark.read.json("in/province.json")

    ss.show()



    spark.stop()
  }
}