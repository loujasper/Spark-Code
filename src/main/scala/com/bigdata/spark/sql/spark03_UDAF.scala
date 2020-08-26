package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object spark03_UDAF {

//  TODO 求平均年龄

  def main(args: Array[String]): Unit = {
    val con: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

    val spark = SparkSession.builder().config(con).getOrCreate()

    import spark.implicits._

    val rdd1 = spark.sparkContext.makeRDD(List(
      (1,"zhangsan", 30L),
      (2, "lisi", 34L),
      (3, "wangwu", 12L),
      (4, "zhaoliu", 53L)
    ))
    val df: DataFrame = rdd1.toDF("id","username","age")

    df.createTempView("user")

    //    TODO 创建UDAF函数
        val udaf: myAvgAgeUDAF = new myAvgAgeUDAF

//    TODO 注册到SparkSQL中
    spark.udf.register("avgAge",udaf)

//    TODO 在SQL中使用聚合函数
    spark.sql("select avgAge(age) from user").show



    spark.stop()
  }

  /**
    * 自定义聚合函数
    * 1.继承UserDefinedAggregateFunction
    * 2.重写
    *
    */

//  totalAge、count
  class myAvgAgeUDAF extends UserDefinedAggregateFunction{

//   TODO 1.输入数据的结构信息
    override def inputSchema: StructType = {
      StructType(Array(StructField("age",LongType)))
    }

//   TODO 2.缓冲区的数据结构信息：年龄的综合，人的数量
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("age",LongType),
        StructField("count",LongType)
      ))
    }

//    TODO 3.聚合函数返回的结果类型
    override def dataType: DataType = LongType

//    TODO 4.函数稳定性
    override def deterministic: Boolean = true

//    TODO 5.函数初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L  //表示缓冲区第一个字段age
      buffer(1) = 0L  //表示缓冲区第二个字段count
    }

//    TODO 6.更新缓冲区的数据 将input 更新到 buffer中
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getLong(0) //input.getLong(0)表示输入只有一个
      buffer(1) = buffer.getLong(1) + 1
    }

//    TODO 7.合并缓冲区，合并两个缓冲区
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

//    TODO 8.函数计算
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }

}
