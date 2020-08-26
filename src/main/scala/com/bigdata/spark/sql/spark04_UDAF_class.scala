package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

import scala.collection.immutable.Range
import scala.collection.immutable.Range.Int

object spark04_UDAF_class {

  //  TODO 求平均年龄

  def main(args: Array[String]): Unit = {
    val con: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

    val spark = SparkSession.builder().config(con).getOrCreate()

    import spark.implicits._

    val rdd1 = spark.sparkContext.makeRDD(List(
      (1, "zhangsan", 30L),
      (2, "lisi", 34L),
      (3, "wangwu", 12L),
      (4, "zhaoliu", 53L)
    ))
    val df: DataFrame = rdd1.toDF("id", "username", "age")
    val ds: Dataset[User] = df.as[User]

    df.createTempView("user")

    //    TODO 创建UDAF函数
    val udaf: myAvgAgeUDAFClass = new myAvgAgeUDAFClass

    //    TODO 注册到SparkSQL中 将整条数据当成一个对象，因为聚合函数时强类型的，
    //    TODO 那么SQL中没有类型的概念，所以无法使用,DataSet有类型的概念.可以使用DSL语法访问

//    .tocolumn 将聚合函数转换为查询的列让DataSet访问
    udaf.toColumn

    ds.select(udaf.toColumn).show

    spark.stop()
  }

  /**
    * 自定义聚合函数
    * 1.Aggregator
    * IN 输入数据的类型
    * BUFFER 缓冲区的数据类型 AvgBuffer
    * OUT 输出数据类型
    */

  //  totalAge、count

//  定义缓冲区类型
  case class AvgBuffer(var totalage:Long,var count:Long)
//  输入数据类型
  case class User(id:Long,username:String,age:Long)

  class myAvgAgeUDAFClass extends Aggregator[User, AvgBuffer, Long] {
    //    TODO 缓冲区初始值
    override def zero: AvgBuffer = {
      AvgBuffer(0L, 0L)
    }

//    TODO 聚合数据
    override def reduce(buffer: AvgBuffer, user: User): AvgBuffer = {
      buffer.totalage = buffer.totalage + user.age
      buffer.count = buffer.count + 1
      buffer
    }
//    TODO 合并缓冲区
    override def merge(buffer1: AvgBuffer, buffer2: AvgBuffer): AvgBuffer = {
      buffer1.totalage = buffer1.totalage + buffer2.totalage
      buffer1.count = buffer1.count + buffer2.count
      buffer1
    }

    override def finish(reduction: AvgBuffer): Long = {
      reduction.totalage / reduction.count
    }

//    TODO 自定义缓冲区编码
    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

//    TODO 自定义输出编码
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}