package com.bigdata.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object Spaeksql01_test {

  def main(args: Array[String]): Unit = {
//    TODO 创建环境对象

    //     TODO spark环境配置
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SpaekSQL")

//    Builder 创建
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //    导入隐式转换，不能是导入包中的内容，而是导入对象当中的内容，所以应该在spark环境对象名称之后导入此包(要求这个对象用val声明)
    import spark.implicits._

    val jsonDf: DataFrame = spark.read.json("in/user.json")

//    TODO spark sql
//    TODO 将df装换位临时视图

    jsonDf.createOrReplaceTempView("user")

    spark.sql("select * from user").show

//    TODO DSL
// DataFrame提供一个特定领域语言(domain-specific language, DSL)去管理结构化的数据。
// 可以在 Scala, Java, Python 和 R 中使用 DSL，使用 DSL 语法风格不必去创建临时视图了

    jsonDf.select("username", "age").show

//    如果查询列名采用单引号，在idea中需要隐式转换
    jsonDf.select('username,'age).show

//    TODO RDD <=> DataFrame
    val rdd = spark.sparkContext.makeRDD(List(
        (1,"zhangsan", 30),
        (2, "lisi", 34),
        (3, "wangwu", 12),
        (4, "zhaoliu", 53)
    ))
    val df: DataFrame = rdd.toDF("id","username","age")
    df.show
    val dfToRDD: RDD[Row] = df.rdd
    dfToRDD.collect().foreach(println)




//    TODO RDD <=> DataSet

    val userRDD = rdd.map {
      case (id, username, age) => {
        User(id, username, age)
      }
    }

    val userDS = userRDD.toDS()
    val dsToRDD: RDD[User] = userDS.rdd

//    TODO DataFrame <=> DataSet

    val dfToDS: Dataset[User] = df.as[User]

    val dsToDF: DataFrame = dfToDS.toDF()





//  释放对象
    spark.stop()


  }

  case class User(id : Int,username : String,age :Int)
}
