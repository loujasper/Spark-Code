package com.bigdata.spark.sql

import com.bigdata.spark.sql.Spaeksql01_test.User
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object spark02_02 {

  def main(args: Array[String]): Unit = {
    val con: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

    val spark = SparkSession.builder().config(con).getOrCreate()

    import spark.implicits._

    val rdd = spark.sparkContext.makeRDD(List(
      (1,"zhangsan", 30),
      (2, "lisi", 34),
      (3, "wangwu", 12),
      (4, "zhaoliu", 53)
    ))
    val df: DataFrame = rdd.toDF("id","username","age")

    df.createTempView("user")


/*  1.会涉及到编码的问题，所以开发中更多用于DataSet[row]格式
    val df: DataFrame = rdd.toDF("id","username","age")

    val ds: Dataset[Row] = df.map(row => {
      val id = row(0)
      val name = row(1)
      val age = row(2)
      Row(id, "username :" + name, age)
    })*/

/*2.
    val userRDD = rdd.map {
      case (id, username, age) => {
        User(id, username, age)
      }
    }

    val userDS = userRDD.toDS()

    val newDs: Dataset[User] = userDS.map(
      user => {
        User(user.id, user.name, user.age)
      }
    )
    newDs.show
*/

//    TODO 使用自定义函数在sql中完成数据的操作。
    spark.udf.register("addName",(x:String)=>"Name："+x)
    spark.udf.register("changeAge",(x:String)=> 18)

    spark.sql("select addName(username),changeAge(age) from user").show



    spark.stop()
  }


  case class User(id : Int,name :String,age:Int)
}
