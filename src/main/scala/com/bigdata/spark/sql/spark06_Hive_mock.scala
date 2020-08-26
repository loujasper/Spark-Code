package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object spark06_Hive_mock {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val con: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

    val spark = SparkSession.builder().enableHiveSupport().config(con).getOrCreate()

    spark.sql("show databases").show
    spark.sql("use spark_on_hive").show

    spark.sql(
      """
        |CREATE EXTERNAL TABLE if not exists `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |location '/warehouse/spark_on_hive/user_visit_action'
      """.stripMargin
    )

    spark.sql(
      """
        |load data local inpath 'input/user_visit_action.txt' into table spark_on_hive.user_visit_action
      """.stripMargin)

    spark.sql(
      """
        |CREATE EXTERNAL TABLE if not exists `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
        |location '/warehouse/spark_on_hive/product_info'
      """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'input/product_info.txt' into table spark_on_hive.product_info
      """.stripMargin)

    spark.sql(
      """
        |CREATE EXTERNAL TABLE if not exists `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
        |location '/warehouse/spark_on_hive/city_info'
      """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'input/city_info.txt' into table spark_on_hive.city_info
      """.stripMargin)

    spark.sql("select * from spark_on_hive.product_info").show()
    spark.sql("select * from spark_on_hive.city_info").show()
    spark.sql("select * from spark_on_hive.user_visit_action").show(10)
    spark.stop
  }
}



