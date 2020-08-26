package com.bigdata.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object spark07_hive_req {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkHive")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    spark.sql("use spark_on_hive")

    spark.sql(
            """
              |select
              |	*
              |from
              |(
              |	select
              |	*,
              |	rank() over(partition by area order by click_count desc) rank
              |	from
              |	(
              |		select
              |			area,
              |			product_name,
              |			count(*) as click_count
              |		from
              |		(
              |			select
              |				uva.click_product_id,
              |				po.product_name,
              |				ci.area
              |			from
              |			spark_on_hive.user_visit_action uva
              |			join spark_on_hive.product_info as po
              |			on uva.click_product_id = po.product_id
              |			join spark_on_hive.city_info ci
              |			on uva.city_id = ci.city_id
              |			where uva.click_product_id > -1
              |		) t
              |		group by area,product_name
              |	)t2
              |)t3
              |where rank <= 3
            """.stripMargin).show()


    spark.stop()

  }
}
