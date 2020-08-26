package com.bigdata.spark.sql


import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, types}




object spark08_hive_req {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkHive")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    import  spark.implicits._

    spark.sql("use spark_on_hive")
//  TODO 获取满足条件的数据
    spark.sql(
      """
        |select
        |uva.click_product_id,
        |po.product_name,
        |ci.city_name,
        |ci.area
        |from
        |spark_on_hive.user_visit_action uva
        |join spark_on_hive.product_info as po
        |on uva.click_product_id = po.product_id
        |join spark_on_hive.city_info ci
        |on uva.city_id = ci.city_id
        |where uva.click_product_id > -1
      """.stripMargin).createOrReplaceTempView("t1")

    val udaf = new cityClick
//    val udaf = new CityRemarkUDAF

    spark.udf.register("cityRemark",udaf)

//    TODO 对区域和商品进行分组，统计商品点击的数量
    spark.sql(
      """
        |select
        |	area,
        |	product_name,
        |	count(*) as click_count,
        | cityRemark(city_name)
        |from t1
        |group by area,product_name
      """.stripMargin).createOrReplaceTempView("t2")

//    TODO 将统计的结果根据数量进行降序排序
    spark.sql(
      """
        |	select
        |	*,
        |	rank() over(partition by area order by click_count desc) rank
        |	from t2
      """.stripMargin).createOrReplaceTempView("t3")

//    TODO 将排序后的结果取前三名
    spark.sql(
      """
        |select
        |	*
        |from t3 where rank <= 3
      """.stripMargin).show(10)

    spark.stop()
  }



  class cityClick extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = {
      StructType(Array(StructField("city", StringType)))
    }

    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("city", MapType(StringType,LongType))
      ))
    }

    override def dataType: DataType = {
      StringType
    }

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = Map[String, Long]()  //     buffer(1) = Map[String, Long]()
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val cityName: String = input.getString(0) //获取之前的点击城市

      val cityMap: Map[String, Long] = buffer.getAs[Map[String, Long]](0)     //获取新的点击的城市

      // 判断是够在原来的内存是否存在，如果存假则1＋1 = 2，否则是 0 + 1 =1
      val cityClickCount = cityMap.getOrElse(cityName,0L) + 1

      buffer(0) = cityMap.updated(cityName, cityClickCount) //将新加的数据更新到原来的内存中
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

      val map1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
      val map2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)

      buffer1(0) = map1.foldLeft(map2) {
        case (map, (k, v)) => {
          map.updated(k, map.getOrElse(k, 0L) + v)
        }
      }
    }

    override def evaluate(buffer: Row): Any = {
      val citymap: collection.Map[String, Long] = buffer.getMap[String, Long](0)

      val cityCount: List[(String, Long)] = citymap.toList.sortWith(
        (left, right) => left._2 > right._2
      ).take(2)


      val s = new StringBuilder
      val hasRest = citymap.size > 2
      var rest = 0L
      cityCount.foreach{
        case (city, cnt) => {
          var r = cnt
          s.append(city +" "+ r )
          rest =rest + r
        }
      }
/*      if (hasRest) {
        s.toString() +"其他" + rest
      }else {
        s.toString()
      }*/

    }
  }

  /**
    *
  val hasRest = citymap.size > 2
      var rest = 0L

      cityToCount.foreach{
        case(city, cnt) => {
          var r = cnt * 100 / totalcnt
          s.append(city + " " + r + "%,")
          rest = rest + r
        }
      }

      if (hasRest) {
        s.toString() +"其他" + (100 - rest) +"%"
      }else {
        s.toString()
      }
    错误提示：scala.MatchError: () (of class scala.runtime.BoxedUnit) 因为数据不规则
    */



//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++






  //    TODO 获取每个地区的点击的总和、获取该地区每个城市点击总和
  //    (地区点击总和，每个城市的点击总和)
  //    (地区商品的点击总和，Map(城市，点击SUM))
  //    某一个城市的点击的sum / 商品点击总和
  //  TODO 城市备注的聚合函数
  class CityRemarkUDAF extends UserDefinedAggregateFunction{

  //    TODO 输入的数据其实就是城市名称
    override def inputSchema: StructType = {
      StructType(Array(StructField("cityName", StringType)))
    }

  //    TODO 缓冲区中的数据应该为，总共点击数量，Map[cityname,cnt](按照城市汇总每个城市的点击数量)
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("total", LongType),
        StructField("cityMap", MapType(StringType,LongType))
      ))
    }
  //    TODO 返回城市备注的字符串
    override def dataType: types.DataType = {
      StringType
    }

  //    TODO 稳定性
    override def deterministic: Boolean = true

  //    TODO 缓冲区的初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L  // 当时用update方法时，就可以使用scala中的 buffer = buffer.update(0, 0L)
      buffer(1) = Map[String, Long]()
    }
  //    TODO 更新缓冲区
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val cityName: String = input.getString(0) //首先应该先得到城市名称（一行数据）分两步

  //    TODO 1.点击总和需要增加
      buffer(0) = buffer.getLong(0) + 1  //每次输入一个城市就加1

  //    TODO 2.城市点击增加
      val cityMap: Map[String, Long] = buffer.getAs[Map[String, Long]](1) //获取新的点击城市的数量
      //判断新加入缓存中得城市在 cityName 是否出现过
      val newClickCount = cityMap.getOrElse(cityName,0L) + 1

  //    TODO 将新的城市加入到点击更新到缓存中
      buffer(1) = cityMap.updated(cityName, newClickCount)

    }
  //    TODO合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

  //    合并点击数量总和
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  //    buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))

  //    两个Map合并
      val map1 = buffer1.getAs[Map[String, Long]](1)
      val map2 = buffer2.getAs[Map[String, Long]](1)

  //    合并城市点击Map, 两个Map合并通常用一下两个方法
      buffer1(1) = map1.foldLeft(map2) {
        case (map,(k, v)) => {
          map.updated(k, map.getOrElse(k,0L) + v)
        }
      }
    }

  //    TODO 对缓冲进行计算，生成字符串
    override def evaluate(buffer: Row): Any = {
      val totalcnt: Long = buffer.getLong(0) //首先拿到缓冲区的点击的总量
      val citymap: collection.Map[String, Long] = buffer.getMap[String, Long](1)  //拿到每个城市的点击总量

  //    两个Map的排序，取前两个
      val cityToCount: List[(String, Long)] = citymap.toList.sortWith(
        (left, right) => left._2 > right._2
      ).take(2)

      val s = new StringBuilder  //因为最后是要合并成字符串，需要用这个对象进行拼接

      val hasRest = citymap.size > 2
      var rest = 0L

      cityToCount.foreach{
        case(city, cnt) => {
          var r = cnt * 100 / totalcnt
          s.append(city + " " + r + "%,")
          rest = rest + r
        }
      }

      if (hasRest) {
        s.toString() +"其他" + (100 - rest) +"%"
      }else {
        s.toString()
      }
    }

  }


}
