package com.bigdata.spark.Streaming.req.service

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.spark.Streaming.req.bean.Ad_Click_log
import com.bigdata.spark.Streaming.req.dao.DateAreaCityCountAnalysisDao
import com.bigdata.summer.framework.core.TService
import com.bigdata.summer.framework.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
  * 实时统计每天各地区各城市各广告的点击总流量，并将其存入MySQL。
  *
  */

class DateAreaCityCountAnalysisService extends TService{

  private val dateAreaCityCountAnalysisDao = new DateAreaCityCountAnalysisDao

  override def analysis() = {

//    TODO 读取kafka数据
    val ds: DStream[String] = dateAreaCityCountAnalysisDao.readKafka()

//    TODO 将数据转换为样例类来使用
    val logDS: DStream[Ad_Click_log] = ds.map(
      data => {
        val datas: Array[String] = data.split(" ")
        Ad_Click_log( datas(0), datas(1), datas(2), datas(3), datas(4) )
      }
    )
//    TODO 调整数据格式---（（2020-08-27，广东，深圳，002），1）
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dayDS = logDS.map(
      log => {
        val ts = log.ts
        val day = sdf.format(new Date(ts.toLong))
        (( day,log.area ,log.city ,log.adid ) ,1)
      }
    )
//  TODO 将数据进行统计
    val resultDS: DStream[((String, String, String, String), Int)] = dayDS.reduceByKey(_+_)

//    将统计结果报错到Mysql中
    resultDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          datas => {
//            TODO 获取数据库连接
            val connection: Connection = JDBCUtil.getConnection()
            val pstat: PreparedStatement = connection.prepareStatement(
              """
                insert into area_city_ad_count(dt, area, city, adid, count)
                values(?,?,?,?,?)
                on duplicate key
                update count = count + ?
              """.stripMargin)
//            TODO 操作数据库
            datas.foreach{
              case ( (dt,area,city,adid),count ) => {
                pstat.setString(1,dt)
                pstat.setString(2,area)
                pstat.setString(3,city)
                pstat.setString(4,adid)
                pstat.setLong(5,count)
                pstat.setLong(6,count)
                pstat.executeUpdate()
              }
            }

//            TODO 关闭资源
            pstat.close()
            connection.close()

          }
        )
      }
    )
    ds.print()
  }
}
