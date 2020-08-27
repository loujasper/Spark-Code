package com.bigdata.spark.Streaming.req.service

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import com.bigdata.spark.Streaming.req.bean.Ad_Click_log
import com.bigdata.spark.Streaming.req.dao.BlackListDao
import com.bigdata.summer.framework.core.TService
import com.bigdata.summer.framework.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.ListBuffer

/**
  * 需求：实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉黑。注：黑名单保存到MySQL中。
  *
  * 逻辑:
  * 1读取Kafka的数据
  * 2将数据转换为样例类来使用
  * 3.周期性获取黑名单的信息，判断当前用户的点击数据是否在黑名单中
  * 4.封装数据库连接 val connection: Connection = JDBCUtil.getConnection()
  * 5.通过HJDBC连接mysql,并获取到黑名单用户ID
  * 6.获取查询结果，利用ListBuffer 将黑名单ID append ListBuffer集合中
  * 7.如果用户在黑名单中，那么将数据过滤掉，不会进行统计
  * 8.拿到不在黑名单用户的列表中，如果用户在黑名单中，那么将数据过滤掉，不会进行统计
  * 10.将正常数据进行点击的统计 格式转换利用map算子将数据拼接
  *     key = day-userid-adid
  *     (day-userid-adid,1) => (day-userid-adid, sum)
  *
  * 11.将统计的结果中超过阀值的用户信息拉入到黑名单中。
  *     注意： 有状态保存sparkstreaming-->updateStateByKey=>checkpoint=>HDFS=>产生大量的小文件
  *           所以统计结果放在mysql中。(redis有数据过期自动删除功能)
  * 12.更新(新增)用户点击数量，判断是否超过阀值，如果超过阀值，则拉入黑名单（利用SQL语句
  *     insert into black_list(userid)
  *     select userid from user_ad_count
  *     where dt = ? and userid = ? and adid = ?
  *     and count >= 100
  *     on duplicate key
  *     update userid = ? ）
  * 13.关闭数据库连接
  */

class BlackListService extends TService{

  private val blackListDao = new BlackListDao

  override def analysis() = {

//    TODO 读取Kafka的数据
    val ds: DStream[String] = blackListDao.readKafka()


//    TODO 将数据转换为样例类来使用
    val logDS = ds.map(
      data => {
        val datas = data.split(" ")
        Ad_Click_log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

//    TODO 周期性获取黑名单的信息，判断当前用户的点击数据是否在黑名单中
    val reduceDS: DStream[((String, String, String), Int)] = logDS.transform(

      rdd => {
//        CODE ：transform中的会周期性执行，没采集一份数据就会形成一个RDD，就会执行这段代码
//        TODO 读取mysql数据库获取黑名单信息
//        TODO JDBC
        val connection: Connection = JDBCUtil.getConnection()

//        TODO 执行SQL
        val pstat: PreparedStatement = connection.prepareStatement(
          """
          select userid from black_list
          """.stripMargin)

//        TODO 获取查询结果
        val rs: ResultSet = pstat.executeQuery()

//        TODO 利用ListBuffer 获取黑名单ID集合
        val blackIds = ListBuffer[String]()

//        TODO 循环遍历
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }

        rs.close()
        pstat.close()
        connection.close()

//        TODO 如果用户在黑名单中，那么将数据过滤掉，不会进行统计
        val filterRDD = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          })

//        TODO 将正常数据进行点击的统计 格式转换利用map算子将数据拼接
        /**
          * key = day-userid-adid
          * (day-userid-adid,1) => (day-userid-adid, sum)
          *
          */
        val sdf = new SimpleDateFormat("yyyy-MM-dd") //格式化日期

        val mapRDD = filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        )

//    TODO 将正常数据进行点击的统计
        mapRDD.reduceByKey(_ + _)
      }
    )


//    TODO 将统计的结果中超过阀值的用户信息拉入到黑名单中。
    reduceDS.foreachRDD(
      rdd => {
//        rdd.foreachPartition() 以分区为单位对数据进行遍历

        rdd.foreachPartition(
          datas => {
            val conn = JDBCUtil.getConnection()
            val pstat = conn.prepareStatement( //插入数据如果有重复的key则更新count
              """
                insert into user_ad_count(dt,userid,adid,count)
                values(?,?,?,?)
                on duplicate key
                update count = count + ?
              """.stripMargin)

            val pstat1 = conn.prepareStatement( //插入数据如果有重复的key则更新count
              """
                insert into black_list(userid)
                select userid from user_ad_count
                where dt = ? and userid = ? and adid = ?
                and count >= 100
                on duplicate key
                update userid = ?
              """.stripMargin)

          datas.foreach{
            case ((day, userid, adid), sum) => {
              pstat.setString(1, day) //一共5个？，所以插入五个数据
              pstat.setString(2, userid)
              pstat.setString(3, adid)
              pstat.setLong(4, sum)
              pstat.setLong(5, sum)
              pstat.executeUpdate()

              pstat1.setString(1, day)
              pstat1.setString(2, userid)
              pstat1.setString(3, adid)
              pstat1.setString(4, userid)
              pstat1.executeUpdate()
            }
          }
          pstat.close()
          pstat1.close()
          conn.close()
        }

      )
    }
  )
    ds.print()
  }





def analysis2() = {

//    TODO 读取Kafka的数据
    val ds: DStream[String] = blackListDao.readKafka()


//    TODO 将数据转换为样例类来使用
    val logDS = ds.map(
      data => {
        val datas = data.split(" ")
        Ad_Click_log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

//    TODO 周期性获取黑名单的信息，判断当前用户的点击数据是否在黑名单中

    val reduceDS: DStream[((String, String, String), Int)] = logDS.transform(

      rdd => {
//        CODE ：transform中的会周期性执行，没采集一份数据就会形成一个RDD，就会执行这段代码
//        TODO 读取mysql数据库获取黑名单信息
//        TODO JDBC
        val connection: Connection = JDBCUtil.getConnection()

//        TODO 执行SQL
        val pstat: PreparedStatement = connection.prepareStatement(
          """
          select userid from black_list
          """.stripMargin)

//        TODO 获取查询结果
        val rs: ResultSet = pstat.executeQuery()

//        TODO 利用ListBuffer 获取黑名单ID集合
        val blackIds = ListBuffer[String]()

//        TODO 循环遍历
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }

        rs.close()
        pstat.close()
        connection.close()

//    TODO 如果用户在黑名单中，那么将数据过滤掉，不会进行统计
        val filterRDD = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          })

//    TODO 将正常数据进行点击的统计 格式转换利用map算子将数据拼接
        /**
          * key = day-userid-adid
          * (day-userid-adid,1) => (day-userid-adid, sum)
          *
          */
        val sdf = new SimpleDateFormat("yyyy-MM-dd") //格式化日期

        val mapRDD = filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        )

//    TODO 将正常数据进行点击的统计
        mapRDD.reduceByKey(_ + _)
      }
    )


//    TODO 将统计的结果中超过阀值的用户信息拉入到黑名单中。
    reduceDS.foreachRDD(
      rdd => {

        val conn = JDBCUtil.getConnection()
        val pstat = conn.prepareStatement( //插入数据如果有重复的key则更新count
          """
                insert into user_ad_count(dt,userid,adid,count)
                values(?,?,?,?)
                on duplicate key
                update count = count + ?
              """.stripMargin)

        val pstat1 = conn.prepareStatement( //插入数据如果有重复的key则更新count
          """
                insert into black_list(userid)
                select userid from user_ad_count
                where dt = ? and userid = ? and adid = ?
                and count >= 100
                on duplicate key
                update userid = ?
              """.stripMargin)

        rdd.foreach {
//      TODO data 是每一个采集周期中用户点击同一个广告的数量

          case ((day, userid, adid), sum) => {
//          TODO 有状态保存sparkstreaming-->updateStateByKey=>checkpoint=>HDFS=>产生大量的小文件
//          TODO 所以统计结果放在mysql中。(redis有数据过期自动删除功能)
//          TODO 更新(新增)用户点击数量
            pstat.setString(1, day) //一共5个？，所以插入五个数据
            pstat.setString(2, userid)
            pstat.setString(3, adid)
            pstat.setLong(4, sum)
            pstat.setLong(5, sum)
            pstat.executeUpdate()

//          TODO 获取最新的用户统计数据
//          TODO 是否超过阀值，如果超过阀值，则拉入黑名单
            pstat1.setString(1, day)
            pstat1.setString(2, userid)
            pstat1.setString(3, adid)
            pstat1.setString(4, userid)
            pstat1.executeUpdate()

            pstat.close()
            pstat1.close()
            conn.close()
          }
        }
      }
    )
    ds.print()
  }


   def analysis1() = {

//    TODO 读取Kafka的数据
    val ds: DStream[String] = blackListDao.readKafka()


//    TODO 将数据转换为样例类来使用
    val logDS = ds.map(
      data => {
        val datas = data.split(" ")
        Ad_Click_log(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    //    TODO 周期性获取黑名单的信息，判断当前用户的点击数据是否在黑名单中

    val reduceDS: DStream[((String, String, String), Int)] = logDS.transform(

      rdd => {
        //        CODE ：transform中的会周期性执行，没采集一份数据就会形成一个RDD，就会执行这段代码
        //        TODO 读取mysql数据库获取黑名单信息
        //        TODO JDBC
        val connection: Connection = JDBCUtil.getConnection()

        //        TODO 执行SQL
        val pstat: PreparedStatement = connection.prepareStatement(
          """
          select userid from black_list
          """.stripMargin)

        //        TODO 获取查询结果
        val rs: ResultSet = pstat.executeQuery()

        //        TODO 利用ListBuffer 获取黑名单ID集合
        val blackIds = ListBuffer[String]()

        //        TODO 循环遍历
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }

        rs.close()
        pstat.close()
        connection.close()

        //    TODO 如果用户在黑名单中，那么将数据过滤掉，不会进行统计
        val filterRDD = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          })

        //    TODO 将正常数据进行点击的统计 格式转换利用map算子将数据拼接
        /**
          * key = day-userid-adid
          * (day-userid-adid,1) => (day-userid-adid, sum)
          *
          */
        val sdf = new SimpleDateFormat("yyyy-MM-dd") //格式化日期

        val mapRDD = filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        )

        //    TODO 将正常数据进行点击的统计
        mapRDD.reduceByKey(_ + _)
      }
    )


    //    TODO 将统计的结果中超过阀值的用户信息拉入到黑名单中。
    reduceDS.foreachRDD(
      rdd => {
        rdd.foreach {
          //              TODO data 是每一个采集周期中用户点击同一个广告的数量

          case ((day, userid, adid), sum) => {
            //              TODO 有状态保存sparkstreaming-->updateStateByKey=>checkpoint=>HDFS=>产生大量的小文件
            //              TODO 所以统计结果放在mysql中。(redis有数据过期自动删除功能)
            //              TODO 更新(新增)用户点击数量
            val conn = JDBCUtil.getConnection()
            val pstat = conn.prepareStatement( //插入数据如果有重复的key则更新count
              """
                insert into user_ad_count(dt,userid,adid,count)
                values(?,?,?,?)
                on duplicate key
                update count = count + ?
              """.stripMargin)

            pstat.setString(1, day) //一共5个？，所以插入五个数据
            pstat.setString(2, userid)
            pstat.setString(3, adid)
            pstat.setLong(4, sum)
            pstat.setLong(5, sum)
            pstat.executeUpdate()

            //              TODO 获取最新的用户统计数据
            //              TODO 是否超过阀值，如果超过阀值，则拉入黑名单
            val pstat1 = conn.prepareStatement( //插入数据如果有重复的key则更新count
              """
                insert into black_list(userid)
                select userid from user_ad_count
                where dt = ? and userid = ? and adid = ?
                and count >= 100
                on duplicate key
                update userid = ?
              """.stripMargin)

            pstat1.setString(1, day)
            pstat1.setString(2, userid)
            pstat1.setString(3, adid)
            pstat1.setString(4, userid)
            pstat1.executeUpdate()

            pstat.close()
            pstat1.close()
            conn.close()
          }
        }
      }
    )
    ds.print()
  }

}










