package com.bigdata.spark.req.service

import com.bigdata.spark.req.bean
import com.bigdata.spark.req.dao.PageflowDao
import com.bigdata.summer.framework.core.TService
import org.apache.spark.rdd.RDD

class PageflowService extends TService {

  private val pageflowDao = new PageflowDao

  override def analysis() ={

//    TODO 对指定的页面流程进行单跳转换率的统计

//    TODO 获取原始用户行为数据
    val actionRDD: RDD[bean.UserVisitAction] = pageflowDao.getUserVisitAction("in/user_visit_action.txt")
    actionRDD.cache()

//    1,2,3,4,5,6,7
//    1-2,2-3,3-4,4-5
    val flowsIds = List(1,2,3,4,5,6,7)
    val okflowIds: List[String] = flowsIds.zip(flowsIds.tail).map( t=> (t._1 + "-" +t._2) )



//    TODO 计算分母

    val filterRDD: RDD[bean.UserVisitAction] = actionRDD.filter(
      action => {
        flowsIds.init.contains(action.page_id.toInt) //init 去掉7 去掉尾部
      }
    )

    val pageToOneRDD: RDD[(Long, Int)] = filterRDD.map(
      action => { //这块不需要全部统计，应该先做过滤
        (action.page_id, 1)
      }
    )

    val pageToSum: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_+_)

    val pageCountArray: Array[(Long, Int)] = pageToSum.collect()

//    TODO 计算分子

//    TODO 将数据根据用户的sessionid进行分组
    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(_.session_id)

    val pageflowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(
      iter => {
//        TODO 将分组后的数据根据时间进行排序
        val actions: List[bean.UserVisitAction] = iter.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
//        TODO 将排序后的数据 进行结构转换 actions -》 pageid
        val pageids: List[Long] = actions.map(_.page_id)

        pageids.filter(
          id => {
            flowsIds.contains(id)
          }
        )

//        TODO 将转换后的结果进行格式的转换 使用拉链
//        zip() 用拉链
//         tail()方法用于返回一个新的TreeSet，该树集由除第一个元素外的所有元素组成。
//        （1,2,3,4） -> (1-2),(2-3),(3-4)
        val zipids: List[(Long, Long)] = pageids.zip(pageids.tail)

//        ((1-2),1) ，((2-3),1)，((3-4),1)
        zipids.map {
          case (pageid1, pageid2) => {
            (pageid1 + "-" + pageid2, 1)
          }
        }.filter{
          case (ids, one) => {
            okflowIds.contains(ids)
          }
        }

      }
    )

//    TODO 将分组后的数据进行结构转换(sessionid,(((1-2),1),(2-3),1),(3-4),1))) -> (((1-2),1),(2-3),1),(3-4),1))
    val pageidSumRDD: RDD[List[(String, Int)]] = pageflowRDD.map(_._2)

//    ((1-2),1) ，((2-3),1) --> (1-2,1)，（2-3,1）
    val pageflowRDD1: RDD[(String, Int)] = pageidSumRDD.flatMap(list => list)

//    TODO 聚合操作 (1-2,1)，(1-2,1)，（2-3,1），（2-3,1），（2-3,1）  -->  (1-2,2),2-3,3）
    val pageflowSumRDD: RDD[(String, Int)] = pageflowRDD1.reduceByKey(_+_)


//    TODO 计算页面单跳转化率
    pageflowSumRDD.foreach{
      case (pageflow, sum) => {
//      1-2 -> 1
        val pageid: String = pageflow.split("-")(0)
        val value: Int = pageCountArray.toMap.getOrElse(pageid.toLong, 1)
        println("页面跳转【" + pageflow + "】的转换率为 "+ (sum.toDouble / value) )
      }
    }

  }




//  TODO ---------------------------------------------------------------------------------

  def analysis1() ={

    //    TODO 获取原始用户行为数据
    val actionRDD: RDD[bean.UserVisitAction] = pageflowDao.getUserVisitAction("in/user_visit_action.txt")
    actionRDD.cache()

    //    TODO 计算分母
    val pageToOneRDD: RDD[(Long, Int)] = actionRDD.map(
      action => {
        (action.page_id, 1)
      }
    )
    val pageToSum: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_+_)

    val pageCountArray: Array[(Long, Int)] = pageToSum.collect()

    //   TODO 计算分子

    //    TODO 将数据根据用户的sessionid进行分组
    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(_.session_id)

    val pageflowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(
      iter => {
        //        TODO 将分组后的数据根据时间进行排序
        val actions: List[bean.UserVisitAction] = iter.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        //        TODO 将排序后的数据 进行结构转换 actions -》 pageid
        val pageids: List[Long] = actions.map(_.page_id)

        //        TODO 将转换后的结果进行格式的转换

        //        （1,2,3,4） -> (1-2),(2-3),(3-4)
        //        zip()
        //         tail()方法用于返回一个新的TreeSet，该树集由除第一个元素外的所有元素组成。
        val zipids: List[(Long, Long)] = pageids.zip(pageids.tail)

        //        ((1-2),1) ，((2-3),1)
        zipids.map {
          case (pageid1, pageid2) => {
            (pageid1 + "-" + pageid2, 1)
          }
        }

      }
    )

    //    TODO 将分组后的数据进行结构转换
    val pageidSumRDD: RDD[List[(String, Int)]] = pageflowRDD.map(_._2)

    //    ((1-2),1) ，((2-3),1) --> (1-2,1)，（2-3,1）
    val pageflowRDD1: RDD[(String, Int)] = pageidSumRDD.flatMap(list => list)

    //    TODO 聚合操作 (1-2,1)，(1-2,1)，（2-3,1），（2-3,1），（2-3,1）  -->  (1-2,2),2-3,3）
    val pageflowSumRDD: RDD[(String, Int)] = pageflowRDD1.reduceByKey(_+_)


    //    TODO 计算页面单跳转化率
    pageflowSumRDD.foreach{
      case (pageflow, sum) => {
        val pageid: String = pageflow.split("-")(0)
        val value: Int = pageCountArray.toMap.getOrElse(pageid.toLong, 1)
        println("页面跳转【" + pageflow + "】的转换率为 "+ (sum.toDouble / value) )
      }
    }



  }
}
