package com.bigdata.spark.req.service

import com.bigdata.spark.req.bean
import com.bigdata.spark.req.bean.HotCategory
import com.bigdata.spark.req.dao.HotCategorySessionAnalysisTop10Dao
import com.bigdata.summer.framework.core.TService
import com.bigdata.summer.framework.util.EnvUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 分别统计每个品类点击的次数，下单的次数和支付的次数：
  *（品类，点击总数）（品类，下单总数）（品类，支付总数）
  * */

class HotCategorySessionAnalysisTop10Service extends TService {

  private val hotCategorySessionAnalysisTop10Dao = new HotCategorySessionAnalysisTop10Dao

  override def analysis(data : Any) = {
    //    TODO 读取数据
    val top10: List[HotCategory] = data.asInstanceOf[List[HotCategory]]

    val top10Ids: List[String] = top10.map(_.categoryId)

//    算子之外的集合与算子之内的逻辑操作，会涉及网络传输问题，导致执行过程中出现冗余数据，可使用广播变量对齐优化
//    TODO 使用广播变量来实现数据传播
    val bcList: Broadcast[List[String]] = EnvUtils.getEnv().broadcast(top10Ids)


//  TODO Top10热门品类中每个品类的Top10活跃Session统计

//    TODO 获取用户行为数据
    val actionRDD: RDD[bean.UserVisitAction] = hotCategorySessionAnalysisTop10Dao.getUserVisitAction("in/user_visit_action.txt")

//    TODO 对数据进行过滤
//    TODO 对用户的点击行为进行过滤，
    println("before filter : " + actionRDD.count())
    val filterRDD: RDD[bean.UserVisitAction] = actionRDD.filter(
      action => {
        if (action.click_category_id != -1) {
          bcList.value.contains(action.click_category_id.toString)

//          var flag = false
//
//          top10.foreach(
//            hc => {
//              if (hc.categoryId.toLong == action.click_category_id) {
//                flag = true
//              }
//            }
//          )
//          flag
        } else {
          false
        }
      }
    )

    println("after filter : " + filterRDD.count())

//    TODO 将过滤后的数据进行处理（品类_session,1） => (品类_session, sum)
    val rdd: RDD[(String, Int)] = filterRDD.map(
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    )
    val reudceRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)


//    TODO 将统计后的结果进行结构转换 (品类_session, sum) => (品类，（session，sum）)
    val mapRDD: RDD[(String, (String, Int))] = reudceRDD.map {
      case (key, count) => {
        val ks: Array[String] = key.split("_")
        (ks(0), (ks(1), count))
      }
    }

//    TODO 将转换结构后的数据对品类进行分组
//    （品类，Iterator[(session1,sum1),(session2,sum2)]）

    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()




//    TODO 将分组后的数据进行排序,取Top10

    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(10)
      }
    )


    resultRDD.collect()




  }

}