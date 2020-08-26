package com.bigdata.spark.req.service

import com.bigdata.spark.req.bean.HotCategory
import com.bigdata.spark.req.dao.HotCategoryAnalysisTop10Dao
import com.bigdata.spark.req.helper.HotCategoryAccumulator
import com.bigdata.summer.framework.core.TService
import com.bigdata.summer.framework.util.EnvUtils
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 分别统计每个品类点击的次数，下单的次数和支付的次数：
  *（品类，点击总数）（品类，下单总数）（品类，支付总数）
  * */

class HotCategoryAnalysisTop10Service extends TService {

  private val hotCategoryAnalysisTop10Dao = new HotCategoryAnalysisTop10Dao

  override def analysis() = {
    //    TODO 读取数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("in/user_visit_action.txt")

    //    TODO 对品类进行点击的统计
    //    TODO 使用累加器对数据进行聚合

    val acc: HotCategoryAccumulator = new HotCategoryAccumulator

    EnvUtils.getEnv().register(acc, "hotCategory")

//    TODO 将数据循环向累加器中放
    actionRDD.foreach(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1"){
          acc.add(datas(6), "click")
        }else if(datas(8) != "null"){
          val ids: Array[String] = datas(8).split(",")
          ids.foreach(
            id => {
              acc.add(id, "order")
              acc
            }
          )
        }else if(datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.foreach(
            id => {
              acc.add(id, "pay")
              acc
            }
          )
        }else{
          Nil
        }
      }
    )

//    TODO 获取累加器值
    val accValue: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accValue.map(_._2)

//    TODO 排序
    categories.toList.sortWith(
      (leftHC, rightHC) => {
        if (leftHC.clickCount > rightHC.clickCount) {
          true
        } else if (leftHC.clickCount == rightHC.clickCount) {
          if (leftHC.orderCount > rightHC.orderCount) {
            true
          } else if (leftHC.orderCount == rightHC.orderCount) {
            leftHC.payCount > rightHC.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10)



  }

  /*************************************************************************************/

  def analysis4() = {
    //    TODO 读取数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("in/user_visit_action.txt")

    //    TODO 对品类进行点击的统计
    //    line =>
    //    click = HotCategory(1,0,0)
    //    order = HotCategory(0,1,0)
    //    pay   = HotCategory(0,0,1)

    val flatmapRDD = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        //          TODO 点击场合
        if (datas(6) != "-1") {
          List((datas(6), HotCategory(datas(6), 1, 0, 0))) //HotCategory(datas(6),1, 0, 0)相当于伴生对象， 不需要new

          //          TODO 下单场合
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => (id, HotCategory(id, 0, 1, 0)))

          //          TODO 支付场合
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => (id, HotCategory(id, 0, 0, 1)))

        } else {
          Nil
        }
      }
    )
    val resultRDD: RDD[(String, HotCategory)] = flatmapRDD.reduceByKey(
      (c1, c2) => {
        c1.clickCount = c1.clickCount + c2.clickCount
        c1.orderCount = c1.orderCount + c2.orderCount
        c1.payCount = c1.payCount + c2.payCount
        c1
      }
    )
//    TODO 排序优化
    resultRDD.collect().sortWith(
      (left, right) => {
        val leftHC = left._2
        val rightHC = right._2
        if (leftHC.clickCount > rightHC.clickCount) {
          true
        } else if (leftHC.clickCount == rightHC.clickCount) {
          if (leftHC.orderCount > rightHC.orderCount) {
            true
          } else if (leftHC.orderCount == rightHC.orderCount) {
            leftHC.payCount > rightHC.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10)
  }

  def analysis3() = {
    //    TODO 读取数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("in/user_visit_action.txt")

    //    TODO 对品类进行点击的统计
//    line =>
//    click = (1,0,0)
//    order = (0,1,0)
//    pay   = (0,0,1)

    val flatmapRDD = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
//          TODO 点击场合
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))

//          TODO 下单场合
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))

//          TODO 支付场合
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))

        } else {
          Nil
        }
      }
    )
    val resultRDD: RDD[(String, (Int, Int, Int))] = flatmapRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    resultRDD.sortBy(_._2, ascending = false).take(10)

  }


  /************************************************************************************************/

  def analysis2() = {
//    TODO 读取数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("in/user_visit_action.txt")

//    TODO 对品类进行点击的统计
//    (category, clickCount)
    val clickRDD: RDD[(String, Int)] = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        (datas(6), 1)
      }
    ).filter(_._1 != "-1")
    val categoryToClickCountRDD: RDD[(String, Int)] = clickRDD.reduceByKey(_+_)

//    TODO 对品类进行下单的统计
//    (category, orderCount)
//    (品类1，品类2，品类3，10)  => (品类1，10),(品类2，10),(品类3，10)
//    所以的到数据之后要将其在进行一次扁平化处理(flatmap())
    val orderRDD = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8)
      }
    ).filter(_ != "null")

    val orderToOneRDD = orderRDD.flatMap({
      id => {
        val ids = id.split(",")
        ids.map( id => (id,1))
      }
    })

    val categoryToOrderCountRDD: RDD[(String, Int)] = orderToOneRDD.reduceByKey(_+_)

//    TODO 对品类进行支付的统计
//    (category, payCount)
    val payRDD = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10)
      }).filter(_ != "null")

    val payToOneRDD = payRDD.flatMap({
      id => {
        val ids = id.split(",")
        ids.map( id => (id,1))
      }
    })

    val categoryTopayCountRDD: RDD[(String, Int)] = payToOneRDD.reduceByKey(_+_)

//    TODO 将上面统计的结果转换结构
//    tuple => (元素1，元素2,元素3)
//    （品类，点击数量），（品类，下单数量），（品类，支付数量）
//    使用reduceByKey()前提是  --（品类，(点击数量,0,0)），（品类，(0,下单数量0,)），（品类,(0,0支付数量)）
    val newCategoryToClickCountRDD: RDD[(String, (Int, Int, Int))] = categoryToClickCountRDD.map {
      case (id, clickCount) => {
        (id, (clickCount, 0, 0))
      }
    }
    val newCategoryToOrderCountRDD: RDD[(String, (Int, Int, Int))] = categoryToOrderCountRDD.map {
      case (id, orderCount) => {
        (id, (0, orderCount, 0))
      }
    }
    val newcategoryTopayCountRDD: RDD[(String, (Int, Int, Int))] = categoryTopayCountRDD.map {
      case (id, payCount) => {
        (id, (0, 0, payCount))
      }
    }
    val countRDD: RDD[(String, (Int, Int, Int))] = newCategoryToClickCountRDD.union(newCategoryToOrderCountRDD).union(newcategoryTopayCountRDD)

    val reduceRDD: RDD[(String, (Int, Int, Int))] = countRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )


//    TODO 将转换结构后的数据进行排序（降序）

    val sortRDD: RDD[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2, ascending = false)

//    TODO 将排序后的结果取前10名
    val result: Array[(String, (Int, Int, Int))] = sortRDD.take(10)

    result

  }

/***************************************************************************************************************/

// TODO 优化
def analysis1() = {
    //    TODO 读取数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("in/user_visit_action.txt")

    actionRDD.cache()

    //    TODO 对品类进行点击的统计
    //    (category, clickCount)
    val clickRDD: RDD[(String, Int)] = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        (datas(6), 1)
      }
    ).filter(_._1 != "-1")
    val categoryToClickCountRDD: RDD[(String, Int)] = clickRDD.reduceByKey(_+_)

    //    TODO 对品类进行下单的统计
    //    (category, orderCount)
    //    (品类1，品类2，品类3，10)  => (品类1，10),(品类2，10),(品类3，10)
    //    所以的到数据之后要将其在进行一次扁平化处理(flatmap())
    val orderRDD = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8)
      }
    ).filter(_ != "null")

    val orderToOneRDD = orderRDD.flatMap({
      id => {
        val ids = id.split(",")
        ids.map( id => (id,1))
      }
    })

    val categoryToOrderCountRDD: RDD[(String, Int)] = orderToOneRDD.reduceByKey(_+_)

    //    TODO 对品类进行支付的统计
    //    (category, payCount)
    val payRDD = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10)
      }).filter(_ != "null")

    val payToOneRDD = payRDD.flatMap({
      id => {
        val ids = id.split(",")
        ids.map( id => (id,1))
      }
    })

    val categoryTopayCountRDD: RDD[(String, Int)] = payToOneRDD.reduceByKey(_+_)

    //    TODO 将上面统计的结果转换结构
    //    tuple => (元素1，元素2,元素3)
    //    （品类，点击数量），（品类，下单数量），（品类，支付数量）
    //    （品类，（点击数量，下单数量，支付数量））
    val joinRDD: RDD[(String, (Int, Int))] = categoryToClickCountRDD.join(categoryToOrderCountRDD)
    val joinRseultRDD: RDD[(String, ((Int, Int), Int))] = joinRDD.join(categoryTopayCountRDD)

    //工作中，对key保持不变，只对value进行操作，就是用mapValues
    val mapRDD: RDD[(String, (Int, Int, Int))] = joinRseultRDD.mapValues {
      case ((clickCount, orderCount), payCount) => {
        (clickCount, orderCount, payCount)
      }
    }
    //    TODO 将转换结构后的数据进行排序（降序）

    val sortRDD: RDD[(String, (Int, Int, Int))] = mapRDD.sortBy(_._2, ascending = false)

    //    TODO 将排序后的结果取前10名
    val result: Array[(String, (Int, Int, Int))] = sortRDD.take(10)

    result

  }
}
