package com.bigdata.spark.req.helper

import com.bigdata.spark.req.bean.HotCategory
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable


/**
  * 热门品类累加器
  *
  * 1.集成AccumulatorV2,定义泛型[In,Out]
  *   In:(品类，行为类型)
  *   Out :Map[品类，HotCategory]
  *
  * 2.重写方法(6个)
  */

class HotCategoryAccumulator extends AccumulatorV2[(String, String),mutable.Map[String, HotCategory]]{

  val HotCategoryMap = mutable.Map[String, HotCategory]()  //其实就是输出的返回值

  override def isZero: Boolean = HotCategoryMap.isEmpty

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
    new HotCategoryAccumulator  //直接new主类名
  }

  override def reset(): Unit = {
    HotCategoryMap.clear()
  }

  override def add(v: (String, String)): Unit = {
    val cid = v._1
    val actionType = v._2

    val hotCategory: HotCategory = HotCategoryMap.getOrElse(cid,HotCategory(cid, 0,0,0)) //当取不到cid时，用HotCategory(cid, 0,0,0) 全新的数据

    actionType match {
      case "click" => hotCategory.clickCount += 1
      case "order" => hotCategory.orderCount += 1
      case "pay" => hotCategory.payCount += 1
      case _ =>
    }
    HotCategoryMap(cid) = hotCategory
  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
    other.value.foreach{
        case (cid, hotCategory) => {
          val hc: HotCategory = HotCategoryMap.getOrElse(cid,HotCategory(cid, 0,0,0))
          hc.clickCount += hotCategory.clickCount
          hc.orderCount += hotCategory.orderCount
          hc.payCount += hotCategory.payCount

          HotCategoryMap(cid) = hc
      }
    }

  }

  override def value: mutable.Map[String, HotCategory] = HotCategoryMap
}
