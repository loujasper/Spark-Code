package com.bigdata.spark.req.controller

import com.bigdata.spark.req.bean
import com.bigdata.spark.req.service.{HotCategoryAnalysisTop10Service, HotCategorySessionAnalysisTop10Service}
import com.bigdata.summer.framework.core.TController

class HotCategorySessionAnalysisTop10Controller extends TController {

  private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service
  private val hotCategorySessionAnalysisTop10Service = new HotCategorySessionAnalysisTop10Service

  override def execute(): Unit = {
    val categories: List[bean.HotCategory] = hotCategoryAnalysisTop10Service.analysis()
    val result: Array[(String, List[(String, Int)])] = hotCategorySessionAnalysisTop10Service.analysis(categories)
    result.foreach(println)

  }
}

