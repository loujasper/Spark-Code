package com.bigdata.spark.req.controller

import com.bigdata.spark.req.service.HotCategoryAnalysisTop10Service
import com.bigdata.summer.framework.core.TController

class HotCategoryAnalysisTop10Controller extends TController {

  private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service

  override def execute(): Unit = {
    val result = hotCategoryAnalysisTop10Service.analysis()
    result.foreach(println)
  }
}

