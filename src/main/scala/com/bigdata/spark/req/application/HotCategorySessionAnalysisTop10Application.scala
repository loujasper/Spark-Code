package com.bigdata.spark.req.application

import com.bigdata.spark.req.controller.HotCategorySessionAnalysisTop10Controller
import com.bigdata.summer.framework.core.TApplication

object HotCategorySessionAnalysisTop10Application extends App with TApplication {
  start("spark"){
    val Controller = new HotCategorySessionAnalysisTop10Controller
    Controller.execute()
  }
}
