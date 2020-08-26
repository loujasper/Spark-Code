package com.bigdata.spark.req.application

import com.bigdata.spark.req.controller.HotCategoryAnalysisTop10Controller
import com.bigdata.summer.framework.core.TApplication


object HotCategoryAnalysisTop10Application extends App with TApplication {
  start("spark"){
    val Controller = new HotCategoryAnalysisTop10Controller
    Controller.execute()
  }
}
