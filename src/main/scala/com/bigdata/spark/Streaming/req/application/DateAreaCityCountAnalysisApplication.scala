package com.bigdata.spark.Streaming.req.application

import com.bigdata.spark.Streaming.req.controller.DateAreaCityCountAnalysisController
import com.bigdata.summer.framework.core.TApplication

object DateAreaCityCountAnalysisApplication extends TApplication{

  def main(args: Array[String]): Unit = {
    start("sparkStreaming"){
      val controller = new DateAreaCityCountAnalysisController
      controller.execute()
    }
  }



}
