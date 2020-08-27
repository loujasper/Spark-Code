package com.bigdata.spark.Streaming.req.controller

import com.bigdata.spark.Streaming.req.service.DateAreaCityCountAnalysisService
import com.bigdata.summer.framework.core.TController

class DateAreaCityCountAnalysisController extends TController {

  private val dateAreaCityCountAnalysisService = new DateAreaCityCountAnalysisService

  override def execute(): Unit = {
    val result =  dateAreaCityCountAnalysisService.analysis()
  }
}
