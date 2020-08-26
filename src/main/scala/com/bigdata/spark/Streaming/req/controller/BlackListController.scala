package com.bigdata.spark.Streaming.req.controller


import com.bigdata.spark.Streaming.req.service.BlackListService
import com.bigdata.summer.framework.core.TController


class BlackListController extends TController{

  private val blackListService: BlackListService = new BlackListService

  override def execute(): Unit = {
    val result = blackListService.analysis()
  }
}
