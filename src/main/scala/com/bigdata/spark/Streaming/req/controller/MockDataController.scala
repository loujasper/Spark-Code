package com.bigdata.spark.Streaming.req.controller

import com.bigdata.spark.Streaming.req.service.MockDataService
import com.bigdata.summer.framework.core.TController


class MockDataController extends TController{

  private val mockDataService: MockDataService = new MockDataService

  override def execute(): Unit = {
    val result = mockDataService.analysis()
  }
}
