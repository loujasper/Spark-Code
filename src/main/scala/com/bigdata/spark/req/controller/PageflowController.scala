package com.bigdata.spark.req.controller

import com.bigdata.spark.req.service.PageflowService
import com.bigdata.summer.framework.core.TController

class PageflowController extends TController {

  private val pageflowService = new PageflowService

  override def execute(): Unit = {

    val result = pageflowService.analysis()

  }
}
