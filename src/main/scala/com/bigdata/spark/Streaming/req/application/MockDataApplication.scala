package com.bigdata.spark.Streaming.req.application

import com.bigdata.spark.Streaming.req.controller.MockDataController
import com.bigdata.summer.framework.core.TApplication

object MockDataApplication extends App with TApplication{

  start("sparkStreaming"){
  val controller = new MockDataController
    controller.execute()
  }

}
