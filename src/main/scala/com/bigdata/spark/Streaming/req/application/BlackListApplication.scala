package com.bigdata.spark.Streaming.req.application


import com.bigdata.spark.Streaming.req.controller.BlackListController
import com.bigdata.summer.framework.core.TApplication

object BlackListApplication extends App with TApplication{

  start("sparkStreaming"){
  val controller = new BlackListController
    controller.execute()
  }

}
