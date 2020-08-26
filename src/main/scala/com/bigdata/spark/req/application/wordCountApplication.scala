package com.bigdata.spark.req.application

import com.bigdata.spark.req.controller.WordCountController
import com.bigdata.summer.framework.core.TApplication


object wordCountApplication extends App with TApplication{

  start("spark"){

//    TODO 获取 Controller 执行excuter
    val controller = new WordCountController
    controller.execute()


  }
}
