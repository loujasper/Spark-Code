package com.bigdata.spark.req.application

import com.bigdata.spark.req.controller.PageflowController
import com.bigdata.summer.framework.core.TApplication


object PageflowApplication extends App with TApplication{

  start("spark") {
    val Controller = new PageflowController

    Controller.execute()
  }

}
