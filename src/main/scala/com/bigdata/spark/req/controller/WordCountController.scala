package com.bigdata.spark.req.controller

import com.bigdata.spark.req.service.WordCountService
import com.bigdata.summer.framework.core.TController
import org.apache.spark.rdd.RDD
/**
  *
  *
  */
class WordCountController extends TController{

  private val wordCountService = new WordCountService

  override def execute():Unit = {
//    TODO 接收处理之后的数据，并返回
    val wordCountArray: Array[(String, Int)] = wordCountService.analysis()
    println(wordCountArray.mkString(","))



  }
}
