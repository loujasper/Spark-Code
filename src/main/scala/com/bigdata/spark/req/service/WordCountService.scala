package com.bigdata.spark.req.service

import com.bigdata.spark.req.application.wordCountApplication.envData
import com.bigdata.spark.req.dao.WordCountDao
import com.bigdata.summer.framework.core.TService
import com.bigdata.summer.framework.util.EnvUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountService extends TService{

  private val wordCountDao = new WordCountDao

  override def analysis() = {

//  TODO 调用Dao对象，获取到数据之后，处理业务数据
    val fileRDD: RDD[String] = wordCountDao.readFile("in/word.txt")

    val resultRDD: Array[(String, Int)] = fileRDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect()

    resultRDD
  }
}
