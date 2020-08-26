package com.bigdata.spark.req.dao

import com.bigdata.summer.framework.core.TDao
import com.bigdata.summer.framework.util.EnvUtils
import org.apache.spark.rdd.RDD

class WordCountDao extends TDao{
//  TODO 获取数据
  override def readFile(path: String): RDD[String] = EnvUtils.getEnv().textFile(path)
}
