package com.bigdata.spark.req.dao

import com.bigdata.summer.framework.core.TDao
import com.bigdata.summer.framework.util.EnvUtils
import org.apache.spark.rdd.RDD

class HotCategoryAnalysisTop10Dao extends TDao {

  override def readFile(path: String): RDD[String] = EnvUtils.getEnv().textFile(path)

}
